package com.nuvio.app.features.downloads

import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.Call
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okio.ByteString.Companion.decodeBase64
import okio.ByteString.Companion.encodeUtf8

// Only fresh, large files backed by a strong ETag use the segmented path.
// Legacy .part files continue through the existing single-stream engine.
internal suspend fun trySegmentedAndroidDownload(
    item: DownloadItem,
    directory: File,
    client: OkHttpClient,
    onHeaders: (Long?, String?) -> Unit,
    onProgress: (Long, Long?) -> Unit,
    minParallelBytes: Long = 16L * 1024 * 1024,
    connections: Int = 4,
): File? = withContext(Dispatchers.IO) {
    require(File(item.fileName).name == item.fileName && item.fileName.isNotBlank())
    require(connections in 2..4)
    check(directory.isDirectory || directory.mkdirs()) { "Cannot create downloads directory" }
    val partial = File(directory, item.fileName + ".part")
    val segments = File(directory, item.fileName + ".segments")
    if (partial.exists() && !segments.exists()) return@withContext null
    if (partial.isFile && segments.exists()) {
        // Only possible after assembly: never overwrite an existing partial.
        val manifest = readSegmentManifest(segments)
        if (partial.length() == manifest.total && segmentFilesComplete(segments, manifest)) {
            onHeaders(manifest.total, manifest.etag)
            onProgress(manifest.total, manifest.total)
            segments.deleteRecursively()
            return@withContext partial
        }
        throw IOException("Existing partial and segmented state conflict; neither was changed")
    }

    val probe = client.newCall(segmentRequest(item, "bytes=0-0"))
    val identity = probe.execute().use { response ->
        if (response.code != 206) return@withContext if (segments.exists()) {
            throw IOException("Cannot verify source identity for existing segmented download")
        } else null
        val range = strictContentRange(response)
        val etag = response.header("ETag")
        if (range == null || range.first != 0L || range.second != 0L ||
            etag.isNullOrBlank() || etag.startsWith("W/") ||
            !response.header("Content-Encoding").orEmpty().equals("identity", ignoreCase = true)
                && !response.header("Content-Encoding").isNullOrBlank()
        ) {
            if (segments.exists()) throw IOException("Existing segments require strong source identity")
            return@withContext null
        }
        range.third to etag
    }
    val total = identity.first
    val etag = identity.second
    if (total < minParallelBytes || total < connections.toLong()) {
        if (segments.exists()) throw IOException("Source no longer qualifies for segmented resume")
        return@withContext null
    }

    val expected = SegmentManifest(total, etag, connections)
    if (segments.exists()) {
        if (readSegmentManifest(segments) != expected) {
            throw IOException("Source changed: preserving existing segments without mixing bytes")
        }
    } else {
        check(segments.mkdir()) { "Could not create segmented transfer directory" }
        writeSegmentManifest(segments, expected)
    }

    val spans = (0 until connections).map { index ->
        val start = total * index / connections
        val end = total * (index + 1) / connections - 1L
        start to end
    }
    val already = AtomicLong(0L)
    spans.forEachIndexed { index, span ->
        val length = segmentFile(segments, index).takeIf { it.exists() }?.length() ?: 0L
        if (length > span.second - span.first + 1) {
            throw IOException("Segment exceeds its assigned range; preserving partial data")
        }
        already.addAndGet(length)
    }

    onHeaders(total, etag)
    onProgress(already.get(), total)
    val calls = CopyOnWriteArrayList<Call>()
    val progressLock = Any()
    coroutineScope {
        // Cancellation must actively close blocking OkHttp sockets, not just cancel coroutines.
        val cancelSockets = launch {
            try { awaitCancellation() }
            finally { calls.forEach { it.cancel() } }
        }
        try {
            spans.mapIndexed { index, span ->
                async(Dispatchers.IO) {
                    val outputFile = segmentFile(segments, index)
                    val existing = outputFile.takeIf { it.exists() }?.length() ?: 0L
                    val start = span.first + existing
                    if (start > span.second) return@async
                    val call = client.newCall(segmentRequest(item, "bytes=" + start + "-" + span.second, etag))
                    calls.add(call)
                    try {
                        ensureActive()
                        call.execute().use { response ->
                            val actual = strictContentRange(response)
                            if (response.code != 206 ||
                                actual != Triple(start, span.second, total) ||
                                response.header("ETag") != etag ||
                                response.header("Content-Encoding")?.let { !it.equals("identity", true) } == true
                            ) throw IOException("Segment response identity/range mismatch")
                            val body = response.body ?: throw IOException("Empty range response")
                            FileOutputStream(outputFile, existing > 0).use { output ->
                                body.byteStream().use { input ->
                                    val buffer = ByteArray(128 * 1024)
                                    var written = existing
                                    val limit = span.second - span.first + 1
                                    while (true) {
                                        ensureActive()
                                        val n = input.read(buffer)
                                        if (n < 0) break
                                        if (n == 0) continue
                                        if (written + n > limit) throw IOException("Range response exceeded assigned segment")
                                        output.write(buffer, 0, n)
                                        written += n
                                        synchronized(progressLock) {
                                            onProgress(already.addAndGet(n.toLong()), total)
                                        }
                                    }
                                    if (written != limit) throw IOException("Segment ended before the requested range")
                                    output.fd.sync()
                                }
                            }
                        }
                    } finally {
                        call.cancel()
                        calls.remove(call)
                    }
                }
            }.awaitAll()
        } finally {
            cancelSockets.cancel()
            calls.forEach { it.cancel() }
        }
    }

    ensureActive()
    if (!segmentFilesComplete(segments, expected)) throw IOException("Segments incomplete")
    val assembling = File(directory, item.fileName + ".assembling")
    FileOutputStream(assembling, false).use { output ->
        spans.indices.forEach { index ->
            ensureActive()
            segmentFile(segments, index).inputStream().use { it.copyTo(output, 128 * 1024) }
        }
        output.fd.sync()
    }
    if (assembling.length() != total) throw IOException("Assembly size differs from validated ranges")
    if (partial.exists() || !assembling.renameTo(partial)) {
        throw IOException("Could not atomically publish segmented download")
    }
    segments.deleteRecursively()
    onProgress(total, total)
    partial
}

private data class SegmentManifest(val total: Long, val etag: String, val count: Int)
private fun segmentFile(directory: File, index: Int) = File(directory, "part-" + index)

private fun readSegmentManifest(directory: File): SegmentManifest {
    val lines = File(directory, "manifest").readLines()
    if (lines.size != 4 || lines[0] != "nuvio-segments-v1") throw IOException("Invalid segment manifest")
    val total = lines[1].toLongOrNull() ?: throw IOException("Invalid segment size")
    val count = lines[3].toIntOrNull() ?: throw IOException("Invalid segment count")
    if (total <= 0 || count !in 2..4) throw IOException("Invalid segmented transfer")
    val etag = lines[2].decodeBase64()?.utf8() ?: throw IOException("Invalid segment ETag")
    if (etag.isBlank() || etag.startsWith("W/")) throw IOException("Missing strong ETag")
    return SegmentManifest(total, etag, count)
}

private fun writeSegmentManifest(directory: File, manifest: SegmentManifest) {
    val file = File(directory, "manifest")
    val tmp = File(directory, "manifest.tmp")
    val encoded = manifest.etag.encodeUtf8().base64()
    FileOutputStream(tmp, false).use {
        it.write(("nuvio-segments-v1\n" + manifest.total + "\n" + encoded + "\n" + manifest.count + "\n").toByteArray())
        it.fd.sync()
    }
    if (!tmp.renameTo(file)) throw IOException("Could not persist segment manifest")
}

private fun segmentFilesComplete(directory: File, manifest: SegmentManifest): Boolean =
    (0 until manifest.count).all { index ->
        val start = manifest.total * index / manifest.count
        val end = manifest.total * (index + 1) / manifest.count - 1L
        segmentFile(directory, index).takeIf { it.isFile }?.length() == end - start + 1
    }

private fun segmentRequest(item: DownloadItem, range: String, etag: String? = null): Request =
    Request.Builder().url(item.sourceUrl).apply {
        item.sourceHeaders.forEach { (key, value) ->
            if (!key.equals("Range", true) && !key.equals("If-Range", true) &&
                !key.equals("Accept-Encoding", true)) header(key, value)
        }
        header("Accept-Encoding", "identity")
        header("Range", range)
        etag?.let { header("If-Range", it) }
    }.build()

private fun strictContentRange(response: Response): Triple<Long, Long, Long>? {
    val header = response.header("Content-Range") ?: return null
    val match = Regex("bytes (\\d+)-(\\d+)/(\\d+)").matchEntire(header.trim()) ?: return null
    val start = match.groupValues[1].toLongOrNull() ?: return null
    val end = match.groupValues[2].toLongOrNull() ?: return null
    val total = match.groupValues[3].toLongOrNull() ?: return null
    if (total <= 0L || start > end || end >= total) return null
    return Triple(start, end, total)
}
