package com.nuvio.app.features.downloads

import android.util.AtomicFile
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.RandomAccessFile
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import okhttp3.OkHttpClient
import okhttp3.Request

/** Identity mismatches are not transient HTTP failures and must not be retried. */
internal class DownloadIdentityException(message: String) : IOException(message)

private const val RANGE_CHUNK_BYTES = 8L * 1024 * 1024
private const val MIN_PARALLEL_BYTES = 16L * 1024 * 1024
private const val MAX_CHUNKS = 65_536

@Serializable
private data class RangeCheckpoint(
    val format: Int = 1,
    val total: Long,
    val chunkSize: Long,
    val sourceHash: String,
    val etag: String,
    val completed: Set<Int>,
)

private val checkpointJson = Json { ignoreUnknownKeys = false }

/**
 * A four-connection alternative to the sequential transfer. A strong ETag is
 * mandatory: a file length or a debrid torrent name alone cannot prove identity.
 * Existing legacy .part files are deliberately left with the sequential engine.
 *
 * Returns null only when no multi-range state exists and the origin does not
 * support safe parallel transfer. Never silently falls back on an existing
 * range checkpoint, because sparse .part file length is not download progress.
 */
internal suspend fun transferAndroidMultiRange(
    item: DownloadItem,
    directory: File,
    client: OkHttpClient,
    connections: Int,
    onHeaders: (Long?, String?) -> Unit,
    onProgress: (Long, Long?) -> Unit,
): File? = withContext(Dispatchers.IO) {
    require(connections in 2..8)
    require(File(item.fileName).name == item.fileName && item.fileName.isNotBlank())
    check(directory.isDirectory || directory.mkdirs()) { "Cannot create downloads directory" }

    val partial = File(directory, "${item.fileName}.part")
    val checkpointFile = File(directory, "${item.fileName}.part.ranges.json")
    if (!checkpointFile.exists() && partial.exists()) return@withContext null
    if (checkpointFile.exists() && !partial.exists()) {
        // The renamed final file may be recovered by the scheduler; a stale
        // checkpoint alone has no usable data and is safe to remove.
        AtomicFile(checkpointFile).delete()
    }
    val resuming = checkpointFile.exists()

    val probe = downloadRangeRequest(item, 0L, 0L, null)
    val call = client.newCall(probe)
    val canceller = launch(start = CoroutineStart.UNDISPATCHED) {
        try { awaitCancellation() } finally { call.cancel() }
    }
    val identity: Pair<Long, String>
    try {
        call.execute().use { response ->
            if (response.code == 200 && !resuming) {
                // A server can ignore Range. Consume this response directly: it
                // may be a one-use signed URL, and a second GET is not guaranteed.
                val body = response.body ?: throw IOException("Empty download response")
                val length = body.contentLength().takeIf { it >= 0L }
                val originValidator = response.header("ETag")?.takeUnless { it.startsWith("W/") }
                    ?: response.header("Last-Modified")
                onHeaders(length, originValidator)
                var written = 0L
                onProgress(0L, length)
                body.byteStream().use { input ->
                    FileOutputStream(partial, false).use { output ->
                        val buffer = ByteArray(64 * 1024)
                        while (true) {
                            ensureActive()
                            val count = input.read(buffer)
                            if (count < 0) break
                            if (count == 0) continue
                            output.write(buffer, 0, count)
                            written += count
                            onProgress(written, length)
                        }
                        output.fd.sync()
                    }
                }
                if (length != null && written != length) throw IOException("Truncated download response")
                return@withContext partial
            }
            if (response.code != 206) {
                if (response.code in 200..299) {
                    if (resuming) throw DownloadIdentityException("Cannot resume: byte ranges are unavailable")
                    return@withContext null
                }
                throw DownloadHttpException(response.code)
            }
            val range = parseStrictContentRange(response.header("Content-Range"))
            val strongEtag = response.header("ETag")?.trim()?.takeIf { it.startsWith("\"") && it.endsWith("\"") }
            if (range == null || range.first != 0L || range.second != 0L ||
                range.third < MIN_PARALLEL_BYTES || strongEtag == null) {
                if (resuming) throw DownloadIdentityException("Cannot safely resume: source identity or byte ranges changed")
                return@withContext null
            }
            if (response.body?.contentLength()?.takeIf { it >= 0L }?.let { it != 1L } == true) {
                throw IOException("Invalid probe length")
            }
            val probeStream = response.body?.byteStream() ?: throw IOException("Empty range probe")
            if (probeStream.read() < 0 || probeStream.read() != -1) {
                throw IOException("Invalid range probe body")
            }
            identity = range.third to strongEtag
        }
    } finally {
        canceller.cancel()
        call.cancel()
    }

    val (total, etag) = identity
    val numChunksLong = (total - 1L) / RANGE_CHUNK_BYTES + 1L
    if (numChunksLong > MAX_CHUNKS) {
        if (resuming) throw DownloadIdentityException("Cannot resume: too many range chunks")
        return@withContext null
    }
    val numChunks = numChunksLong.toInt()
    val sourceHash = MessageDigest.getInstance("SHA-256")
        .digest(item.sourceUrl.toByteArray(Charsets.UTF_8))
        .joinToString("") { "%02x".format(it) }
    val recorded = if (resuming) {
        try {
            checkpointJson.decodeFromString<RangeCheckpoint>(
                AtomicFile(checkpointFile).readFully().decodeToString()
            )
        } catch (_: Exception) {
            throw DownloadIdentityException("Cannot resume: range checkpoint is damaged")
        }
    } else null
    if (recorded != null &&
        (recorded.format != 1 || recorded.total != total || recorded.etag != etag ||
            recorded.sourceHash != sourceHash || recorded.chunkSize != RANGE_CHUNK_BYTES ||
            recorded.completed.any { it < 0 || it >= numChunks } || partial.length() != total)) {
        throw DownloadIdentityException("Cannot resume: the saved source does not match; partial data was preserved")
    }

    val completed = recorded?.completed?.toMutableSet() ?: mutableSetOf()
    val stateLock = Any()
    fun chunkLength(index: Int): Long =
        minOf(RANGE_CHUNK_BYTES, total - index.toLong() * RANGE_CHUNK_BYTES)
    val confirmed = completed.sumOf(::chunkLength)
    val progress = AtomicLong(confirmed)
    val checkpoint = AtomicFile(checkpointFile)
    fun persist() {
        val encoded = checkpointJson.encodeToString(
            RangeCheckpoint(total = total, chunkSize = RANGE_CHUNK_BYTES,
                sourceHash = sourceHash, etag = etag, completed = completed)
        )
        val output = checkpoint.startWrite()
        try {
            output.write(encoded.toByteArray(Charsets.UTF_8))
            checkpoint.finishWrite(output)
        } catch (error: Throwable) {
            checkpoint.failWrite(output)
            throw error
        }
    }

    // Store metadata before creating a sparse .part. A process death cannot
    // accidentally make the sequential downloader treat its length as progress.
    synchronized(stateLock) { persist() }
    if (!resuming) {
        RandomAccessFile(partial, "rw").use { it.setLength(total) }
    }

    onHeaders(total, etag)
    onProgress(confirmed, total)
    val next = AtomicInteger(0)
    coroutineScope {
        (0 until minOf(connections, numChunks)).map {
            async(Dispatchers.IO) {
                while (true) {
                    ensureActive()
                    val index = next.getAndIncrement()
                    if (index >= numChunks) break
                    if (synchronized(stateLock) { index in completed }) continue
                    val start = index.toLong() * RANGE_CHUNK_BYTES
                    val end = start + chunkLength(index) - 1L
                    val request = downloadRangeRequest(item, start, end, etag)
                    val chunkCall = client.newCall(request)
                    val abort = launch(start = CoroutineStart.UNDISPATCHED) {
                        try { awaitCancellation() } finally { chunkCall.cancel() }
                    }
                    try {
                        chunkCall.execute().use { response ->
                            if (response.code != 206) {
                                if (response.code !in 200..299) throw DownloadHttpException(response.code)
                                throw DownloadIdentityException("Server ignored the verified byte range")
                            }
                            val got = parseStrictContentRange(response.header("Content-Range"))
                            if (got != Triple(start, end, total) || response.header("ETag") != etag) {
                                throw DownloadIdentityException("Source changed or returned the wrong byte range")
                            }
                            val expected = end - start + 1L
                            val body = response.body ?: throw IOException("Empty range response")
                            if (body.contentLength() >= 0L && body.contentLength() != expected) {
                                throw IOException("Range response length does not match")
                            }
                            RandomAccessFile(partial, "rw").use { output ->
                                output.seek(start)
                                body.byteStream().use { input ->
                                    val buffer = ByteArray(64 * 1024)
                                    var remaining = expected
                                    while (remaining > 0L) {
                                        ensureActive()
                                        val count = input.read(buffer, 0, minOf(remaining, buffer.size.toLong()).toInt())
                                        if (count < 0) throw IOException("Range ended before its last byte")
                                        if (count == 0) continue
                                        output.write(buffer, 0, count)
                                        remaining -= count
                                        onProgress(progress.addAndGet(count.toLong()), total)
                                    }
                                    if (input.read() != -1) throw IOException("Range contained extra bytes")
                                }
                                output.fd.sync()
                            }
                        }
                        // A chunk only counts as resumable after its bytes are
                        // durable and its completion marker is atomically saved.
                        synchronized(stateLock) {
                            completed.add(index)
                            persist()
                        }
                    } finally {
                        abort.cancel()
                        chunkCall.cancel()
                    }
                }
            }
        }.awaitAll()
    }

    if (synchronized(stateLock) { completed.size } != numChunks) {
        throw IOException("Not all verified ranges are complete")
    }
    onProgress(total, total)
    partial
}

private fun downloadRangeRequest(
    item: DownloadItem, start: Long, end: Long, validator: String?,
): Request = Request.Builder().url(item.sourceUrl).apply {
    item.sourceHeaders.forEach { (key, value) ->
        if (!key.equals("Range", true) && !key.equals("If-Range", true) &&
            !key.equals("Accept-Encoding", true)) header(key, value)
    }
    header("Accept-Encoding", "identity")
    header("Range", "bytes=$start-$end")
    validator?.let { header("If-Range", it) }
}.build()

private fun parseStrictContentRange(value: String?): Triple<Long, Long, Long>? {
    val match = Regex("bytes (\\d+)-(\\d+)/(\\d+)").matchEntire(value?.trim().orEmpty())
        ?: return null
    val first = match.groupValues[1].toLongOrNull() ?: return null
    val last = match.groupValues[2].toLongOrNull() ?: return null
    val total = match.groupValues[3].toLongOrNull() ?: return null
    if (first < 0L || last < first || total <= last) return null
    return Triple(first, last, total)
}
