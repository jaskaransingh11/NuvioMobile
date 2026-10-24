package com.nuvio.app.features.downloads

import java.io.File
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import okio.Buffer
import org.junit.Rule
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import org.junit.rules.TemporaryFolder
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34])
class AndroidMultiRangeTransferTest {
    @get:Rule val temporary = TemporaryFolder()
    private val bytes = ByteArray(24 * 1024 * 1024) { (it % 251).toByte() }

    private fun rangeResponse(request: RecordedRequest, etag: String): MockResponse {
        val range = Regex("bytes=(\\d+)-(\\d+)").matchEntire(
            request.getHeader("Range").orEmpty()
        ) ?: return MockResponse().setResponseCode(400)
        val first = range.groupValues[1].toInt()
        val last = range.groupValues[2].toInt()
        if (first !in bytes.indices || last !in bytes.indices || first > last) {
            return MockResponse().setResponseCode(416)
        }
        return MockResponse().setResponseCode(206)
            .setHeader("ETag", etag)
            .setHeader("Content-Range", "bytes $first-$last/${bytes.size}")
            .setBody(Buffer().write(bytes, first, last - first + 1))
    }

    @Test
    fun downloadsWholeFileUsingDistinctRangesAndKeepsHeaders() = runBlocking {
        MockWebServer().use { server ->
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse =
                    rangeResponse(request, "\"v1\"")
            }
            val dir = temporary.newFolder()
            val progress = mutableListOf<Long>()
            val output = transferAndroidDownload(
                downloadItem(server.url("/video").toString()), dir, null,
                parallelConnections = 4,
                onHeaders = { size, etag ->
                    assertEquals(bytes.size.toLong(), size)
                    assertEquals("\"v1\"", etag)
                },
                onProgress = { done, _ -> synchronized(progress) { progress.add(done) } },
            )
            assertContentEquals(bytes, output.readBytes())
            assertEquals(bytes.size.toLong(), synchronized(progress) { progress.last() })
            assertEquals(bytes.size.toLong(), output.length())
            assertEquals(4, server.requestCount) // 1-byte probe + 3 independent 8-MiB ranges
            repeat(4) {
                val request = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
                assertEquals("identity", request.getHeader("Accept-Encoding"))
                assertEquals("Bearer test", request.getHeader("Authorization"))
                if (request.getHeader("Range") != "bytes=0-0") {
                    assertEquals("\"v1\"", request.getHeader("If-Range"))
                }
            }
            assertTrue(File(dir, "video.mkv.part.ranges.json").isFile)
        }
    }

    @Test
    fun resumesOnlyDurablyCompletedChunksAndMatchesEntireSource() = runBlocking {
        MockWebServer().use { server ->
            var failuresRemaining = 1
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (request.getHeader("Range") == "bytes=16777216-25165823" &&
                        failuresRemaining-- > 0) {
                        return MockResponse().setResponseCode(503)
                    }
                    return rangeResponse(request, "\"v1\"")
                }
            }
            val dir = temporary.newFolder()
            val item = downloadItem(server.url("/video").toString())
            assertFailsWith<DownloadHttpException> {
                transferAndroidDownload(item, dir, null, parallelConnections = 2,
                    onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            }
            val checkpoint = File(dir, "video.mkv.part.ranges.json")
            val partial = File(dir, "video.mkv.part")
            assertTrue(checkpoint.isFile)
            assertEquals(bytes.size.toLong(), partial.length())
            val before = checkpoint.readText()
            assertTrue(before.contains("completed"))
            val output = transferAndroidDownload(item, dir, null, parallelConnections = 2,
                onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            assertContentEquals(bytes, output.readBytes())
            assertTrue(server.requestCount >= 6)
        }
    }

    @Test
    fun changedEtagFailsClosedAndNeverOverwritesSparsePartial() = runBlocking {
        MockWebServer().use { server ->
            var firstRun = true
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    if (firstRun && request.getHeader("Range") == "bytes=16777216-25165823") {
                        return MockResponse().setResponseCode(503)
                    }
                    return rangeResponse(request, if (firstRun) "\"source-A\"" else "\"source-B\"")
                }
            }
            val dir = temporary.newFolder()
            val item = downloadItem(server.url("/video").toString())
            assertFailsWith<DownloadHttpException> {
                transferAndroidDownload(item, dir, null, parallelConnections = 2,
                    onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            }
            val checkpoint = File(dir, "video.mkv.part.ranges.json")
            val originalCheckpoint = checkpoint.readBytes()
            val partial = File(dir, "video.mkv.part")
            val originalLength = partial.length()
            firstRun = false
            assertFailsWith<IOException> {
                transferAndroidDownload(item, dir, null, parallelConnections = 4,
                    onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            }
            assertContentEquals(originalCheckpoint, checkpoint.readBytes())
            assertEquals(originalLength, partial.length())
            assertFalse(File(dir, "video.mkv").exists())
        }
    }

    @Test
    fun ignoredRangeConsumesOriginalResponseWithoutRefetchingSignedUrl() = runBlocking {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setBody("one-use signed URL payload"))
            val dir = temporary.newFolder()
            val output = transferAndroidDownload(downloadItem(server.url("/one-use").toString()),
                dir, null, parallelConnections = 4,
                onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            assertEquals("one-use signed URL payload", output.readText())
            assertEquals(1, server.requestCount)
            assertEquals("bytes=0-0", server.takeRequest().getHeader("Range"))
        }
    }

    @Test
    fun noStrongValidatorFallsBackToExistingSequentialEngine() = runBlocking {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(206)
                .setHeader("Content-Range", "bytes 0-0/25165824")
                .setBody("x"))
            server.enqueue(MockResponse().setBody("ordinary content"))
            val dir = temporary.newFolder()
            val output = transferAndroidDownload(downloadItem(server.url("/video").toString()),
                dir, null, parallelConnections = 4,
                onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            assertEquals("ordinary content", output.readText())
            assertFalse(File(dir, "video.mkv.part.ranges.json").exists())
            assertEquals("bytes=0-0", server.takeRequest().getHeader("Range"))
            assertEquals(null, server.takeRequest().getHeader("Range"))
        }
    }

    @Test
    fun changedSourceUrlCannotReuseOldRangeCheckpoint() = runBlocking {
        MockWebServer().use { server ->
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse =
                    if (request.getHeader("Range") == "bytes=16777216-25165823")
                        MockResponse().setResponseCode(503)
                    else rangeResponse(request, "\"v1\"")
            }
            val dir = temporary.newFolder()
            val item = downloadItem(server.url("/one").toString())
            assertFailsWith<DownloadHttpException> {
                transferAndroidDownload(item, dir, null, parallelConnections = 2,
                    onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            }
            val originalState = File(dir, "video.mkv.part.ranges.json").readBytes()
            assertFailsWith<IOException> {
                transferAndroidDownload(item.copy(sourceUrl = server.url("/two").toString()),
                    dir, null, parallelConnections = 2,
                    onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            }
            assertContentEquals(originalState,
                File(dir, "video.mkv.part.ranges.json").readBytes())
        }
    }

    @Test
    fun missingCheckpointNeverTreatsPreallocatedLengthAsProgress() = runBlocking {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setBody("real fresh payload"))
            val dir = temporary.newFolder()
            val partial = File(dir, "video.mkv.part")
            partial.writeText("old")
            val output = transferAndroidDownload(downloadItem(server.url("/video").toString()),
                dir, null, parallelConnections = 4,
                onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            // Old legacy parts are handled by the current sequential downloader.
            // A 200 response discards the old prefix rather than appending.
            assertEquals("real fresh payload", output.readText())
            assertEquals(1, server.requestCount)
        }
    }

    @Test
    fun missingEtagOnSegmentIsRejectedAndPartialPreserved() = runBlocking {
        MockWebServer().use { server ->
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    val response = rangeResponse(request, "\"v1\"")
                    if (request.getHeader("Range") == "bytes=8388608-16777215") {
                        response.removeHeader("ETag")
                    }
                    return response
                }
            }
            val dir = temporary.newFolder()
            assertFailsWith<DownloadIdentityException> {
                transferAndroidDownload(downloadItem(server.url("/video").toString()), dir, null,
                    parallelConnections = 2,
                    onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            }
            assertTrue(File(dir, "video.mkv.part.ranges.json").isFile)
            assertFalse(File(dir, "video.mkv").exists())
        }
    }

    @Test
    fun serverIgnoringRangeOnChunkCannotOverwriteSparseFile() = runBlocking {
        MockWebServer().use { server ->
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse =
                    if (request.getHeader("Range") == "bytes=8388608-16777215") {
                        MockResponse().setResponseCode(200).setBody("not a range")
                    } else rangeResponse(request, "\"v1\"")
            }
            val dir = temporary.newFolder()
            assertFailsWith<DownloadIdentityException> {
                transferAndroidDownload(downloadItem(server.url("/video").toString()), dir, null,
                    parallelConnections = 2,
                    onHeaders = { _, _ -> }, onProgress = { _, _ -> })
            }
            assertTrue(File(dir, "video.mkv.part.ranges.json").isFile)
            assertFalse(File(dir, "video.mkv").exists())
        }
    }
}
