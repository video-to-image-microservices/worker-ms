package video.to.image.worker_ms.service

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import video.to.image.worker_ms.messaging.ProcessVideoEvent
import video.to.image.worker_ms.messaging.VideoProcessingStatus
import video.to.image.worker_ms.messaging.VideoStatusEvent
import video.to.image.worker_ms.messaging.VideoStatusPublisher
import video.to.image.worker_ms.processing.FrameExtractor
import video.to.image.worker_ms.processing.ZipService
import video.to.image.worker_ms.storage.S3StorageService
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class VideoProcessingServiceTest {

    private lateinit var s3StorageService: S3StorageService
    private lateinit var frameExtractor: FrameExtractor
    private lateinit var zipService: ZipService
    private lateinit var videoStatusPublisher: VideoStatusPublisher
    private lateinit var service: VideoProcessingService

    @TempDir
    lateinit var tempDir: Path

    private val videoProcessId = UUID.fromString("11111111-1111-1111-1111-111111111111")
    private val userId = UUID.fromString("22222222-2222-2222-2222-222222222222")

    @BeforeEach
    fun setUp() {
        s3StorageService = mock()
        frameExtractor = mock()
        zipService = mock()
        videoStatusPublisher = mock()
        service = VideoProcessingService(
            s3StorageService = s3StorageService,
            frameExtractor = frameExtractor,
            zipService = zipService,
            videoStatusPublisher = videoStatusPublisher,
            workDir = tempDir.toString(),
        )
    }

    @Test
    fun `process publishes PROCESSING then PROCESSED on success`() {
        val event = sampleEvent()
        doNothing().whenever(s3StorageService).download(any(), any(), any())
        doNothing().whenever(frameExtractor).extract(any(), any())
        doNothing().whenever(zipService).zipDirectory(any(), any())
        whenever(s3StorageService.upload(any(), any(), any(), any())).thenAnswer {
            val zipPath = it.getArgument<Path>(2)
            Files.createDirectories(zipPath.parent)
            Files.writeString(zipPath, "zip-content")
            null
        }

        service.process(event)

        val order = inOrder(videoStatusPublisher, s3StorageService, frameExtractor, zipService)
        order.verify(videoStatusPublisher).publish(
            VideoStatusEvent(videoProcessId, VideoProcessingStatus.PROCESSING, null, null)
        )
        order.verify(s3StorageService).download(eq(event.bucket), eq(event.storageKey), any())
        order.verify(frameExtractor).extract(any(), any())
        order.verify(zipService).zipDirectory(any(), any())
        order.verify(s3StorageService).upload(
            eq(event.bucket),
            eq(event.outputZipKey),
            any(),
            eq("application/zip"),
        )
        order.verify(videoStatusPublisher).publish(
            VideoStatusEvent(
                videoProcessId,
                VideoProcessingStatus.PROCESSED,
                event.outputZipKey,
                "sample.zip",
            )
        )
        assertTrue(Files.notExists(tempDir.resolve(videoProcessId.toString())))
    }

    @Test
    fun `process publishes FAILED when download fails`() {
        val event = sampleEvent()
        doThrow(RuntimeException("s3 down"))
            .whenever(s3StorageService)
            .download(any(), any(), any())

        service.process(event)

        verify(videoStatusPublisher).publish(
            VideoStatusEvent(videoProcessId, VideoProcessingStatus.PROCESSING, null, null)
        )
        verify(videoStatusPublisher).publish(
            VideoStatusEvent(videoProcessId, VideoProcessingStatus.FAILED, null, null)
        )
        verify(frameExtractor, never()).extract(any(), any())
        verify(zipService, never()).zipDirectory(any(), any())
        verify(s3StorageService, never()).upload(any(), any(), any(), any())
        assertTrue(Files.notExists(tempDir.resolve(videoProcessId.toString())))
    }

    @Test
    fun `process publishes FAILED and skips upload when frame extraction fails`() {
        val event = sampleEvent()
        doNothing().whenever(s3StorageService).download(any(), any(), any())
        doThrow(RuntimeException("ffmpeg failed"))
            .whenever(frameExtractor)
            .extract(any(), any())

        service.process(event)

        verify(videoStatusPublisher).publish(
            VideoStatusEvent(videoProcessId, VideoProcessingStatus.FAILED, null, null)
        )
        verify(zipService, never()).zipDirectory(any(), any())
        verify(s3StorageService, never()).upload(any(), any(), any(), any())
        assertTrue(Files.notExists(tempDir.resolve(videoProcessId.toString())))
    }

    @Test
    fun `process sanitizes the original file name inside its isolated job directory`() {
        val event = sampleEvent(originalFileName = "../../escape.mp4")
        doAnswer {
            val destination = it.getArgument<Path>(2)
            assertEquals("escape.mp4", destination.fileName.toString())
            assertTrue(destination.startsWith(tempDir.resolve(videoProcessId.toString())))
            null
        }.whenever(s3StorageService).download(any(), any(), any())
        doNothing().whenever(frameExtractor).extract(any(), any())
        doNothing().whenever(zipService).zipDirectory(any(), any())
        doNothing().whenever(s3StorageService).upload(any(), any(), any(), any())

        service.process(event)

        verify(videoStatusPublisher).publish(
            VideoStatusEvent(
                videoProcessId,
                VideoProcessingStatus.PROCESSED,
                event.outputZipKey,
                "escape.zip",
            )
        )
    }

    @Test
    fun `process handles two video jobs concurrently without sharing work directories`() {
        val secondProcessId = UUID.fromString("33333333-3333-3333-3333-333333333333")
        val firstEvent = sampleEvent()
        val secondEvent = sampleEvent(secondProcessId, "second.mp4")
        val extractionBarrier = CyclicBarrier(2)

        doNothing().whenever(s3StorageService).download(any(), any(), any())
        doAnswer {
            extractionBarrier.await(5, TimeUnit.SECONDS)
            null
        }.whenever(frameExtractor).extract(any(), any())
        doNothing().whenever(zipService).zipDirectory(any(), any())
        doNothing().whenever(s3StorageService).upload(any(), any(), any(), any())

        val executor = Executors.newFixedThreadPool(2)
        try {
            val first = executor.submit { service.process(firstEvent) }
            val second = executor.submit { service.process(secondEvent) }
            first.get(10, TimeUnit.SECONDS)
            second.get(10, TimeUnit.SECONDS)
        } finally {
            executor.shutdownNow()
        }

        verify(videoStatusPublisher).publish(
            VideoStatusEvent(
                firstEvent.videoProcessId,
                VideoProcessingStatus.PROCESSED,
                firstEvent.outputZipKey,
                "sample.zip",
            )
        )
        verify(videoStatusPublisher).publish(
            VideoStatusEvent(
                secondEvent.videoProcessId,
                VideoProcessingStatus.PROCESSED,
                secondEvent.outputZipKey,
                "second.zip",
            )
        )
        assertTrue(Files.notExists(tempDir.resolve(firstEvent.videoProcessId.toString())))
        assertTrue(Files.notExists(tempDir.resolve(secondEvent.videoProcessId.toString())))
    }

    private fun sampleEvent(
        processId: UUID = videoProcessId,
        originalFileName: String = "sample.mp4",
    ) = ProcessVideoEvent(
        videoProcessId = processId,
        userId = userId,
        bucket = "videos-bucket",
        storageKey = "$userId/$processId/$originalFileName",
        outputZipKey = "$userId/$processId/generated/${originalFileName.substringBeforeLast('.')}.zip",
        originalFileName = originalFileName,
        contentType = "video/mp4",
    )
}
