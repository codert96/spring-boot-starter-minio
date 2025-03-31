package io.github.codert96.minio;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.github.kokorin.jaffree.ffmpeg.FFmpeg;
import com.github.kokorin.jaffree.ffmpeg.UrlInput;
import com.github.kokorin.jaffree.ffmpeg.UrlOutput;
import io.github.codert96.minio.bean.MinioConfigProperties;
import io.minio.*;
import io.minio.errors.MinioException;
import io.minio.errors.ServerException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Headers;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.http.*;
import org.springframework.util.StopWatch;
import org.springframework.util.StreamUtils;
import org.springframework.util.unit.DataSize;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@RequiredArgsConstructor
@SuppressWarnings("unused")
public class MinioFileTemplate implements InitializingBean {
    private static final List<String> UNNECESSARY_HEADERS = Arrays.asList(
            HttpHeaders.COOKIE,
            HttpHeaders.AUTHORIZATION,
            HttpHeaders.SERVER,
            HttpHeaders.SET_COOKIE,
            HttpHeaders.SERVER
    );
    private static final long MIN_PART_SIZE = DataSize.ofMegabytes(5).toBytes();
    private static final TimeBasedGenerator UUID_GENERATOR = Generators.timeBasedGenerator();

    private final MinioClient minioClient;
    private final MinioConfigProperties minioConfigProperties;

    public String upload(MultipartFile multipartFile) throws Exception {
        String fileId = getFilename(multipartFile.getOriginalFilename());
        String filename = Objects.requireNonNullElse(multipartFile.getOriginalFilename(), fileId);
        try (InputStream inputStream = multipartFile.getInputStream()) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(minioConfigProperties.getBucket())
                            .object(fileId)
                            .contentType(multipartFile.getContentType())
                            .stream(inputStream, multipartFile.getSize(), -1)
                            .userMetadata(Map.of("original-filename", filename))
                            .build()
            );
        }
        return fileId;
    }

    public String upload(InputStream inputStream, String filename) throws Exception {
        String fileId = getFilename(filename);
        try (inputStream) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(minioConfigProperties.getBucket())
                            .object(fileId)
                            .userMetadata(Map.of("original-filename", filename))
                            .stream(inputStream, -1, MIN_PART_SIZE)
                            .build()
            );
        }
        return fileId;
    }

    public SseEmitter uploadToMpeg(MultipartFile multipartFile) throws Exception {
        String contentType = multipartFile.getContentType();
        if (!StringUtils.startsWith(contentType, "video/")) {
            throw new UnsupportedOperationException("Unsupported content type: " + contentType);
        }
        UUID uuid = UUID_GENERATOR.generate();
        String filename = "%s.m3u8".formatted(uuid);
        String originalFilename = multipartFile.getOriginalFilename();
        Path tempFile = Files.createTempFile("m3u8_", originalFilename);
        multipartFile.transferTo(tempFile);
        Path tempDirectory = Files.createTempDirectory("m3u8_");

        SseEmitter sseEmitter = new SseEmitter();
        Runnable runnable = () -> cleanUp(tempFile, tempDirectory);
        sseEmitter.onCompletion(runnable);
        sseEmitter.onTimeout(runnable);
        sseEmitter.onError(throwable -> runnable.run());
        Consumer<SseEmitter.SseEventBuilder> sender = sseEventBuilder -> {
            try {
                sseEmitter.send(sseEventBuilder);
            } catch (IOException ex) {
                log.error(ex.getMessage(), ex);
            }
        };

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        FFmpeg.atPath(Paths.get(minioConfigProperties.getFfmpegPath()))
                .addInput(UrlInput.fromPath(tempFile))
                .addOutput(UrlOutput.toPath(Path.of(tempDirectory.toString(), "%s.m3u8".formatted(uuid)))
                        .setFormat("hls")
                        .addArguments("-hls_time", "20")
                        .addArguments("-hls_list_size", "0")
                        .addArguments("-hls_segment_filename", Path.of(tempDirectory.toString(), "%s_%s.ts".formatted(uuid, "%03d")).toString()) // `.ts` 片段文件名
                )
                .setProgressListener(progress -> sender.accept(
                                SseEmitter.event()
                                        .name("progress")
                                        .data(progress, MediaType.APPLICATION_JSON)
                        )
                )
                .executeAsync()
                .toCompletableFuture()
                .whenCompleteAsync((fFmpegResult, t) -> {
                    if (Objects.nonNull(t)) {
                        log.error(t.getMessage(), t);
                        sseEmitter.completeWithError(t);
                        return;
                    }
                    try (Stream<Path> directoryStream = Files.list(tempDirectory)) {
                        CompletableFuture<?>[] array = directoryStream
                                .sorted(Comparator.naturalOrder())
                                .map(path ->
                                        CompletableFuture.supplyAsync(() -> {
                                                    String fileName = path.getFileName().toString();
                                                    try (InputStream inputStream = Files.newInputStream(path)) {
                                                        minioClient.putObject(
                                                                PutObjectArgs.builder()
                                                                        .bucket(minioConfigProperties.getBucket())
                                                                        .object(fileName)
                                                                        .userMetadata(Map.of("original-filename", fileName))
                                                                        .stream(inputStream, -1, MIN_PART_SIZE)
                                                                        .build()
                                                        );
                                                        return fileName;
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                        )
                                )
                                .peek(stringCompletableFuture ->
                                        stringCompletableFuture.whenCompleteAsync((part, e) ->
                                                sender.accept(SseEmitter.event()
                                                        .name("saving")
                                                        .data(Map.of(
                                                                        "filename", filename,
                                                                        "part", part,
                                                                        "error", Optional.ofNullable(e).map(Throwable::getMessage).orElse("")
                                                                ),
                                                                MediaType.APPLICATION_JSON
                                                        )
                                                )
                                        )
                                )
                                .toArray(CompletableFuture[]::new);
                        CompletableFuture.allOf(array)
                                .whenComplete((unused, throwable) -> {
                                            stopWatch.stop();
                                            if (Objects.nonNull(throwable)) {
                                                sseEmitter.completeWithError(throwable);
                                            } else {
                                                sender.accept(
                                                        SseEmitter.event()
                                                                .name("done")
                                                                .data(
                                                                        Map.of(
                                                                                "filename", filename,
                                                                                "totalTime", stopWatch.getTotalTimeSeconds()
                                                                        ),
                                                                        MediaType.APPLICATION_JSON
                                                                )
                                                );
                                                sseEmitter.complete();
                                            }
                                        }
                                );
                    } catch (IOException e) {
                        log.error(e.getMessage(), e);
                    }
                });
        return sseEmitter;
    }

    private void cleanUp(Path tempFile, Path tempDirectory) {
        try {
            Files.deleteIfExists(tempFile);
            log.debug("Deleted file: {}", tempFile);
            try (Stream<Path> stream = Files.list(tempDirectory)) {
                stream.sorted(Comparator.naturalOrder())
                        .forEach(path -> {
                            try {
                                log.debug("Deleted file: {}", path);
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                log.error("Failed to delete temp file: {}", path, e);
                            }
                        });
            }
            Files.deleteIfExists(tempDirectory);
        } catch (IOException e) {
            log.error("Failed to clean up temporary files", e);
        }
    }

    public String replace(String objectName, MultipartFile multipartFile) throws Exception {
        try (InputStream inputStream = multipartFile.getInputStream()) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(minioConfigProperties.getBucket())
                            .object(objectName)
                            .contentType(multipartFile.getContentType())
                            .userMetadata(Map.of("original-filename", Objects.requireNonNull(multipartFile.getOriginalFilename())))
                            .stream(inputStream, multipartFile.getSize(), -1)
                            .build()
            );
        }
        return objectName;
    }

    public String replace(String objectName, InputStream inputStream) throws Exception {
        try (inputStream) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(minioConfigProperties.getBucket())
                            .object(objectName)
                            .stream(inputStream, -1, MIN_PART_SIZE)
                            .build()
            );
        }
        return objectName;
    }

    public ResponseEntity<StreamingResponseBody> download(String objectName) throws IOException {
        HttpHeaders requestHeaders = new HttpHeaders();
        Optional.ofNullable(RequestContextHolder.getRequestAttributes())
                .map(ServletRequestAttributes.class::cast)
                .map(ServletRequestAttributes::getRequest)
                .ifPresent(httpServletRequest ->
                        StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(
                                                httpServletRequest.getHeaderNames().asIterator(),
                                                Spliterator.ORDERED
                                        ),
                                        false
                                )
                                .filter(s -> !UNNECESSARY_HEADERS.contains(s))
                                .forEach(s -> requestHeaders.addAll(s, Collections.list(httpServletRequest.getHeaders(s))))
                );
        GetObjectResponse objectResponse;
        try {
            objectResponse = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(minioConfigProperties.getBucket())
                            .object(objectName)
                            .extraHeaders(requestHeaders.toSingleValueMap())
                            .build()
            );
        } catch (ServerException e) {
            return ResponseEntity.status(e.statusCode()).build();
        } catch (MinioException | InvalidKeyException | IOException | NoSuchAlgorithmException e) {
            throw new IOException(e.getMessage(), e);
        }
        Headers objectResponseHeaders = objectResponse.headers();
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.putAll(objectResponseHeaders.toMultimap());
        responseHeaders.keySet().removeIf(header -> header.startsWith("x-amz"));
        UNNECESSARY_HEADERS.forEach(responseHeaders::remove);
        responseHeaders.setContentDisposition(
                ContentDisposition.inline()
                        .filename(
                                URLEncoder.encode(
                                                Optional.ofNullable(objectResponseHeaders.get("x-amz-meta-original-filename")).orElse(objectName),
                                                StandardCharsets.UTF_8
                                        )
                                        .replace("+", "%20")
                        )
                        .build()
        );
        final GetObjectResponse finalResponse = objectResponse;
        return ResponseEntity
                .status(responseHeaders.containsKey(HttpHeaders.CONTENT_RANGE) ? HttpStatus.PARTIAL_CONTENT : HttpStatus.OK)
                .headers(responseHeaders)
                .body(outputStream -> {
                    try (finalResponse) {
                        StreamUtils.copy(finalResponse, outputStream);
                    }
                });
    }

    private String getFilename(String filename) {
        String ext = Optional.ofNullable(filename)
                .map(FilenameUtils::getExtension)
                .map("."::concat)
                .orElse("");
        return "%s%s".formatted(UUID_GENERATOR.generate(), ext);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(minioConfigProperties.getBucket()).build())) {
            minioClient.makeBucket(
                    MakeBucketArgs.builder()
                            .bucket(minioConfigProperties.getBucket())
                            .build()
            );
        }
    }
}
