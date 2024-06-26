package com.github.codert96.minio;

import io.minio.*;
import lombok.RequiredArgsConstructor;
import okhttp3.Headers;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.unit.DataSize;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.StreamSupport;

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
    private final MinioClient minioClient;
    private final String bucketName;

    public String upload(MultipartFile multipartFile) throws Exception {
        String fileId = getFilename(multipartFile.getOriginalFilename());
        String filename = Objects.requireNonNullElse(multipartFile.getOriginalFilename(), fileId);
        try (InputStream inputStream = multipartFile.getInputStream()) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
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
                            .bucket(bucketName)
                            .object(fileId)
                            .stream(inputStream, -1, MIN_PART_SIZE)
                            .userMetadata(Map.of("original-filename", filename))
                            .build()
            );
        }
        return fileId;
    }

    public String replace(String objectName, MultipartFile multipartFile) throws Exception {
        try (InputStream inputStream = multipartFile.getInputStream()) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .contentType(multipartFile.getContentType())
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
                            .bucket(bucketName)
                            .object(objectName)
                            .stream(inputStream, -1, MIN_PART_SIZE)
                            .build()
            );
        }
        return objectName;
    }

    public ResponseEntity<Resource> download(String objectName) throws Exception {
        HttpHeaders requestHeaders = new HttpHeaders();
        Optional.ofNullable(RequestContextHolder.getRequestAttributes())
                .map(ServletRequestAttributes.class::cast)
                .map(ServletRequestAttributes::getRequest)
                .ifPresent(httpServletRequest -> {
                    Iterator<String> iterator = httpServletRequest.getHeaderNames().asIterator();
                    StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                            .forEach(s -> {
                                Iterator<String> stringIterator = httpServletRequest.getHeaders(s).asIterator();
                                List<String> list = StreamSupport.stream(Spliterators.spliteratorUnknownSize(stringIterator, Spliterator.ORDERED), false).toList();
                                requestHeaders.addAll(s, list);
                            });
                });

        UNNECESSARY_HEADERS.forEach(requestHeaders::remove);

        GetObjectResponse objectResponse = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .extraHeaders(requestHeaders.toSingleValueMap())
                        .build()
        );
        Headers objectResponseHeaders = objectResponse.headers();
        String filename = Optional.ofNullable(objectResponseHeaders.get("x-amz-meta-original-filename")).orElse(objectName);

        HttpHeaders responseHeaders = new HttpHeaders();

        responseHeaders.putAll(objectResponseHeaders.toMultimap());
        UNNECESSARY_HEADERS.forEach(responseHeaders::remove);
        responseHeaders.setContentDisposition(
                ContentDisposition.inline().filename(URLEncoder.encode(filename, StandardCharsets.UTF_8)).build()
        );
        List<String> list = responseHeaders.keySet().stream().filter(strings -> strings.startsWith("x-amz")).toList();
        list.forEach(responseHeaders::remove);
        HttpStatus httpStatus = responseHeaders.containsKey(HttpHeaders.CONTENT_RANGE) ? HttpStatus.PARTIAL_CONTENT : HttpStatus.OK;
        long contentLength = responseHeaders.getContentLength();
        return ResponseEntity
                .status(httpStatus)
                .headers(responseHeaders)
                .body(new InputStreamResource(objectResponse) {
                    @Override
                    public String getFilename() {
                        return filename;
                    }

                    @Override
                    public long contentLength() {
                        return contentLength;
                    }
                });
    }

    private String getFilename(String filename) {
        String string = Base64.getUrlEncoder().encodeToString(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        String ext = Optional.ofNullable(filename)
                .map(FilenameUtils::getExtension)
                .map("."::concat)
                .orElse("");
        return "%s%s".formatted(string, ext);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            minioClient.makeBucket(
                    MakeBucketArgs.builder()
                            .bucket(bucketName)
                            .build()
            );
        }
    }
}
