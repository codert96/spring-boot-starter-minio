package com.github.codert96.minio.config;


import com.github.codert96.minio.MinioFileTemplate;
import com.github.codert96.minio.bean.MinioConfigProperties;
import io.minio.MinioClient;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@RequiredArgsConstructor
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MinioConfigProperties.class)
@ConditionalOnProperty(prefix = "minio", name = "endpoint")
public class MinioConfig {
    private final MinioConfigProperties minioConfigProperties;

    @Bean
    public MinioClient minioClient() {
        return new MinioClient.Builder()
                .endpoint(minioConfigProperties.getEndpoint())
                .credentials(minioConfigProperties.getAccessKey(), minioConfigProperties.getSecretKey())
                .region(minioConfigProperties.getRegion())
                .build();
    }

    @Bean
    public MinioFileTemplate minioFileTemplate(MinioClient minioClient) {
        return new MinioFileTemplate(minioClient, minioConfigProperties);
    }
}
