package io.github.codert96.minio.config;


import io.github.codert96.minio.MinioFileTemplate;
import io.github.codert96.minio.bean.MinioConfigProperties;
import io.minio.MinioClient;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@RequiredArgsConstructor
@Configuration
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
    public MinioFileTemplate minioFileTemplate(ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        return new MinioFileTemplate(minioClient(), minioConfigProperties, threadPoolTaskExecutor);
    }
}
