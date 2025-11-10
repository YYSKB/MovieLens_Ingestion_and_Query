package com.david.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        // 允许所有路径 (/**) 的请求
        registry.addMapping("/**")
                // 允许来自 http://localhost:63342 的请求
                .allowedOrigins("http://localhost:63342")
                // 或者在实验中，如果您不确定端口，可以允许所有来源（但生产环境不推荐）
                // .allowedOrigins("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*")
                .allowCredentials(true);
    }
}