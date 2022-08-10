package com.example.demo.config;

import com.google.gson.JsonObject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ConcurrentLinkedQueue;

@Configuration
public class CommonConfiguration {

  @Bean(name = "counter")
  public ConcurrentLinkedQueue<JsonObject> counter() {
    return new ConcurrentLinkedQueue<>();
  }

  @Primary
  @Bean(name = "taskExecutor")
  public AsyncTaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(10);
    executor.setMaxPoolSize(10);
    executor.setThreadNamePrefix("Julian-");
    executor.afterPropertiesSet();
    return executor;
  }
}
