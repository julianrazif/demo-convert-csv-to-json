package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Slf4j
@EnableAsync
@EnableTransactionManagement
@EnableBatchProcessing
@SpringBootApplication
public class DemoApplication {

  private final JobLauncher jobLauncher;
  private final Job job;

  public DemoApplication(JobLauncher jobLauncher, @Qualifier("demoJob") Job job) {
    this.jobLauncher = jobLauncher;
    this.job = job;
  }

  public static void main(String[] args) {
    SpringApplication.run(DemoApplication.class, args);
  }

  @Bean
  public CommandLineRunner runner() {
    return args -> {
      log.info("Proses ETL pada file CSV yang berjumlah 1jt row data person");

      JobParameters jobParameters = new JobParametersBuilder()
        .addString("url", "convertcsv.csv")
        .addLong("maxRows", 99999L)
        .toJobParameters();

      JobExecution execution = jobLauncher.run(job, jobParameters);
      log.info("STATUS :: {}", execution.getStatus());
    };
  }

}
