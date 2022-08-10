package com.example.demo.config;

import com.example.demo.model.Person;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.*;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Configuration
public class JobConfiguration {

  private final JobLauncher jobLauncher;
  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;
  private final ResourcePatternResolver resourcePatternResolver;
  private final ConcurrentLinkedQueue<JsonObject> counter;
  private final AsyncTaskExecutor taskExecutor;
  private final PlatformTransactionManager transactionManager;

  public JobConfiguration(JobLauncher jobLauncher,
                          JobBuilderFactory jobBuilderFactory,
                          StepBuilderFactory stepBuilderFactory,
                          ResourcePatternResolver resourcePatternResolver,
                          @Qualifier("counter") ConcurrentLinkedQueue<JsonObject> counter,
                          @Qualifier("taskExecutor") AsyncTaskExecutor taskExecutor,
                          PlatformTransactionManager transactionManager) {
    this.jobLauncher = jobLauncher;
    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
    this.resourcePatternResolver = resourcePatternResolver;
    this.counter = counter;
    this.taskExecutor = taskExecutor;
    this.transactionManager = transactionManager;
  }

  private String getFilesBasePath() {
    String filesBasePath = "/etc/demo/";
    String osName = System.getProperty("os.name").toLowerCase();

    if (!osName.contains("linux")) {
      filesBasePath = System.getProperty("user.home") + File.separator;
    }

    return filesBasePath;
  }

  @Bean
  public LineMapper<Person> personLineMapper() {
    DefaultLineMapper<Person> personDefaultLineMapper = new DefaultLineMapper<>();

    DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
    tokenizer.setNames("seq", "first", "last", "birthday", "gender", "city");

    personDefaultLineMapper.setLineTokenizer(tokenizer);

    personDefaultLineMapper.setFieldSetMapper(fieldSet -> new Person(
      fieldSet.readInt("seq"),
      fieldSet.readString("first"),
      fieldSet.readString("last"),
      fieldSet.readString("birthday"),
      fieldSet.readString("gender"),
      fieldSet.readString("city")
    ));

    personDefaultLineMapper.afterPropertiesSet();

    return personDefaultLineMapper;
  }

  /*
   * Tasklet
   */
  @Bean
  public Tasklet cleanupTasklet() {
    return (contribution, chunkContext) -> {
      log.info("baseSplitFile={}", getFilesBasePath() + "csvsplit");

      List<File> listFiles = Arrays.asList(Objects.requireNonNull(new File(getFilesBasePath() + "csvsplit").listFiles()));

      listFiles.parallelStream().forEach((file) -> {
        String filename = file.getName();

        Optional<String> extension = Optional.of(filename)
          .filter(f -> f.contains("."))
          .map(f -> f.substring(filename.lastIndexOf(".") + 1));

        if (extension.isPresent() && extension.get().equals("csv")) {
          if (!file.delete()) {
            log.warn("can't delete file: " + filename);
          } else {
            log.info("deleted: " + filename);
          }
        }
      });

      return RepeatStatus.FINISHED;
    };
  }

  @Bean
  @StepScope
  public Tasklet splitTasklet(@Value("#{jobParameters['url']}") String url, @Value("#{jobParameters['maxRows']}") Long maxRows) {
    return new Tasklet() {
      private synchronized InputStream getFileInputStream() throws IOException {
        return new FileInputStream(getFilesBasePath() + url);
      }

      private int splitCSVFiles(Long maxRows, InputStream is) throws IOException {
        log.info("baseSplitFile={}", getFilesBasePath() + "csvsplit");

        int file = 0;

        try (Scanner s = new Scanner(is)) {
          int cnt = 0;

          Path path = Paths.get(getFilesBasePath() + "csvsplit" + String.format("%s_%d.%s", "/split", file, "csv"));
          BufferedWriter writer = null;

          try {
            writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW);
            String header = s.nextLine() + System.lineSeparator();

            while (s.hasNextLine()) {
              if (cnt == 0) {
                log.info("First Line={}", header);
                writer.write(header);
              }

              if (++cnt == maxRows && s.hasNextLine()) {
                writer.write(s.nextLine());
                writer.close();
                path = Paths.get(getFilesBasePath() + "csvsplit" + String.format("%s_%d.%s", "/split", ++file, "csv"));
                writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW);
                cnt = 0;
              } else {
                writer.write(s.nextLine() + System.lineSeparator());
              }
            }
          } catch (IOException ex) {
            log.error("splitCSVFiles", ex);
          } finally {
            if (writer != null) {
              writer.close();
            }
          }
        } catch (Exception ex) {
          log.error("splitCSVFiles", ex);
        }

        return file;
      }

      @Override
      public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws CustomRetryableException {
        try {
          InputStream is = getFileInputStream();
          int count = this.splitCSVFiles(maxRows, is);
          log.info("File was split on {} files", count);
          return RepeatStatus.FINISHED;
        } catch (IOException ex) {
          throw new CustomRetryableException(ex.getMessage());
        }
      }
    };
  }

  /*
   * Reader
   */
  @Bean
  @StepScope
  public FlatFileItemReader<Person> personFlatFileItemReader(@Value("#{stepExecutionContext['fileName']}") String fileName) throws MalformedURLException {
    log.info("fileName={}", fileName);

    FlatFileItemReader<Person> reader = new FlatFileItemReader<>();

    reader.setResource(new UrlResource(fileName));
    reader.setLinesToSkip(1);

    reader.setLineMapper(personLineMapper());

    return reader;
  }

  /*
   * Processor
   */
  @Bean
  public ItemProcessor personItemProcessor() {
    return new PersonItemProcessor();
  }

  /*
   * Writer
   */
  @Bean
  public ItemWriter personItemWriter() {
    return new PersonItemWriter(counter);
  }

  /*
   * Step
   */
  @Bean
  public Step cleanup() {
    return stepBuilderFactory.get("cleanup")
      .tasklet(cleanupTasklet())
      .build();
  }

  @Bean
  public Step split() {
    return stepBuilderFactory.get("split")
      .tasklet(splitTasklet(null, null))
      .build();
  }

  @Bean
  public Step splitFilesChecker() {
    return stepBuilderFactory.get("splitFilesChecker")
      .tasklet((contribution, chunkContext) -> {
        Iterator<File> ir = Arrays.stream(Objects.requireNonNull(new File(getFilesBasePath() + "csvsplit").listFiles())).iterator();
        while (ir.hasNext()) {
          log.info("Files={}", Paths.get(ir.next().toURI()));
        }
        return RepeatStatus.FINISHED;
      }).build();
  }

  @Bean
  public Step csvToPersonSlaveStep() throws Exception {
    return this.stepBuilderFactory.get("csvToPersonSlaveStep")
      .transactionManager(transactionManager)
      .<Person, Person>chunk(100)
      .reader(personFlatFileItemReader(null))
      .processor(personItemProcessor())
      .writer(personItemWriter())
      .faultTolerant()
      .retryLimit(200)
      .retry(CustomRetryableException.class)
      .throttleLimit(400)
      .build();
  }

  @Bean
  public Step csvToPersonMasterStep() throws Exception {
    MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
    partitioner.setResources(resourcePatternResolver.getResources("file:" + getFilesBasePath() + "csvsplit" + "/*.csv"));
    return stepBuilderFactory.get("csvToPersonSlaveStep")
      .partitioner(csvToPersonSlaveStep().getName(), partitioner)
      .gridSize(10)
      .step(csvToPersonSlaveStep())
      .taskExecutor(taskExecutor)
      .build();
  }

  @Bean
  Flow extract() {
    return new FlowBuilder<SimpleFlow>("extract")
      .start(split()).on("COMPLETED")
      .to(splitFilesChecker())
      .build();
  }

  @Bean
  public Step load() {
    return stepBuilderFactory.get("load")
      .tasklet((contribution, chunkContext) -> {
        log.info("Total person has been written={}", counter.size());
        return RepeatStatus.FINISHED;
      })
      .build();
  }

  @Bean
  public Step jobStepJobTransform() throws Exception {
    return stepBuilderFactory.get("jobStepJobTransform")
      .job(jobTransform())
      .launcher(jobLauncher)
      .build();
  }

  /*
   * Job
   */
  @Bean(name = "jobTransform")
  public Job jobTransform() throws Exception {
    return jobBuilderFactory.get("jobTransform")
      .start(csvToPersonMasterStep())
      .build();
  }

  @Primary
  @Bean(name = "demoJob")
  public Job demoJob() throws Exception {
    return this.jobBuilderFactory.get("demoJob")
      .listener(new JobListener())
      .start(cleanup()).on("COMPLETED")
      .to(extract())
      .next(jobStepJobTransform())
      .next(load())
      .end()
      .build();
  }
}
