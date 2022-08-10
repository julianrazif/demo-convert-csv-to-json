package com.example.demo.config;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class PersonItemWriter implements ItemWriter<JsonObject> {

  private final ConcurrentLinkedQueue<JsonObject> counter;

  public PersonItemWriter(ConcurrentLinkedQueue<JsonObject> counter) {
    this.counter = counter;
  }

  @Override
  public void write(@NonNull List<? extends JsonObject> items) throws CustomRetryableException {
    try {
      log.info("Total person will be processed by chunk={}", items.size());
      log.info("Started . . .");
      for (JsonObject item : items) {
        counter.add(item);
        log.info("Write person object={} success", new Gson().toJson(item));
      }
      log.info("End . . .");
    } catch (Exception ex) {
      log.warn("Retry . . .");
      throw new CustomRetryableException(ex.getMessage());
    }
  }
}
