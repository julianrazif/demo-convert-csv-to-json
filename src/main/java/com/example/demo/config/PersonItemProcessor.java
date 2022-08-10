package com.example.demo.config;

import com.example.demo.model.Person;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class PersonItemProcessor implements ItemProcessor<Person, JsonObject> {

  @Override
  public JsonObject process(Person person) throws Exception {
    log.info("Process person={}", person);
    JsonObject personJsonObject = new JsonObject();
    personJsonObject.addProperty("full name", person.getFirst().concat(" ").concat(person.getLast()));
    personJsonObject.addProperty("birthday", person.getBirthDay());
    personJsonObject.addProperty("gender", person.getGender());
    personJsonObject.addProperty("city", person.getCity());
    return personJsonObject;
  }
}
