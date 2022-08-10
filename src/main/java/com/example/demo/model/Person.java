package com.example.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class Person {

  private final Integer seq;
  private final String first;
  private final String last;
  private final String birthDay;
  private final String gender;
  private final String city;
}
