package com.example.demo.config;

public class CustomRetryableException extends Exception {

  public CustomRetryableException(String message) {
    super(message);
  }
}
