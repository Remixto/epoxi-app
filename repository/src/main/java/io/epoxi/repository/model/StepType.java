package io.epoxi.repository.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * the ETL step
 */
public enum StepType {
  
  EXTRACT("EXTRACT"),  
  TRANSFORM("TRANSFORM"),  
  LOAD("LOAD");

  private final String value;

  StepType(String value) {
    this.value = value;
  }

  @Override
  @JsonValue
  public String toString() {
    return String.valueOf(value);
  }

  @JsonCreator
  public static StepType fromValue(String text) {
    for (StepType b : StepType.values()) {
      if (String.valueOf(b.value).equals(text)) {
        return b;
      }
    }
    return null;
  }
  
}

