package com.apache.mesos.tachyos.config;

public enum NodeType {
  MASTER("Master"),
  WORKER("Worker");
  private String value;

  private NodeType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
