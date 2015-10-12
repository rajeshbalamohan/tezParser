package com.hw.tez.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TaskAttempt {

  private static Pattern taskIdPattern =
      Pattern.compile("attempt_(\\d*)_(\\d*)_(\\d*)_(\\d*)_(\\d*)_(\\d*)");

  public String attemptId;
  public long startTime;
  public long finishTime;
  public long timeTaken;
  public String status;

  public Task task;

  public TaskAttempt(Task task) {
    this.task = task;
  }

  public static String getVertexId(String taId) {
    if (taId == null || taId.trim().length() == 0) {
      return "";
    }
    Matcher m = taskIdPattern.matcher(taId);
    StringBuilder sb = new StringBuilder();
    if (m.find()) {
      sb.append("vertex_");
      sb.append(m.group(1)).append("_");
      sb.append(m.group(2)).append("_");
      sb.append(m.group(3)).append("_");
      sb.append(m.group(4));
    }
    return sb.toString();
  }

  public static String getTaskId(String taId) {
    if (taId == null || taId.trim().length() == 0) {
      return "";
    }
    Matcher m = taskIdPattern.matcher(taId);
    StringBuilder sb = new StringBuilder();
    if (m.find()) {
      sb.append("vertex_");
      sb.append(m.group(1)).append("_");
      sb.append(m.group(2)).append("_");
      sb.append(m.group(3)).append("_");
      sb.append(m.group(4)).append("_");
      sb.append(m.group(5));
    }
    return sb.toString();
  }
}
