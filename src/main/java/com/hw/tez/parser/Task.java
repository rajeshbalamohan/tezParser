package com.hw.tez.parser;

import com.google.common.collect.Maps;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TaskCounter;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Task implements IParsable<Task> {

  private static Pattern taskIdPattern =
      Pattern.compile("task_(\\d*)_(\\d*)_(\\d*)_(\\d*)_(\\d*)");

  public String taskId;
  public long startTime;
  public long finishTime;
  public long timeTaken;
  public String status;

  public Vertex vertex;

  public Map<String, String> TASK_COUNTERS_MAP = Maps.newHashMap();

  public Task(Vertex vertex) {
    this.vertex = vertex;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("taskId: ");
    sb.append(taskId).append(", startTime: ");
    sb.append(startTime).append(", finishTime: ");
    sb.append(finishTime).append(", timeTaken: ");
    sb.append(timeTaken).append(", status: ");
    sb.append(status);
    sb.append(" , Counters ").append(TASK_COUNTERS_MAP);
    return sb.toString();
  }

  public static String getVertexId(String tId) {
    if (tId == null || tId.trim().length() == 0) {
      return "";
    }
    Matcher m = taskIdPattern.matcher(tId);
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

  @Override public void parse(Map<String, String> map) {
    if (map.get(Constants.TASK_ID) != null) {
      taskId = map.get(Constants.TASK_ID);
      status = map.get(Constants.STATUS);
      if (startTime == 0) {
        startTime = Long.parseLong(map.get(Constants.START_TIME) == null ? "0" :
            map.get(Constants.START_TIME));
      }
      if (finishTime == 0) {
        finishTime = Long.parseLong(
            map.get(Constants.FINISH_TIME) == null ? "0" :
                map.get(Constants.FINISH_TIME));
      }
      if (timeTaken == 0) {
        timeTaken = Long.parseLong(map.get(Constants.TIME_TAKEN) == null ? "0" :
            map.get(Constants.TIME_TAKEN));
      }

      for (TaskCounter counter : TaskCounter.values()) {
        if (map.get(counter.name()) != null) {
          TASK_COUNTERS_MAP.put(counter.name(), map.get(counter.name()));
        }
      }

      for (DAGCounter counter : DAGCounter.values()) {
        if (map.get(counter.name()) != null) {
          TASK_COUNTERS_MAP.put(counter.name(), map.get(counter.name()));
        }
      }

    }

  }
}
