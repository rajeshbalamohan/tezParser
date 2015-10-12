package com.hw.tez.parser;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TaskCounter;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Vertex implements IParsable<Vertex> {

  //All fields are pulbic. Only for parsing. No need for annoying gettings

  public String vertexName;
  public String vertexId;
  public long initRequestedTime;
  public long initedTime;
  public long startRequestedTime;
  public long startedTime;
  public long finishTime;
  public long timeTaken;
  public long numTasks;
  public String status;

  public DAG dag; //Not safe, but who cares. This is for reverse mapping

  //Map of tasks
  public Map<String, Task> taskMap = new TreeMap<String, Task>();

  public static Set<String> VERTEX_COUNTERS = Sets.newHashSet();

  public static Map<String, String> VERTEX_COUNTERS_MAP = Maps.newHashMap();

  public Vertex(DAG dag) {
    this.dag = dag;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("vertexId: ");
    sb.append(vertexId).append(", vertexName: ");
    sb.append(vertexName).append(", initRequestedTime: ");
    sb.append(initRequestedTime).append(", initedTime: ");
    sb.append(initedTime).append(", startRequestedTime: ");
    sb.append(startRequestedTime).append(", startedTime: ");
    sb.append(startedTime).append(", finishTime: ");
    sb.append(finishTime).append(", timeTaken: ");
    sb.append(timeTaken).append(", status: ");
    sb.append(status).append(", numTasks: ");
    sb.append(numTasks);
    sb.append("\n");

    for (Map.Entry<String, Task> entry : taskMap.entrySet()) {
      sb.append("\t\t");
      sb.append(entry.getValue().toString());
      sb.append("\n");
    }

    return sb.toString();
  }

  public long initTime() {
    return (initedTime > 0) ? (initedTime - initRequestedTime) : -1;
  }

  public long startRequestedTime() {
    return (startedTime > 0) ? (startedTime - startRequestedTime) : -1;
  }

  public long totalTimeTaken() {
    return (finishTime > 0) ? (finishTime - startedTime) : -1;
  }

  @Override public void parse(Map<String, String> map) {
    if (map.get(Constants.VERTEX_ID) != null) {
      vertexId = map.get(Constants.VERTEX_ID);
      vertexName = map.get(Constants.VERTEX_NAME);
      status = map.get(Constants.STATUS);
      if (initRequestedTime == 0) {
        initRequestedTime = Long.parseLong(
            map.get(Constants.INIT_REQUESTED_TIME) == null ? "0" :
                map.get(Constants.INIT_REQUESTED_TIME));
      }
      if (initedTime == 0) {
        initedTime = Long.parseLong(
            map.get(Constants.INITED_TIME) == null ? "0" :
                map.get(Constants.INITED_TIME));
      }
      if (numTasks == 0) {
        numTasks = Long.parseLong(
            map.get(Constants.VERTEX_TASKS) == null ? "0" :
                map.get(Constants.VERTEX_TASKS));
      }
      if (startRequestedTime == 0) {
        startRequestedTime = Long.parseLong(
            map.get(Constants.START_REQUESTED_TIME) == null ? "0" :
                map.get(Constants.START_REQUESTED_TIME));
      }
      if (startedTime == 0) {
        startedTime = Long.parseLong(
            map.get(Constants.STARTED_TIME) == null ? "0" :
                map.get(Constants.STARTED_TIME));
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
    }
    for (TaskCounter counter : TaskCounter.values()) {
      if (map.get(counter.name()) != null) {
        VERTEX_COUNTERS_MAP.put(counter.name(), map.get(counter.name()));
      }
    }

    for (DAGCounter counter : DAGCounter.values()) {
      if (map.get(counter.name()) != null) {
        VERTEX_COUNTERS_MAP.put(counter.name(), map.get(counter.name()));
      }
    }
  }

}
