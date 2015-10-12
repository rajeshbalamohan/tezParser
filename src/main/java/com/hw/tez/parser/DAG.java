package com.hw.tez.parser;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TaskCounter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DAG implements IParsable<DAG> {

  enum EVENT {
    DAG("DAG"), VERTEX("VERTEX"), TASK("TASK"), TASK_ATTEMPT("TASK_ATTEMPT");

    private String event;

    EVENT(String event) {
      this.event = event;
    }

    public static EVENT getEvent(String event) {
      if (event.contains("DAG_")) {
        return DAG;
      } else if (event.contains("TASK_ATTEMPT_")) {
        return TASK_ATTEMPT;
      } else if (event.contains("VERTEX_")) {
        return VERTEX;
      } else if (event.contains("TASK_")) {
        return TASK;
      }
      return null;
    }
  }

  public Map<String, String> DAG_COUNTERS_MAP = Maps.newTreeMap();

  public String id;
  public long submitTime;
  public long initTime;
  public long startTime;
  public long finishTime;

  public Map<String, Vertex> vertexMap = new LinkedHashMap<String, Vertex>();

  //containerId to list of task attempts
  public Map<String, List<AttemptDetails>> containerMap = Maps.newHashMap();

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("id: ");
    sb.append(id).append(", submitTime: ");
    sb.append(submitTime).append(", initTime: ");
    sb.append(initTime).append(", startTime: ");
    sb.append(startTime).append(", finishTime: ");
    sb.append(finishTime).append(", timeTaken: ")
        .append(finishTime - startTime);
    sb.append(" , Counters").append(DAG_COUNTERS_MAP);
    sb.append("\n");

    for (Map.Entry<String, Vertex> entry : vertexMap.entrySet()) {
      sb.append("\t");
      sb.append(entry.getValue().toString());
      sb.append("\n");
    }

    return sb.toString();
  }

  public void parseDAG(Map<String, String> map) {
    if (map.get(Constants.DAG) != null) {
      id = map.get(Constants.DAG);
      submitTime = Long.parseLong(map.get(Constants.SUBMIT_TIME) == null ?
          ((submitTime == 0) ? "0" : submitTime + "") :
          map.get(Constants.SUBMIT_TIME));
      initTime = Long.parseLong(map.get(Constants.INIT_TIME) == null ?
          ((initTime == 0) ? "0" : initTime + "") :
          map.get(Constants.INIT_TIME));
      startTime = Long.parseLong(map.get(Constants.START_TIME) == null ?
          ((startTime == 0) ? "0" : startTime + "") :
          map.get(Constants.START_TIME));
      finishTime = Long.parseLong(map.get(Constants.FINISH_TIME) == null ?
          ((finishTime == 0) ? "0" : finishTime + "") :
          map.get(Constants.FINISH_TIME));
      for (DAGCounter counter : DAGCounter.values()) {
        if (map.get(counter.name()) != null)
          DAG_COUNTERS_MAP.put(counter.name(), map.get(counter.name()));
      }
      for (TaskCounter counter : TaskCounter.values()) {
        if (map.get(counter.name()) != null) {
          DAG_COUNTERS_MAP.put(counter.name(), map.get(counter.name()));
        }
      }
    }
  }

  public void parseVertex(Map<String, String> map) {
    String vertexId = map.get(Constants.VERTEX_ID);
    Vertex vertex = vertexMap.get(vertexId);
    if (vertex == null) {
      vertex = new Vertex(this);
      vertex.vertexId = vertexId;
      vertexMap.put(vertexId, vertex);
    }
    vertex.parse(map);
  }

  public void parseTask(Map<String, String> map) {
    String taskId = map.get(Constants.TASK_ID);
    String vertexId = Task.getVertexId(taskId);
    for (Map.Entry<String, Vertex> entry : vertexMap.entrySet()) {
      Vertex v = entry.getValue();
      if (v.vertexId.equalsIgnoreCase(vertexId)) {
        Task task = v.taskMap.get(taskId);
        if (task == null) {
          task = new Task(entry.getValue());
          v.taskMap.put(taskId, task);
        }
        task.parse(map);
      }
    }
  }

  public void parseTaskAttempt(Map<String, String> map) {
    String containerId = map.get(Constants.CONTAINER_ID);
    if (containerId != null) {
      List<AttemptDetails> attemptDetails = containerMap.get(containerId);
      if (attemptDetails == null) {
        attemptDetails = Lists.newLinkedList();
        containerMap.put(containerId, attemptDetails);
      }
      AttemptDetails details = new AttemptDetails();
      details.attemptId = map.get(Constants.TASK_ATTEMPT_ID);
      details.containerId = containerId;
      details.nodeId = map.get(Constants.NODE_ID);
      details.vertexName = map.get(Constants.VERTEX_NAME);
      attemptDetails.add(details);
    }
  }

  @Override public void parse(Map<String, String> map) {
    EVENT event = EVENT.getEvent(map.get(Constants.EVENT));
    if (event == null) {
      return;
    }
    switch (event) {
    case DAG:
      parseDAG(map);
      break;
    case VERTEX:
      parseVertex(map);
      break;
    case TASK_ATTEMPT:
      parseTaskAttempt(map);
      break;
    case TASK:
      parseTask(map);
      break;
    default:
      break;
    }
  }

  static class AttemptDetails {
    String attemptId;
    String vertexName;
    String containerId;
    String nodeId;
  }
}
