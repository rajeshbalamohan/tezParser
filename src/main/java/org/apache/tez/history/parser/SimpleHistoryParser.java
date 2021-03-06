/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.history.parser;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.history.logging.impl.SimpleHistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.history.parser.datamodel.BaseParser;
import org.apache.tez.history.parser.datamodel.Constants;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.EdgeInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.TaskInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

/**
 * Parser utility to parse data generated by SimpleHistoryLogging to in-memory datamodel provided
 * in org.apache.tez.history.parser.datamodel
 * <p/>
 * <p/>
 * Most of the information should be available. Minor info like VersionInfo may not be available,
 * as it is not captured in SimpleHistoryLogging.
 */
public class SimpleHistoryParser extends BaseParser {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleHistoryParser.class);
  private final File historyFile;

  public SimpleHistoryParser(File historyFile) {
    super();
    Preconditions.checkArgument(historyFile.exists(), historyFile + " does not exist");
    this.historyFile = historyFile;
  }

  /**
   * Get in-memory representation of DagInfo
   *
   * @return DagInfo
   * @throws TezException
   */
  public DagInfo getDAGData(String dagId) throws TezException {
    try {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(dagId), "Please provide valid dagId");
      dagId = dagId.trim();
      parseContents(historyFile, dagId);
      linkParsedContents();
      return dagInfo;
    } catch (IOException e) {
      LOG.error("Error in reading DAG ", e);
      throw new TezException(e);
    } catch (JSONException e) {
      LOG.error("Error in parsing DAG ", e);
      throw new TezException(e);
    }
  }

  private void populateOtherInfo(JSONObject source, JSONObject destination) throws JSONException {
    if (source == null || destination == null) {
      return;
    }
    for (Iterator it = source.keys(); it.hasNext(); ) {
      String key = (String) it.next();
      Object val = source.get(key);
      destination.put(key, val);
    }
  }

  private void populateOtherInfo(JSONObject source, String entityName,
      Map<String, JSONObject> destMap) throws JSONException {
    JSONObject destinationJson = destMap.get(entityName);
    JSONObject destOtherInfo = destinationJson.getJSONObject(Constants.OTHER_INFO);
    populateOtherInfo(source, destOtherInfo);
  }

  private void parseContents(File historyFile, String dagId)
      throws JSONException, FileNotFoundException, TezException {
    Scanner scanner = new Scanner(historyFile);
    scanner.useDelimiter(SimpleHistoryLoggingService.RECORD_SEPARATOR);
    JSONObject dagJson = null;
    Map<String, JSONObject> vertexJsonMap = Maps.newHashMap();
    Map<String, JSONObject> taskJsonMap = Maps.newHashMap();
    Map<String, JSONObject> attemptJsonMap = Maps.newHashMap();
    TezDAGID tezDAGID = TezDAGID.fromString(dagId);
    while (scanner.hasNext()) {
      String line = scanner.next();
      JSONObject jsonObject = new JSONObject(line);
      String entity = jsonObject.getString(Constants.ENTITY);
      String entityType = jsonObject.getString(Constants.ENTITY_TYPE);
      switch (entityType) {
      case Constants.TEZ_DAG_ID:
        if (!dagId.equals(entity)) {
          LOG.warn(dagId + " is not matching with " + entity);
          continue;
        }
        // Club all DAG related information together (DAG_INIT, DAG_FINISH etc). Each of them
        // would have a set of entities in otherinfo (e.g vertex mapping, dagPlan, start/finish
        // time etc).
        if (dagJson == null) {
          dagJson = jsonObject;
        }
        JSONObject otherInfo = jsonObject.optJSONObject(Constants.OTHER_INFO);
        JSONObject dagOtherInfo = dagJson.getJSONObject(Constants.OTHER_INFO);
        populateOtherInfo(otherInfo, dagOtherInfo);
        break;
      case Constants.TEZ_VERTEX_ID:
        String vertexName = entity;
        TezVertexID tezVertexID = TezVertexID.fromString(vertexName);
        if (!tezDAGID.equals(tezVertexID.getDAGId())) {
          LOG.warn(vertexName + " does not belong to " + tezDAGID);
          continue;
        }
        if (!vertexJsonMap.containsKey(vertexName)) {
          vertexJsonMap.put(vertexName, jsonObject);
        }
        otherInfo = jsonObject.optJSONObject(Constants.OTHER_INFO);
        populateOtherInfo(otherInfo, vertexName, vertexJsonMap);
        break;
      case Constants.TEZ_TASK_ID:
        String taskName = entity;
        TezTaskID tezTaskID = TezTaskID.fromString(taskName);
        if (!tezDAGID.equals(tezTaskID.getVertexID().getDAGId())) {
          LOG.warn(taskName + " does not belong to " + tezDAGID);
          continue;
        }
        if (!taskJsonMap.containsKey(taskName)) {
          taskJsonMap.put(taskName, jsonObject);
        }
        otherInfo = jsonObject.optJSONObject(Constants.OTHER_INFO);
        populateOtherInfo(otherInfo, taskName, taskJsonMap);
        break;
      case Constants.TEZ_TASK_ATTEMPT_ID:
        String taskAttemptName = entity;
        TezTaskAttemptID tezAttemptId = TezTaskAttemptID.fromString(taskAttemptName);
        if (!tezDAGID.equals(tezAttemptId.getTaskID().getVertexID().getDAGId())) {
          LOG.warn(taskAttemptName + " does not belong to " + tezDAGID);
          continue;
        }
        if (!attemptJsonMap.containsKey(taskAttemptName)) {
          attemptJsonMap.put(taskAttemptName, jsonObject);
        }
        otherInfo = jsonObject.optJSONObject(Constants.OTHER_INFO);
        populateOtherInfo(otherInfo, taskAttemptName, attemptJsonMap);
        break;
      default:
        break;
      }
    }
    scanner.close();
    if (dagJson != null) {
      this.dagInfo = DagInfo.create(dagJson);
    } else {
      LOG.error("Dag is not yet parsed. Looks like partial file.");
      throw new TezException(
          "Please provide a valid/complete history log file containing " + dagId);
    }
    for (JSONObject jsonObject : vertexJsonMap.values()) {
      VertexInfo vertexInfo = VertexInfo.create(jsonObject);
      this.vertexList.add(vertexInfo);
      LOG.debug("Parsed vertex {}", vertexInfo.getVertexName());
    }
    for (JSONObject jsonObject : taskJsonMap.values()) {
      TaskInfo taskInfo = TaskInfo.create(jsonObject);
      this.taskList.add(taskInfo);
      LOG.debug("Parsed task {}", taskInfo.getTaskId());
    }
    for (JSONObject jsonObject : attemptJsonMap.values()) {
      TaskAttemptInfo attemptInfo = TaskAttemptInfo.create(jsonObject);
      this.attemptList.add(attemptInfo);
      LOG.debug("Parsed task attempt {}", attemptInfo.getTaskAttemptId());
    }
  }

  //TODO: For quick sanity checking
  public static void main(String[] args) throws FileNotFoundException, JSONException, TezException {
    File file = new File(args[0]);
    Preconditions.checkArgument(file.exists(), file + " does not exist");
    SimpleHistoryParser parser = new SimpleHistoryParser(file);
    DagInfo info = parser.getDAGData("dag_1437098194051_0159_1");
    System.out.println(info.getVersionInfo()); //Won't be present in simplehistorylogging
    System.out.println(info.getContainerMapping());
    for (VertexInfo vertexInfo : info.getVertices()) {
      System.out.println(vertexInfo.getVertexName());
      System.out.println("\t Processor: " + vertexInfo.getProcessorClassName());
      System.out.println("\t failed tasks: " + vertexInfo.getFailedTasks());
      System.out.println("\t taskCount: " + vertexInfo.getTasks().size());
      System.out.println("\t maxTaskDuration: " + vertexInfo.getMaxTaskDuration());
      System.out.println("\t timeTaken: " + vertexInfo.getTimeTaken());
      System.out.println("\t firstTaskToStart: " + vertexInfo.getFirstTaskToStart());
      System.out.println("\t lastTaskFinished: " + vertexInfo.getLastTaskToFinish());
      for (TaskInfo task : vertexInfo.getTasks()) {
        System.out.println("\t\t Task : " + task.getTaskId());
        System.out.println("\t\t\t Task startTime: " + task.getStartTime());
        System.out.println("\t\t\t Task finishTime: " + task.getFinishTime());
        System.out.println("\t\t\t Task timeTaken: " + task.getTimeTaken());
        System.out.println("\t\t\t Task attempts: " + task.getTaskAttempts().size());
      }
      System.out.println("\t avg: " + vertexInfo.getAvgTaskDuration());
      //System.out.println("\t counters: " + vertexInfo.getTezCounters());
      for (EdgeInfo edgeInfo : vertexInfo.getInputEdges()) {
        System.out.println("\t IncomingEdge : " + edgeInfo);
      }
      for (EdgeInfo edgeInfo : vertexInfo.getOutputEdges()) {
        System.out.println("\t OutgoingEdge : " + edgeInfo);
      }
    }
  }
}