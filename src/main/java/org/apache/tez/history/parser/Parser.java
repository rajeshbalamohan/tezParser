package org.apache.tez.history.parser;

import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.TaskInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;

import java.io.File;

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
public class Parser {

  public static void main(String[] args) throws Exception {
    /*
    SimpleHistoryParser parser = new SimpleHistoryParser(new File(args[0]));

    String dagId = args[1];
    Preconditions.checkArgument(dagId != null, "Please enter valid dagId");
    DagInfo dagInfo = parser.getDAGData(dagId);

    SVGUtil svgUtil = new SVGUtil(dagInfo);
    svgUtil.convertToXML("sample.svg");

    System.out.println("Done");
    */

    ATSFileParser parser = new ATSFileParser(new File(args[0]));
    DagInfo dagInfo = parser.getDAGData(args[1]);
    for(VertexInfo vertexInfo : dagInfo.getVertices()) {
      for(TaskInfo taskInfo : vertexInfo.getTasks()) {
        for(TaskAttemptInfo attemptInfo : taskInfo.getTaskAttempts()) {
          System.out.println(attemptInfo.getTaskInfo().getVertexInfo().getVertexName()
              + " : " +  attemptInfo);
          System.out.println("\t" + attemptInfo.getTezCounters());
        }
      }

    }
    SVGUtil svgUtil = new SVGUtil(dagInfo);
    svgUtil.convertToXML("sample.svg");
    System.out.println("Done");

  }
}
