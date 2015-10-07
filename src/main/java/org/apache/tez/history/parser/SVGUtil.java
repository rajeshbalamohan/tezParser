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

import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.TaskInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;
import org.plutext.jaxb.svg11.Line;
import org.plutext.jaxb.svg11.ObjectFactory;
import org.plutext.jaxb.svg11.Svg;
import org.plutext.jaxb.svg11.Title;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;

public class SVGUtil {

  final Svg svg = obj.createSvg();
  final QName titleName = new QName("title");

  static int MAX_DAG_RUNTIME = 0;
  static final int SCREEN_WIDTH = 1800;

  private final DagInfo dagInfo;

  //Gaps between various components
  final int DAG_GAP = 70;
  final int VERTEX_GAP = 10;
  final int TASK_GAP = 5;
  final int STROKE_WIDTH = 5;

  //To compute the size of the graph.
  long MIN_X = Long.MAX_VALUE;
  long MAX_X = Long.MIN_VALUE;

  int x1 = 0;
  int y1 = 0;
  int y2 = 0;

  static ObjectFactory obj = new ObjectFactory();

  private Line createLine(int x1, int y1, int x2, int y2) {
    Line line = obj.createLine();
    line.setX1(scaleDown(x1) + "");
    line.setY1(y1 + "");
    line.setX2(scaleDown(x2) + "");
    line.setY2(y2 + "");
    return line;
  }

  private static Title createTitle(String msg) {
    Title t = obj.createTitle();
    t.setContent(msg);
    return t;
  }

  private float scaleDown(int len) {
    return (len * 1.0f / MAX_DAG_RUNTIME) * SCREEN_WIDTH;
  }

  /**
   * Draw the DAG
   *
   * @param dagInfo
   */
  private void drawDAG(DagInfo dagInfo) {
    Title title = createTitle(dagInfo.getDagId() + " : " + dagInfo.getTimeTaken() + " ms");

    int duration = (int) dagInfo.getTimeTaken();
    MAX_DAG_RUNTIME = duration;

    MIN_X = Math.min(dagInfo.getStartTime(), MIN_X);
    MAX_X = Math.max(dagInfo.getFinishTime(), MAX_X);

    Line line = createLine(x1, y1, x1 + duration, y2);
    line.getSVGDescriptionClass().add(new JAXBElement<Title>(titleName, Title.class, title));
    line.setStyle("stroke: black; stroke-width:20");
    line.setOpacity("0.3");
    svg.getSVGDescriptionClassOrSVGAnimationClassOrSVGStructureClass().add(line);

    drawVertex(dagInfo);
  }

  /**
   * Draw the vertices
   *
   * @param dag
   */
  public void drawVertex(DagInfo dag) {
    Collection<VertexInfo> vertices = dag.getVertices();
    // Add corresponding vertex details
    TreeSet<VertexInfo> vertexSet = new TreeSet<VertexInfo>(
        new Comparator<VertexInfo>() {
          @Override
          public int compare(VertexInfo o1, VertexInfo o2) {
            return (int) (o1.getFirstTaskToStart().getStartTime() - o2
                .getFirstTaskToStart().getStartTime());
          }
        });
    vertexSet.addAll(vertices);
    for (VertexInfo vertex : vertexSet) {
      //Set vertex start time as the one when its first task attempt started executing
      x1 = (int) vertex.getStartTime();
      y1 += VERTEX_GAP;

      int duration = ((int) (vertex.getTimeTaken()));
      Line line = createLine(x1, y1, x1 + duration, y1);
      line.setStyle("stroke: red; stroke-width:" + STROKE_WIDTH);
      line.setOpacity("0.3");
      String titleStr = vertex.getVertexName() + ":"
          + (vertex.getTimeTaken())
          + " ms, RelativeTimeToDAG:"
          + (vertex.getInitTime() - dag.getStartTime())
          + " ms, counters:" + vertex.getTezCounters();
      Title title = createTitle(titleStr);
      line.getSVGDescriptionClass().add(
          new JAXBElement<Title>(titleName, Title.class, title));
      svg.getSVGDescriptionClassOrSVGAnimationClassOrSVGStructureClass()
          .add(line);
      // For each vertex, draw the tasks
      drawTask(vertex);
    }

    x1 = x1 + (int) dagInfo.getTimeTaken();
    y1 = y1 + DAG_GAP;
    y2 = y1;
  }

  /**
   * Draw tasks
   *
   * @param vertex
   */
  public void drawTask(VertexInfo vertex) {
    Collection<TaskInfo> tasks = vertex.getTasks();
    // Add corresponding task details
    TreeSet<TaskInfo> taskSet = new TreeSet<TaskInfo>(new Comparator<TaskInfo>() {
      @Override
      public int compare(TaskInfo o1, TaskInfo o2) {
        if (o1.getSuccessfulTaskAttempt() != null && o2.getSuccessfulTaskAttempt() != null) {
          return (int) (o1.getSuccessfulTaskAttempt().getStartTime()
              - o2.getSuccessfulTaskAttempt().getStartTime());
        } else {
          return -1;
        }
      }
    });
    taskSet.addAll(tasks);
    for (TaskInfo task : taskSet) {
      for (TaskAttemptInfo taskAttemptInfo : task.getTaskAttempts()) {
        x1 = (int) taskAttemptInfo.getStartTime();
        y1 += TASK_GAP;

        int duration = (int) taskAttemptInfo.getTimeTaken();
        Line line = createLine(x1, y1, x1 + duration, y1);
        line.setStyle("stroke: green; stroke-width:" + STROKE_WIDTH);

        String titleStr = "RelativeTimeToVertex:"
            + (task.getStartTime() - vertex.getInitTime()) + " ms, "
            + task.toString() + ", counters:" + task.getTezCounters();
        Title title = createTitle(titleStr);
        line.getSVGDescriptionClass().add(
            new JAXBElement<Title>(titleName, Title.class, title));
        svg.getSVGDescriptionClassOrSVGAnimationClassOrSVGStructureClass()
            .add(line);

      }
    }
  }

  /**
   * Convert DAG to graph
   *
   * @throws java.io.IOException
   * @throws javax.xml.bind.JAXBException
   */
  public void convertToXML(String fileName) throws IOException, JAXBException {
    drawDAG(dagInfo);

    svg.setHeight("" + y2);
    svg.setWidth("" + (MAX_X - MIN_X));

    String tempFileName = System.nanoTime() + ".svg";
    File file = new File(tempFileName);
    JAXBContext jaxbContext = JAXBContext.newInstance(Svg.class);
    Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
    jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    jaxbMarshaller.marshal(svg, file);

    //TODO: dirty workaround to get rid of XMLRootException issue
    BufferedReader reader = new BufferedReader(new FileReader(file));
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileName)));
    while (reader.ready()) {
      String line = reader.readLine().replaceAll(
          " xmlns:ns3=\"http://www.w3.org/2000/svg\" xmlns=\"\"", "");
      writer.write(line);
      writer.newLine();
    }
    reader.close();
    writer.close();
    file.delete();
  }

  public SVGUtil(DagInfo dagInfo) {
    this.dagInfo = dagInfo;
  }

}
