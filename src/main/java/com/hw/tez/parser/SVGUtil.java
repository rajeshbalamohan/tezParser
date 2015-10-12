package com.hw.tez.parser;

import org.apache.tez.common.counters.TaskCounter;
import org.plutext.jaxb.svg11.Line;
import org.plutext.jaxb.svg11.ObjectFactory;
import org.plutext.jaxb.svg11.Svg;
import org.plutext.jaxb.svg11.Text;
import org.plutext.jaxb.svg11.Title;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Need to refactor code a lot.  Every task, vertex, taskattempt should able to generate a SVG code of their own. (more like visitor)
 */
public class SVGUtil {

  Svg svg = obj.createSvg();
  QName titleName = new QName("title");

  private Map<String, DAG> dagMap;

  //Gaps between various components
  int DAG_GAP = 70;
  int VERTEX_GAP = 30;
  int TASK_GAP = 15;

  //To compute the size of the graph.
  long MIN_X = Long.MAX_VALUE;
  long MAX_X = Long.MIN_VALUE;

  int x1 = 0;
  int y1 = 0;
  int x2 = 0;
  int y2 = 0;

  long currentDAGLenInSeconds = 0;

  static ObjectFactory obj = new ObjectFactory();

  private static Line createLine(int x1, int y1, long x2, int y2) {
    Line line = obj.createLine();
    line.setX1(x1 + "");
    line.setY1(y1 + "");
    line.setX2(x2 + "");
    line.setY2(y2 + "");
    return line;
  }

  private static Title createTitle(String msg) {
    Title t = obj.createTitle();
    t.setContent(msg);
    return t;
  }

  private void addLineToSVG(Line line) {
    svg.getSVGDescriptionClassOrSVGAnimationClassOrSVGStructureClass()
        .add(obj.createLine(line));
  }

  private void addTextToSVG(Text text) {
    svg.getSVGDescriptionClassOrSVGAnimationClassOrSVGStructureClass()
        .add(obj.createText(text));
  }

  /**
   * Draw the DAG
   *
   * @param treeMap
   */
  private void drawDAG(TreeMap<Long, DAG> treeMap) {
    DAG firstDag = null;
    for (Map.Entry<Long, DAG> e : treeMap.entrySet()) {
      DAG dag = e.getValue();
      int lenInSeconds =
          ((int) (dag.finishTime - dag.startTime) / (1000)); // milliseconds

      if (lenInSeconds <= 0) {
        //Possibly an incomplete DAG. Draw until the last info is available
        long maxFinishTime = -1;
        long minStartTime = Long.MAX_VALUE;
        for (Vertex vertex : dag.vertexMap.values()) {
          for (Task task : vertex.taskMap.values()) {
            if (task.finishTime > maxFinishTime) {
              maxFinishTime = task.finishTime;
            } else if (task.startTime > maxFinishTime) {
              maxFinishTime = task.startTime;
            }
          }
          if (vertex.startedTime < minStartTime) {
            minStartTime = vertex.startedTime;
          }
        }

        System.out.println("Resetting DAG finish time to : " + maxFinishTime);
        lenInSeconds = (int) ((maxFinishTime - minStartTime) / 1000);
        currentDAGLenInSeconds = lenInSeconds;
      }

      if (firstDag != null) {
        x1 = (int) ((dag.startTime - firstDag.startTime) / 1000);
      } else {
        firstDag = dag;
      }
      MIN_X = Math.min(dag.startTime, MIN_X);
      MAX_X = Math.max(dag.finishTime, MAX_X);

      System.out.println(
          "***********" + lenInSeconds + "*************" + dag.finishTime
              + " : " + dag.startTime);
      Line line = createLine(x1, y1, x1 + lenInSeconds, y2);
      Title title = createTitle(
          dag.id + " : " + (dag.finishTime - dag.startTime) + " ms. " + dag
              .toString());
      line.getSVGDescriptionClass().add(obj.createTitle(title));
      line.setStyle("stroke: black; stroke-width:20");
      line.setDisplay("Test");

      addLineToSVG(line);

      drawVertex(dag);
      y1 = y1 + DAG_GAP;
      y2 = y1;
    }
  }

  /**
   * Draw the vertices
   *
   * @param dag
   */
  public void drawVertex(DAG dag) {
    Collection<Vertex> vertices = dag.vertexMap.values();
    // Add corresponding vertex details
    TreeSet<Vertex> vertexSet = new TreeSet<Vertex>(new Comparator<Vertex>() {
      @Override public int compare(Vertex o1, Vertex o2) {
        return (int) (o1.initRequestedTime - o2.initRequestedTime);
      }
    });
    vertexSet.addAll(vertices);
    for (Vertex vertex : vertexSet) {

      x1 += (int) (vertex.initRequestedTime - dag.startTime) / 1000;
      y1 += DAG_GAP;

      int lenInSeconds =
          ((int) (vertex.finishTime - vertex.startedTime) / 1000);

      if (vertex.initRequestedTime <= 0 || dag.startTime <= 0) {
        System.out.println(
            "Error in vertex initReq=" + vertex.initRequestedTime + ", dag"
                + ".startTime=" + dag.startTime);
        lenInSeconds = (int) currentDAGLenInSeconds;
      }

      Line line = createLine(x1, y1, x1 + lenInSeconds, y1);
      line.setStyle("stroke: red; stroke-width:20");
      line.setOpacity("0.3");
      String titleStr =
          vertex.vertexId + " : " + (vertex.finishTime - vertex.startedTime)
              + " ms : RelativeTimeToDAG : " + (vertex.initRequestedTime
              - dag.startTime) + " ms. " + vertex.toString();
      Title title = createTitle(titleStr);
      line.getSVGDescriptionClass().add(obj.createTitle(title));
      addLineToSVG(line);
      // For each vertex, draw the tasks
      drawTask(vertex);
    }
  }

  private long getCounter(Map<String, String> counterMap, String counter) {
    long retValue = 0;
    String val = counterMap.get(counter);
    if (val != null) {
      retValue = Long.parseLong(val);
    }
    return retValue;
  }

  private void setOpacity(Line line, Task task) {
    /*
		if (getCounter(task.TASK_COUNTERS_MAP, DAGCounter.RACK_LOCAL_TASKS.name()) > 0) {
			line.setOpacity("0.8");
		} else {
			line.setOpacity("0.3");
		}
		
		//check if spills are happening
		long inputRecords = getCounter(task.TASK_COUNTERS_MAP, TaskCounter.MAP_INPUT_RECORDS.name());
		long spilledRecords = getCounter(task.TASK_COUNTERS_MAP, TaskCounter.SPILLED_RECORDS.name());
		
		if (spilledRecords > (2*inputRecords)) {
			System.out.println("Spilled!!!! : " + task);
			line.setOpacity("0.9");
		} else {
			line.setOpacity("0.3");
		}
		*/
  }

  /**
   * Draw tasks
   *
   * @param vertex
   */
  public void drawTask(Vertex vertex) {
    Collection<Task> tasks = vertex.taskMap.values();
    // Add corresponding task details
    TreeSet<Task> taskSet = new TreeSet<Task>(new Comparator<Task>() {
      @Override public int compare(Task o1, Task o2) {
        return (int) (o1.startTime - o2.startTime);
      }
    });
    taskSet.addAll(tasks);
    int origX1 = x1;
    for (Task task : taskSet) {
      y1 += VERTEX_GAP;

      int lenInSeconds = ((int) (task.finishTime - task.startTime) / 1000);
      Text text = obj.createText();

      x1 = origX1 + ((int) (task.startTime - vertex.startedTime) / 1000);

      if (task.finishTime <= 0 || task.startTime <= 0) {
        System.out.println(
            "Error in task finishTime=" + task.finishTime + ", task"
                + ".startTime=" + task.startTime);
        lenInSeconds = (int) currentDAGLenInSeconds;
      }

      Line line = createLine(x1, y1, x1 + lenInSeconds, y1);
      if (task.TASK_COUNTERS_MAP.get(TaskCounter.INPUT_RECORDS_PROCESSED.name())
          != null) {
        // TODO: mapper??
        line.setStyle("stroke: green; stroke-width:20");
        text.setDisplay("MAP");
      } else if (
          task.TASK_COUNTERS_MAP.get(TaskCounter.REDUCE_INPUT_GROUPS.name())
              != null) {
        // TODO: reducer??
        line.setStyle("stroke: darkmagenta; stroke-width:20");
        text.setDisplay("REDUCE");
      } else {
        // TODO: ??
        line.setStyle("stroke: darkblue; stroke-width:20");
        text.setDisplay("????");
      }

      if (x1 + lenInSeconds > 5) { // greater than 5 seconds
        text.setX("" + x1 + 2);
        text.setY(y1 + "");
        addTextToSVG(text);
      }

      setOpacity(line, task);

      String titleStr = "RelativeTimeToVertex : " + (task.startTime
          - vertex.initRequestedTime) + " ms : " + task.toString();
      Title title = createTitle(titleStr);
      line.getSVGDescriptionClass().add(obj.createTitle(title));
      addLineToSVG(line);

    }
  }

  /**
   * Convert DAG to graph
   *
   * @throws IOException
   * @throws JAXBException
   */
  public void convertToXML(String fileName) throws IOException, JAXBException {
    TreeMap<Long, DAG> treeMap = new TreeMap<Long, DAG>();

    for (Map.Entry<String, DAG> entry : dagMap.entrySet()) {
      if (entry.getValue() != null && entry.getValue().initTime > 0) {
        treeMap.put(entry.getValue().initTime, entry.getValue());
      }
    }
    System.out
        .println("Size of DAG : " + dagMap.size() + " : " + treeMap.size());

    drawDAG(treeMap);

    svg.setHeight("" + y2);
    if ((MAX_X - MIN_X) <= 0) {
      svg.setWidth("" + 100000);
    } else {
      svg.setWidth("" + (MAX_X - MIN_X));
    }

    File file = new File(fileName + ".svg");
    JAXBContext jaxbContext = JAXBContext.newInstance(Svg.class);
    Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
    jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    jaxbMarshaller.marshal(svg, file);
    System.out.println("Wrote SVG to " + file.getAbsolutePath());
  }

  /**
   * Constructor
   *
   * @param dagMap
   */
  public SVGUtil(Map<String, DAG> dagMap) {
    this.dagMap = dagMap;
  }

}
