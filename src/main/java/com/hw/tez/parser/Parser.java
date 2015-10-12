package com.hw.tez.parser;

import org.plutext.jaxb.svg11.Line;
import org.plutext.jaxb.svg11.ObjectFactory;
import org.plutext.jaxb.svg11.Title;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class Parser {
  public static String DAG = "DAG";

  private Map<String, DAG> dagMap = new TreeMap<String, DAG>();

  File file;

  public Parser(File file) {
    this.file = file;
  }

  /**
   * Parse the file
   *
   * @param file
   * @throws IOException
   */
  public void parse(File file) throws Exception {
    BufferedReader reader =
        Files.newBufferedReader(file.toPath(), Charset.defaultCharset());
    while (reader.ready()) {
      String line = reader.readLine();
      if (line.contains("HISTORY")) {
        line = line.substring(line.indexOf("HISTORY"));
        line = line.replaceAll("HISTORY\\]", "");
        line = line.replaceAll("\\[", "").replaceAll("\\]:", ",")
            .replaceAll("\\]", ",");
        line = line.replaceAll("DAG:", "DAG=");
        line = line.replaceAll("Event:", "Event=");
        Map<String, String> map = parseLine(line);
        String event = map.get(Constants.EVENT); // has to be present
        parseDAG(map);
      }
    }
    System.out.println(dagMap);
    SVGUtil util = new SVGUtil(dagMap);
    util.convertToXML(file.getName());
  }

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

  /**
   * Parse a line
   *
   * @param line
   * @return
   */
  private Map<String, String> parseLine(String line) {
    Map<String, String> map = new LinkedHashMap<String, String>();
    String[] data = line.split(",");
    for (String datum : data) {
      String[] kv = datum.split("=");
      if (kv.length > 1) {
        map.put(kv[0].trim(), kv[1].trim());
      }
    }
    return map;
  }

  private DAG parseDAG(Map<String, String> map) {
    String dagId = map.get(Constants.DAG);
    DAG dag = null;
    if (dagId != null) {
      dag = (dagMap.containsKey(dagId)) ? dagMap.get(dagId) : new DAG();
      dag.parse(map);
      dagMap.put(dagId, dag);
    }
    return dag;
  }

  public static void main(String[] args) throws Exception {
    // File file = new File(
    // "/Users/rbalamohan/Downloads/history.log");
    File file = new File(args[0]);
    Parser parser = new Parser(file);
    parser.parse(file);
  }

}
