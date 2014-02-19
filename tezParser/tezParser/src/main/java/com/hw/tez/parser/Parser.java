package com.hw.tez.parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;

import org.plutext.jaxb.svg11.Line;
import org.plutext.jaxb.svg11.ObjectFactory;
import org.plutext.jaxb.svg11.Svg;
import org.plutext.jaxb.svg11.Title;

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
		BufferedReader reader = Files.newBufferedReader(file.toPath(),
				Charset.defaultCharset());
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
		//convertToXML(dagMap);
		SVGUtil util = new SVGUtil(dagMap);
		util.convertToXML();
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
	

	public void convertToXML(Map<String, DAG> dagMap) throws IOException,
			JAXBException {

		QName titleName = new QName("title");

		Svg svg = obj.createSvg();

		int x1 = 0;
		int y1 = 0;
		int x2 = 0;
		int y2 = 0;
		TreeMap<Long, DAG> treeMap = new TreeMap<Long, DAG>();

		for (Map.Entry<String, DAG> entry : dagMap.entrySet()) {
			if (entry.getValue() != null && entry.getValue().initTime > 0) {
				treeMap.put(entry.getValue().initTime, entry.getValue());
			}
		}

		System.out.println("Size of DAG : " + dagMap.size() + " : "
				+ treeMap.size());

		int DAG_GAP = 70;
		int VERTEX_GAP = 30;
		int TASK_GAP = 15;

		long MIN_X = Long.MAX_VALUE;
		long MAX_X = Long.MIN_VALUE;

		for (Map.Entry<Long, DAG> e : treeMap.entrySet()) {
			DAG dag = e.getValue();
			int lenInSeconds = ((int) (dag.finishTime - dag.startTime) / (1000)); // milliseconds
			MIN_X = Math.min(dag.startTime, MIN_X);
			MAX_X = Math.max(dag.finishTime, MAX_X);

			System.out.println("***********" + lenInSeconds + "*************"
					+ dag.finishTime + " : " + dag.startTime);
			Line line = createLine(x1, y1, x1 + lenInSeconds, y2);
			Title title = createTitle(dag.id + " : "
					+ (dag.finishTime - dag.startTime) + " ms");
			line.getSVGDescriptionClass().add(
					new JAXBElement<Title>(titleName, Title.class, title));
			line.setStyle("stroke: black; stroke-width:20");
			line.setOpacity("0.3");
			svg.getSVGDescriptionClassOrSVGAnimationClassOrSVGStructureClass()
					.add(line);

			// Add corresponding vertex details
			TreeSet<Vertex> vertexSet = new TreeSet<Vertex>(
					new Comparator<Vertex>() {
						@Override
						public int compare(Vertex o1, Vertex o2) {
							return (int) (o1.initRequestedTime - o2.initRequestedTime);
						}
					});
			vertexSet.addAll(dag.vertexMap.values());
			for (Vertex vertex : vertexSet) {
				y1 += VERTEX_GAP;

				lenInSeconds = ((int) (vertex.finishTime - vertex.startedTime) / 1000);
				line = createLine(x1, y1, x1 + lenInSeconds, y1);
				line.setStyle("stroke: red; stroke-width:20");
				line.setOpacity("0.3");
				String titleStr = vertex.vertexId + " : "
						+ (vertex.finishTime - vertex.startedTime)
						+ " ms : RelativeTimeToDAG : "
						+ (vertex.initRequestedTime - dag.startTime) + " ms";
				title = createTitle(titleStr);
				line.getSVGDescriptionClass().add(
						new JAXBElement<Title>(titleName, Title.class, title));
				svg.getSVGDescriptionClassOrSVGAnimationClassOrSVGStructureClass()
						.add(line);
			}

			x1 = x1 + lenInSeconds;
			y1 = y1 + DAG_GAP;
			y2 = y1;
		}
		svg.setHeight("" + y2);
		svg.setWidth("" + (MAX_X - MIN_X));

		File file = new File("test.svg");
		JAXBContext jaxbContext = JAXBContext.newInstance(Svg.class);
		Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
		jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		jaxbMarshaller.marshal(svg, file);

		BufferedReader reader = new BufferedReader(new FileReader(file));
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				"test_new.svg")));
		while (reader.ready()) {
			String line = reader.readLine().replaceAll(
					" xmlns:ns3=\"http://www.w3.org/2000/svg\" xmlns=\"\"", "");
			writer.write(line);
			writer.newLine();
		}
		reader.close();
		writer.close();

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
