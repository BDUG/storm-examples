package de.widas.examples.deltastepping.utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.math.NumberUtils;

import de.widas.examples.deltastepping.model.Edge;
import de.widas.examples.deltastepping.model.Vertex;

public class GraphUtils {

    private static List<Vertex> allVertexes = new LinkedList<Vertex>();
    private static List<Edge> allEdges = new LinkedList<Edge>();
    private static Map<String, String> allShortestPathes = new HashMap<String, String>();

    public static void initStandardGraph() {
	Vertex v0 = new Vertex();
	v0.setName("0");
	getAllVertexes().add(v0);
	Vertex v1 = new Vertex();
	v1.setName("1");
	getAllVertexes().add(v1);
	Vertex v2 = new Vertex();
	v2.setName("2");
	getAllVertexes().add(v2);
	Vertex v3 = new Vertex();
	v3.setName("3");
	getAllVertexes().add(v3);
	Vertex v4 = new Vertex();
	v4.setName("4");
	getAllVertexes().add(v4);
	Vertex v5 = new Vertex();
	v5.setName("5");
	getAllVertexes().add(v5);
	Vertex v6 = new Vertex();
	v6.setName("6");
	getAllVertexes().add(v6);
	Edge e1 = new Edge();
	e1.setWeight(1);
	e1.setFrom(v0);
	e1.setTo(v2);
	getAllEdges().add(e1);
	Edge e2 = new Edge();
	e2.setWeight(13);
	e2.setFrom(v0);
	e2.setTo(v1);
	getAllEdges().add(e2);
	Edge e3 = new Edge();
	e3.setWeight(2);
	e3.setFrom(v2);
	e3.setTo(v1);
	getAllEdges().add(e3);
	Edge e4 = new Edge();
	e4.setWeight(7);
	e4.setFrom(v2);
	e4.setTo(v3);
	getAllEdges().add(e4);
	Edge e5 = new Edge();
	e5.setWeight(5);
	e5.setFrom(v2);
	e5.setTo(v3);
	getAllEdges().add(e5);
	Edge e6 = new Edge();
	e6.setWeight(56);
	e6.setFrom(v3);
	e6.setTo(v6);
	getAllEdges().add(e6);
	Edge e7 = new Edge();
	e7.setWeight(15);
	e7.setFrom(v2);
	e7.setTo(v4);
	getAllEdges().add(e7);
	Edge e8 = new Edge();
	e8.setWeight(18);
	e8.setFrom(v2);
	e8.setTo(v5);
	getAllEdges().add(e8);
	Edge e9 = new Edge();
	e9.setWeight(23);
	e9.setFrom(v3);
	e9.setTo(v5);
	getAllEdges().add(e9);
	Edge e10 = new Edge();
	e10.setWeight(50);
	e10.setFrom(v6);
	e10.setTo(v0);
	getAllEdges().add(e10);
	Edge e11 = new Edge();
	e11.setWeight(2);
	e11.setFrom(v4);
	e11.setTo(v2);
	getAllEdges().add(e11);
	Edge e12 = new Edge();
	e12.setWeight(99);
	e12.setFrom(v1);
	e12.setTo(v3);
	getAllEdges().add(e12);
    }

    public static List<Vertex> getAllVertexes() {
	return allVertexes;
    }

    public static List<Edge> getAllEdges() {
	return allEdges;
    }

    public static Map<String, String> getAllShortestPathes() {
	return allShortestPathes;
    }

    public static void changeGraph(String from, String to, String weight) {
	if (NumberUtils.isNumber(weight) && from != null && to != null
		&& !from.equals(to)) {
	    int newWeight = Integer.valueOf(weight);
	    if (newWeight > 0) {
		boolean processed = false;
		for (Edge deltaSteppingEdge : allEdges) {
		    if (from.equals(deltaSteppingEdge.getFrom())
			    && to.equals(deltaSteppingEdge.getTo())) {

			System.out
				.println("Ändere bestehende Verbindung: from:"
					+ from + " to:" + to + " weight:"
					+ newWeight);
			deltaSteppingEdge.setWeight(Integer.valueOf(weight));
			processed = true;
			break;
		    }
		}

		if (!processed) {
		    Vertex fromVertex = null;
		    Vertex toVertex = null;

		    for (Vertex deltaSteppingVertex : allVertexes) {
			if (deltaSteppingVertex.getName().equals(from)) {
			    fromVertex = deltaSteppingVertex;
			}
			if (deltaSteppingVertex.getName().equals(to)) {
			    toVertex = deltaSteppingVertex;
			}
		    }

		    if (fromVertex == null) {
			System.out.println("lege neuen Knoten mit Namen:"
				+ from + " an ");
			Vertex v = new Vertex();
			v.setName(from);
			getAllVertexes().add(v);
		    }
		    if (toVertex == null) {
			System.out.println("lege neuen Knoten mit Namen:" + to
				+ " an ");
			toVertex = new Vertex();
			toVertex.setName(to);
			getAllVertexes().add(toVertex);
		    }
		    Edge edge = new Edge();
		    edge.setWeight(newWeight);
		    edge.setFrom(fromVertex);
		    edge.setTo(toVertex);

		    System.out.println("neue Verbindung: from:" + from + " to:"
			    + to + " weight:" + newWeight);
		    getAllEdges().add(edge);

		}
	    } else if (newWeight < 0) {
		for (Edge deltaSteppingEdge : allEdges) {
		    if (from.equals(deltaSteppingEdge.getFrom().getName())
			    && to.equals(deltaSteppingEdge.getTo().getName())) {

			System.out
				.println("lösche bestehende Verbindung: from:"
					+ from + " to:" + to);
			allEdges.remove(deltaSteppingEdge);
			break;
		    }
		}
	    }
	}
    }

    public static void printShortestPaths() {
	System.out.println("kuerzester PFad nach:");
	for (String s : allShortestPathes.keySet()) {
	    System.out.println(s+ ": "+ allShortestPathes.get(s));
	}
    }

}
