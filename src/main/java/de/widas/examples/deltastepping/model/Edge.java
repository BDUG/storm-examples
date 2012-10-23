package de.widas.examples.deltastepping.model;

public class Edge implements Comparable<Edge> {
    private Vertex from = new Vertex();
    private Vertex to = new Vertex();
    private Integer weight = new Integer(0);

    public Vertex getFrom() {
	return from;
    }

    public void setFrom(Vertex from) {
	this.from = from;
    }

    public Vertex getTo() {
	return to;
    }

    public void setTo(Vertex to) {
	this.to = to;
    }

    public Integer getWeight() {
	return weight;
    }

    public void setWeight(Integer weight) {
	this.weight = weight;
    }

    @Override
    public int compareTo(Edge o) {
	if (null == getFrom() || o.getFrom() == null) {
	    return -1;
	}
	if (null == getTo() || o.getTo() == null) {
	    return 1;
	}
	if (!getTo().getName().equalsIgnoreCase(o.getTo().getName())) {
	    return 1;
	}
	if (!getFrom().getName().equalsIgnoreCase(o.getFrom().getName())) {
	    return -1;
	}
	return 0;
    }

}
