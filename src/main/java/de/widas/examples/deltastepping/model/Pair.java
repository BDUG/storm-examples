package de.widas.examples.deltastepping.model;

public class Pair {
    Vertex lVertex = null;
    Integer tent = new Integer(0);
    String lPath = new String("");
    String lHops = new String("");

    public Vertex getlVertex() {
	return lVertex;
    }

    public void setlVertex(Vertex lVertex) {
	this.lVertex = lVertex;
    }

    public Integer getTent() {
	return tent;
    }

    public void setTent(Integer tent) {
	this.tent = tent;
    }

    public String getlPath() {
	return lPath;
    }

    public void setlPath(String lPath) {
	this.lPath = lPath;
    }

    @Override
    public int hashCode() {
	return lVertex.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
	if ((obj instanceof Pair)) {
	    return ((Pair) obj).hashCode() == hashCode();
	}
	if ((obj instanceof Vertex)) {
	    return ((Vertex) obj).getName().hashCode() == hashCode();
	}
	return false;
    }

    public String getlHops() {
	return lHops;
    }

    public void setlHops(String lHops) {
	this.lHops = lHops;
    }

}
