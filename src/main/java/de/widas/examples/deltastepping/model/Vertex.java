package de.widas.examples.deltastepping.model;

public class Vertex implements Comparable<Vertex> {
    String name = new String("");

    public String getName() {
	return name;
    }

    public void setName(String name) {
	this.name = name;
    }

    @Override
    public int hashCode() {
	return name.hashCode();
    }

    @Override
    public int compareTo(Vertex o) {
	if (equals(o))
	    return 0;
	return -1;
    }

    @Override
    public boolean equals(Object obj) {
	if (obj instanceof Pair) {
	    return ((Pair) obj).hashCode() == hashCode();
	} else if ((obj instanceof Pair)) {
	    return ((Pair) obj).hashCode() == hashCode();
	}
	return false;
    }

}
