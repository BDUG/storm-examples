package de.widas.examples.deltastepping;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.widas.examples.deltastepping.model.Edge;
import de.widas.examples.deltastepping.utils.GraphUtils;

public class DeltaSteppingBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
	String from = tuple.getString(0);
	String to = tuple.getString(1);
	String distance = tuple.getString(2);
	String path = tuple.getString(3);

	if (!(path.startsWith(to) || path.endsWith(to) || path.contains(","
		+ to + ","))) {
	    path += "," + to;
	    collector.emit("result", new Values(to, distance, path));
	    for (Edge edge : GraphUtils.getAllEdges()) {
		if (edge.getFrom().getName().equalsIgnoreCase(to)) {
		    String newdistance = Integer
			    .toString((new Integer(distance) + edge.getWeight()));
		    collector.emit(new Values(edge.getFrom().getName(), edge
			    .getTo().getName(), newdistance, path, from
			    + edge.getTo().getName()));
		}
	    }
	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields("from", "to", "distance", "path",
		"pathfromto"));
	declarer.declareStream("result", new Fields("to", "distance", "path"));
    }
}