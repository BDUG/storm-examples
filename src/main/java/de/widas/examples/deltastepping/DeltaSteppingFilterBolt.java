package de.widas.examples.deltastepping;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.widas.examples.deltastepping.utils.GraphUtils;

public class DeltaSteppingFilterBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    Integer shortestdistance = Integer.MAX_VALUE;
    String shortestpath = "";

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
	String to = tuple.getString(0);
	String distance = tuple.getString(1);
	String path = tuple.getString(2);

	if (GraphUtils.getAllShortestPathes().get(to) == null) {
	    shortestdistance = Integer.MAX_VALUE;
	    shortestpath = "";
	}
	Integer distanceToCheck = Integer.valueOf(distance);
	if (shortestdistance.intValue() > distanceToCheck.intValue()) {
	    shortestdistance = distanceToCheck;
	    shortestpath = path;
	}
	System.out.println(path + "-->" + distance + " | " + shortestpath
		+ "-->" + shortestdistance);
	GraphUtils.getAllShortestPathes().put(to,
		shortestpath + "-->" + shortestdistance);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields("to", "distance", "path"));
    }
}