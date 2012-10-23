package de.widas.examples.deltastepping;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.widas.examples.deltastepping.model.Edge;
import de.widas.examples.deltastepping.utils.GraphUtils;

public class DeltaSteppingInitBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
	if (tuple.size() == 1) {
	    System.out.println("START");

	    // Liste löschen
	    GraphUtils.getAllShortestPathes().clear();

	    // Suche initiieren
	    for (Edge edge : GraphUtils.getAllEdges()) {
		if (edge.getFrom().getName()
			.equalsIgnoreCase((String) tuple.getValues().get(0))) {
		    collector.emit(new Values(edge.getFrom().getName(), edge
			    .getTo().getName(), Integer.toString(edge
			    .getWeight()), edge.getFrom().getName(), edge
			    .getFrom().getName() + edge.getTo().getName()));
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