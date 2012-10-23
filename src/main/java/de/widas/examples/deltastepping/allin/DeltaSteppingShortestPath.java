package de.widas.examples.deltastepping.allin;

import static org.apache.commons.lang.math.NumberUtils.isNumber;

import org.apache.commons.lang.StringUtils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.widas.examples.deltastepping.model.Edge;
import de.widas.examples.deltastepping.utils.GraphUtils;
import de.widas.examples.deltastepping.SystemInSpout;

/**
 */
public class DeltaSteppingShortestPath {

    public static class DeltaSteppingInitBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
	    if (tuple.size() == 1) {
		System.out.println("START");

		// Liste löschen
		GraphUtils.getAllShortestPathes().clear();

		// Suche initiieren
		for (Edge edge : GraphUtils.getAllEdges()) {
		    if (edge.getFrom()
			    .getName()
			    .equalsIgnoreCase((String) tuple.getValues().get(0))) {
			collector.emit(new Values(edge.getFrom().getName(),
				edge.getTo().getName(), Integer.toString(edge
					.getWeight()),
				edge.getFrom().getName(), edge.getFrom()
					.getName() + edge.getTo().getName()));
		    }
		}
	    }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("from", "to", "distance", "path",
		    "pathfromto"));
	    declarer.declareStream("result", new Fields("to", "distance",
		    "path"));
	}
    }

    public static class DeltaSteppingBolt extends BaseBasicBolt {
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
			String newdistance = Integer.toString((new Integer(
				distance) + edge.getWeight()));
			collector.emit(new Values(edge.getFrom().getName(),
				edge.getTo().getName(), newdistance, path, from
					+ edge.getTo().getName()));
		    }
		}
	    }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("from", "to", "distance", "path",
		    "pathfromto"));
	    declarer.declareStream("result", new Fields("to", "distance",
		    "path"));
	}
    }

    public static class DeltaSteppingFilterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	Integer shortestdistance = Integer.MAX_VALUE;
	String shortestpath = "";

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
	    String to = tuple.getString(0);
	    String distance = tuple.getString(1);
	    String path = tuple.getString(2);

	    Integer distanceToCheck = Integer.valueOf(distance);
	    if (shortestdistance.intValue() > distanceToCheck.intValue()) {
		shortestdistance = distanceToCheck;
		shortestpath = path;
	    }
	    GraphUtils.getAllShortestPathes().put(to, path + "-->" + distance);
	    System.out.println(path + "-->" + distance);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("to", "distance", "path"));
	}
    }

    public static class ManipulateGraphBolt extends BaseBasicBolt {

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
	    String words = tuple.getString(0);
	    String[] wordsArray = StringUtils.split(words, " ");
	    if (wordsArray.length == 3) {
		String start = wordsArray[0];
		String end = wordsArray[1];
		String weight = wordsArray[2];

		if (isNumber(weight)) {
		    GraphUtils.changeGraph(start, end, weight);
		}
	    }
	}
    }

    public static class PrintPathBolt extends BaseBasicBolt {

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
	    GraphUtils.printShortestPaths();
	}
    }

    public static void main(String[] args) throws Exception {
	GraphUtils.initStandardGraph();
	TopologyBuilder builder = new TopologyBuilder();
	TopologyBuilder manipulateGraphTopology = new TopologyBuilder();
	builder.setSpout("spout", new SystemInSpout("cg", "print"), 1);

	builder.setBolt("init", new DeltaSteppingInitBolt(), 12)
		.shuffleGrouping("spout");
	// Rekursion sssp --zu--> sssp

	builder.setBolt("sssp", new DeltaSteppingBolt(), 12)
		.fieldsGrouping("init", new Fields("to"))
		.fieldsGrouping("sssp", new Fields("to"));

	builder.setBolt("shortestfilter", new DeltaSteppingFilterBolt(), 12)
		.fieldsGrouping("sssp", "result", new Fields("to"));

	builder.setBolt("changeGraphBolt", new ManipulateGraphBolt(), 1)
		.noneGrouping("spout", "cg");
	builder.setBolt("printGraphBolt", new PrintPathBolt(), 1).noneGrouping(
		"spout", "print");

	Config conf = new Config();
	conf.setDebug(false);

	if (args != null && args.length > 0) {
	    conf.setNumWorkers(3);

	    StormSubmitter.submitTopology(args[0], conf,
		    builder.createTopology());
	} else {
	    conf.setMaxTaskParallelism(3);

	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("shortestpath", conf,
		    builder.createTopology());
	}

    }
}
