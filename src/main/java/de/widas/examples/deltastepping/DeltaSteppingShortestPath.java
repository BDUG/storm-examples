package de.widas.examples.deltastepping;

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

    public static void main(String[] args) throws Exception {
	GraphUtils.initStandardGraph();
	TopologyBuilder builder = new TopologyBuilder();
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
	    conf.setMaxTaskParallelism(12);

	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("shortestpath", conf,
		    builder.createTopology());
	}

    }
}
