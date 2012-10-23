package de.widas.examples.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class AllGroupingWordCountTopology {

    public static void main(String[] args) throws Exception {

	TopologyBuilder builder = new TopologyBuilder();

	builder.setSpout("spout", new SystemInSpout(), 1);

	builder.setBolt("split", new SplitSentenceBolt(), 1).allGrouping(
		"spout");
	builder.setBolt("count", new WordCountBolt(), 2).allGrouping("split");

	builder.setBolt("mergeCounts", new WordCountBolt(), 1).allGrouping(
		"count");

	Config conf = new Config();
	conf.setDebug(true);

	if (args != null && args.length > 0) {
	    conf.setNumWorkers(3);
	    StormSubmitter.submitTopology(args[0], conf,
		    builder.createTopology());
	} else {
	    conf.setMaxTaskParallelism(3);
	    final LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("word-count", conf, builder.createTopology());
	}
    }
}
