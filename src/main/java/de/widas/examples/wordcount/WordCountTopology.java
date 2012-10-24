package de.widas.examples.wordcount;

import java.util.HashMap;
import java.util.Map;

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

/**
 * This topology demonstrates Storm's stream groupings
 * 
 */
public class WordCountTopology {

    public static void main(String[] args) throws Exception {

	TopologyBuilder builder = new TopologyBuilder();

	builder.setSpout("spout", new SystemInSpout(), 1);

	builder.setBolt("split", new SplitSentenceBolt(), 1).noneGrouping(
		"spout");
	builder.setBolt("count", new WordCountBolt(), 1).noneGrouping("split");

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
