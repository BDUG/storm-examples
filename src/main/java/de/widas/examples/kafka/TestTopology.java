package de.widas.examples.kafka;

import java.util.ArrayList;
import java.util.List;

import storm.kafka.KafkaConfig;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.OpaqueTransactionalKafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.google.common.collect.ImmutableList;

public class TestTopology {
    public static class PrinterBolt extends BaseBasicBolt {
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
	    System.out.println(tuple.toString());
	}

    }

    public static void main(String[] args) throws Exception {
	a(args);
    }

    public static void a(String[] args) throws Exception {
	TopologyBuilder builder = new TopologyBuilder();
	List<String> hosts = new ArrayList<String>();
	hosts.add("192.168.198.128:8092");
	SpoutConfig spoutConf = new SpoutConfig(StaticHosts.fromHostString(
		hosts, 1), "test", "/kafkastorm", "id");

	spoutConf.scheme = new StringScheme();
	// spoutConf.forceStartOffsetTime(0);
	// spoutConf.zkServers = ImmutableList.of("192.168.198.128");
	// spoutConf.zkPort = 2181;
	builder.setSpout("spout", new KafkaSpout(spoutConf), 3);

	builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("spout");

	Config conf = new Config();
	LocalCluster cluster = new LocalCluster();
	cluster.submitTopology("kafka-test", conf, builder.createTopology());

	Utils.sleep(600000);
    }

    public static void b(String[] args) throws Exception {
	List<String> hosts = new ArrayList<String>();
	hosts.add("192.168.198.128:8092");
	KafkaConfig kafkaConf = new KafkaConfig(StaticHosts.fromHostString(
		hosts, 2), "test");
	kafkaConf.scheme = new StringScheme();

	LocalCluster cluster = new LocalCluster();
	TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder(
		"id", "spout", new OpaqueTransactionalKafkaSpout(kafkaConf), 1);

	builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("spout");
	Config config = new Config();

	cluster.submitTopology("kafka-test", config, builder.buildTopology());

	Thread.sleep(600000);
    }
}
