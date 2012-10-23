package de.widas.examples.wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SystemInSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    static boolean firstTime = true;

    @Override
    public void open(Map conf, TopologyContext context,
	    SpoutOutputCollector collector) {
	_collector = collector;
    }

    @Override
    public void nextTuple() {

	InputStreamReader isr = new InputStreamReader(System.in);
	BufferedReader br = new BufferedReader(isr);

	String str;
	try {
	    if (firstTime) {
		System.out.println("Ihre Eingabe:");
		firstTime = false;
	    }
	    str = br.readLine();
	    _collector.emit(new Values(str));
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields("word"));
    }

}