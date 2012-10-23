package de.widas.examples.deltastepping;

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

    private static boolean firstTime = true;

    SpoutOutputCollector collector;

    private final String[] filters;

    public SystemInSpout() {
	this((String[]) null);
    }

    public SystemInSpout(String... filters) {
	this.filters = filters;
    }

    @Override
    public void open(Map conf, TopologyContext context,
	    SpoutOutputCollector collector) {
	this.collector = collector;
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
	    boolean filterMatched = false;
	    if (filters != null) {
		for (String filter : filters) {
		    if (str.startsWith(filter)) {
			// wenn filter matched emitten wir in den gleichnamigen
			// stream!
			collector.emit(filter,
				new Values(str.substring(filter.length())
					.trim()));
			filterMatched = true;
			break;
		    }
		}
	    }
	    if (!filterMatched) {
		// wenn kein filter vorhanden ist, oder der filter nicht matched
		// emitten wir in den default stream
		collector.emit(new Values(str));
	    }
	} catch (IOException e) {
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
	if (filters != null && filters.length > 0) {
	    for (String filter : filters) {
		declarer.declareStream(filter, new Fields("word"));
	    }
	}
    }
}