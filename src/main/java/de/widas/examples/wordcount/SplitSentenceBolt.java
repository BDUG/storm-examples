package de.widas.examples.wordcount;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields("word"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
	String words = tuple.getString(0);
	String[] wordsArray = StringUtils.split(words, " ");
	for (int i = 0; wordsArray != null && i < wordsArray.length; i++) {
	    collector.emit(new Values(wordsArray[i]));
	}
    }
}