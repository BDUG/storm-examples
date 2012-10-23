package de.widas.examples.deltastepping;

import static org.apache.commons.lang.math.NumberUtils.isNumber;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import de.widas.examples.deltastepping.utils.GraphUtils;

public class ManipulateGraphBolt extends BaseBasicBolt {

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