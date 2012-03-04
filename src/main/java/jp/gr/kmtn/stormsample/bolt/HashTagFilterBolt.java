package jp.gr.kmtn.stormsample.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * HashTagを含まないツイートを除去するBolt<br/>
 * 併せて、HashTagを含むツイートの場合はHashTagごとに一つのタプルとする
 * 
 * @author kimutan_sk
 */
public class HashTagFilterBolt extends BaseRichBolt {

	/**
	 * GeneratedID
	 */
	private static final long serialVersionUID = 62445282790436146L;

	/**
	 * トポロジに結果を渡すためのCollector
	 */
	private OutputCollector collector_;

	public void execute(Tuple tuple) {
		Status status = (Status) tuple.getValue(0);

		HashtagEntity hashTags[] = status.getHashtagEntities();

		List<Object> result = new ArrayList<Object>();

		if (hashTags != null && hashTags.length >= 1) {

			for (HashtagEntity hashtag : hashTags) {
				collector_.emit(tuple, new Values(hashtag.getText()
						.toLowerCase(), status));
			}
		}

		collector_.ack(tuple);
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag", "tweet"));
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector_ = collector;

	}
}
