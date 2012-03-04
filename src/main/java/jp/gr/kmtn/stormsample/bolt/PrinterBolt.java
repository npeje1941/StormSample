package jp.gr.kmtn.stormsample.bolt;

import twitter4j.Status;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * HashTagが指定条件を満たした場合にログを出力するBolt<br/>
 * 
 * @author kimutan_sk
 */
public class PrinterBolt extends BaseBasicBolt {

	/**
	 * GeneratedID
	 */
	private static final long serialVersionUID = -6056224037808017538L;
	
	/**
	 * 指定条件文字列
	 */
	private String target_;

	/**
	 * コンストラクタ
	 * 
	 * @param target 指定条件文字列
	 */
	public PrinterBolt(String target) {
		this.target_ = target;
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		String hashtag = (String) tuple.getValue(0);
		Status status = (Status) tuple.getValue(1);

		if (hashtag.contains(this.target_)) {
			System.out.println("CollectTag! User:"
					+ status.getUser().getScreenName() + " Tag:" + hashtag
					+ " " + status.getText());
		} else {
			System.out.println("InvalidTag! User:"
					+ status.getUser().getScreenName() + " Tag:" + hashtag
					+ " " + status.getText());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}

}
