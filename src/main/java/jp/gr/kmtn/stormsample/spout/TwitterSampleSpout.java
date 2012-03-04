package jp.gr.kmtn.stormsample.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * TwitterのSampleストリームからツイートを取得するSpout<br/>
 * API制限は無いようだが、現状のツイートを無作為に抽出するため、あまり使えない。
 * 
 * @author kimutan_sk
 */
public class TwitterSampleSpout extends BaseRichSpout {

	/**
	 * GeneratedID
	 */
	private static final long serialVersionUID = -3261191598092027199L;

	/**
	 * TwitterユーザID
	 */
	String userid_;

	/**
	 * Twitterパスワード
	 */
	String password_;

	/**
	 * トポロジに結果を渡すためのCollector
	 */
	SpoutOutputCollector outputCollector_;

	/**
	 * Tweetを蓄積するためのキュー
	 */
	LinkedBlockingQueue<Status> statusQueue_ = null;

	/**
	 * Tweetを取得するストリームオブジェクト
	 */
	TwitterStream twitterStream_;

	/**
	 * コンストラクタ
	 * 
	 * @param username ユーザID
	 * @param pwd パスワード
	 */
	public TwitterSampleSpout(String username, String pwd) {
		userid_ = username;
		password_ = pwd;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		statusQueue_ = new LinkedBlockingQueue<Status>(10000);
		outputCollector_ = collector;
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				statusQueue_.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception e) {
			}

		};
		TwitterStreamFactory fact = new TwitterStreamFactory(
				new ConfigurationBuilder().setUser(userid_)
						.setPassword(password_).build());
		twitterStream_ = fact.getInstance();
		twitterStream_.addListener(listener);
		twitterStream_.sample();
	}

	@Override
	public void nextTuple() {
		Status ret = statusQueue_.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			outputCollector_.emit(new Values(ret));
		}
	}

	@Override
	public void close() {
		twitterStream_.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
