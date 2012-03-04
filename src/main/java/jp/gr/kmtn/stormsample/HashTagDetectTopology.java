package jp.gr.kmtn.stormsample;

import jp.gr.kmtn.stormsample.bolt.HashTagFilterBolt;
import jp.gr.kmtn.stormsample.bolt.PrinterBolt;
import jp.gr.kmtn.stormsample.spout.TwitterSingleSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * ハッシュタグを用いてTweetを抽出するトポロジ起動クラス
 * 
 * @author kimutan_sk
 */
public class HashTagDetectTopology {
	
	/**
	 * トポロジを起動する。<br/>
	 * <br/>
	 * 第１引数　：　SpoutからTweet取得時のフィルタ<br/>
	 * 第２引数　：　Boltで判定する際のハッシュタグ<br/>
	 * 第３引数　：　TwitterUserid<br/>
	 * 第４引数　：　TwitterPassword<br/>
	 * 第５引数　：　トポロジ名（未入力の場合、ローカルクラスタで起動）<br/>
	 * <br/>
	 * @param args 起動引数
	 * @throws Exception 起動失敗時
	 */
	public static void main(String[] args) throws Exception {
		String spoutFilterkeyword = args[0];
		String boltFilterKeyword = args[1];
		String userid = args[2];
		String password = args[3];

		String topologyName = null;
		if (5 <= args.length) {
			topologyName = args[4];
		}

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("twitter", new TwitterSingleSpout(userid, password,
				new String[] { spoutFilterkeyword }));
		builder.setBolt("hashtagfilter", new HashTagFilterBolt(), 5)
				.shuffleGrouping("twitter");

		builder.setBolt("print", new PrinterBolt(boltFilterKeyword), 5)
				.fieldsGrouping("hashtagfilter", new Fields("hashtag"));

		StormTopology topology = builder.createTopology();

		if (topologyName != null) {
			Config conf = new Config();
			conf.setDebug(false);
			conf.setNumWorkers(1);

			conf.put(Config.NIMBUS_HOST, "192.168.2.101");
			conf.put(Config.NIMBUS_THRIFT_PORT, 6627);

			StormSubmitter.submitTopology(topologyName, conf, topology);

		} else {
			Config conf = new Config();

			LocalCluster cluster = new LocalCluster();

			cluster.submitTopology("test", conf, topology);

			Utils.sleep(100000);
			cluster.shutdown();
		}

	}
}
