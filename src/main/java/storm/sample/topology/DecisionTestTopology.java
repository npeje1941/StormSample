package storm.sample.topology;

import java.util.Map;

import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Stormの分岐確認テスト
 * 
 * @author kimutansk
 */
public class DecisionTestTopology
{
    /**
     * デフォルトコンストラクタ
     */
    private DecisionTestTopology()
    {}

    /**
     * 受信した単語長に合わせてTupleの送付先を切り替えるBolt
     * 
     * @author kimutansk
     */
    public static class JudgeBolt extends BaseRichBolt
    {
        /** serialVersionUID */
        private static final long serialVersionUID = 1L;

        /** OutputCollector */
        OutputCollector           collector;

        /** コンポーネントインデックス */
        int                       index;

        /**
         * デフォルトコンストラクタ
         */
        public JudgeBolt()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector)
        {
            this.collector = collector;
            this.index = context.getThisTaskIndex();
        }

        @Override
        public void execute(Tuple tuple)
        {
            String word = tuple.getString(0);

            if (word.length() <= 5)
            {
                System.out.println("JudgeBolt_" + this.index + "ToShortWord:" + word);
                // 短い単語用のStreamに流す
                this.collector.emit("ShortWord", new Values(word));
            }
            else
            {
                System.out.println("JudgeBolt_" + this.index + "ToLongWord:" + word);
                // 長い単語用のStreamに流す
                this.collector.emit("LongWord", new Values(word));
            }

            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer)
        {
            // 分岐用の2個のStremを定義
            declarer.declareStream("ShortWord", new Fields("word"));
            declarer.declareStream("LongWord", new Fields("word"));
        }
    }

    /**
     * 短い単語を出力するBolt
     * 
     * @author kimutansk
     */
    public static class ShortWordBolt extends BaseRichBolt
    {
        /** serialVersionUID */
        private static final long serialVersionUID = 1L;

        /** OutputCollector */
        OutputCollector           collector;

        /** コンポーネントインデックス */
        int                       index;

        /**
         * デフォルトコンストラクタ
         */
        public ShortWordBolt()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector)
        {
            this.collector = collector;
            this.index = context.getThisTaskIndex();
        }

        @Override
        public void execute(Tuple tuple)
        {
            String word = tuple.getString(0);
            System.out.println("ShortWordBolt_" + this.index + ":" + word);
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer)
        {}
    }

    /**
     * 長い単語を出力するBolt
     * 
     * @author kimutansk
     */
    public static class LongWordBolt extends BaseRichBolt
    {
        /** serialVersionUID */
        private static final long serialVersionUID = 1L;

        /** OutputCollector */
        OutputCollector           collector;

        /** コンポーネントインデックス */
        int                       index;

        /**
         * デフォルトコンストラクタ
         */
        public LongWordBolt()
        {}

        @SuppressWarnings("rawtypes")
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector)
        {
            this.collector = collector;
            this.index = context.getThisTaskIndex();
        }

        @Override
        public void execute(Tuple tuple)
        {
            String word = tuple.getString(0);
            System.out.println("LongWordBolt_" + this.index + ":" + word);
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer)
        {}
    }

    /**
     * プログラムエントリポイント<br/>
     * <ul>
     * <li>起動引数:arg[0] 設定値を記述したyamlファイルパス</li>
     * <li>起動引数:arg[1] Stormの起動モード(true:LocalMode、false:DistributeMode)</li>
     * </ul>
     * @param args 起動引数
     * @throws Exception 初期化例外発生時
     */
    public static void main(String[] args) throws Exception
    {
        // プログラム引数の不足をチェック
        if (args.length < 2)
        {
            System.out.println("Usage: java acromusashi.stream.example.topology.DecisionTestTopology ConfigPath isExecuteLocal(true|false)");
            return;
        }

        // 起動引数として使用したパスからStorm設定オブジェクトを生成
        Config conf = StormConfigGenerator.loadStormConfig(args[0]);

        // プログラム引数から設定値を取得(ローカル環境or分散環境)
        boolean isLocal = Boolean.valueOf(args[1]);

        TopologyBuilder builder = new TopologyBuilder();

        // Get setting from StormConfig Object
        int wordSpoutPara = StormConfigUtil.getIntValue(conf, "WordSpout.Parallelism", 2);
        int judgeBoltPara = StormConfigUtil.getIntValue(conf, "JudgeBolt.Parallelism", 2);
        int shortWordBoltPara = StormConfigUtil.getIntValue(conf, "ShortWord.Parallelism", 2);
        int longWordBoltPara = StormConfigUtil.getIntValue(conf, "LongWord.Parallelism", 2);

        builder.setSpout("WordSpout", new TestWordSpout(), wordSpoutPara);
        builder.setBolt("JudgeBolt", new JudgeBolt(), judgeBoltPara).fieldsGrouping("WordSpout", new Fields("word"));

        // ShortWordのStreamを読み込むよう定義
        builder.setBolt("ShortWord", new ShortWordBolt(), shortWordBoltPara).fieldsGrouping("JudgeBolt", "ShortWord",
                new Fields("word"));

        // LongWordのStreamを読み込むよう定義
        builder.setBolt("LongWord", new LongWordBolt(), longWordBoltPara).fieldsGrouping("JudgeBolt", "LongWord",
                new Fields("word"));

        if (isLocal)
        {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("DecisionTest", conf, builder.createTopology());
            Utils.sleep(10000000);
            cluster.killTopology("DecisionTest");
            cluster.shutdown();
        }
        else
        {
            StormSubmitter.submitTopology("DecisionTest", conf, builder.createTopology());
        }
    }
}
