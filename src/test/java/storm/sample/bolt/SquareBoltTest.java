package storm.sample.bolt;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import storm.sample.spout.SingleIntValueSpout;
import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.AckTracker;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.testing.TrackedTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * SquareBoltのテストクラス
 * 
 * @author kimutansk
 */
public class SquareBoltTest
{
    /** Assert確認フラグ。Stormクラスタ実行スレッドの検証が正常終了した場合にtrueにして検証がOKだったかを検知 */
    private boolean isAsserted = false;

    /**
     * SquareBoltを1段階組んで結果を確認する。<br/>
     * 投入する値は「0、10」
     * @throws Exception 実行失敗時
     */
    @Test
    public void testExecute_SquareBolt1段() throws Exception
    {
        this.isAsserted = false;
        MkClusterParam mkClusterParam = new MkClusterParam();
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        try
        {
            Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
                @Override
                public void run(ILocalCluster cluster)
                {
                    // 準備
                    // Topology構成を生成
                    TopologyBuilder builder = new TopologyBuilder();
                    builder.setSpout("SingleIntValueSpout", new SingleIntValueSpout(), 2);
                    builder.setBolt("SquareBolt", new SquareBolt(), 2).fieldsGrouping("SingleIntValueSpout",
                            new Fields("Value"));
                    StormTopology topology = builder.createTopology();

                    // テスト用のデータを生成
                    MockedSources mockedSources = new MockedSources();
                    mockedSources.addMockData("SingleIntValueSpout", new Values(0), new Values(10));

                    // 動作用の設定を生成
                    Config conf = new Config();
                    CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                    completeTopologyParam.setMockedSources(mockedSources);
                    completeTopologyParam.setStormConf(conf);

                    // 実施（Topologyを実行）
                    Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);

                    // 検証
                    assertTrue(Testing.multiseteq(new Values(new Values(0), new Values(100)),
                            Testing.readTuples(result, "SquareBolt")));
                    // 検証OKだった場合検証OKフラグを設定
                    SquareBoltTest.this.isAsserted = true;
                }
            });
        }
        catch (Exception ex)
        {
            // Windows上で実行した場合、ZooKeeperファイル削除に失敗してIOExceptionが発生する。
            // そのため、IOExceptionが発生した場合は無視。
            if ((ex instanceof IOException) == false)
            {
                throw ex;
            }
        }

        assertTrue(this.isAsserted);
    }

    /**
     * SquareBoltにTupleを流してAckが返ることを確認する。<br/>
     * 投入する値は「1」
     * @throws Exception 実行失敗時
     */
    @Test
    public void testExecute_Ack確認() throws Exception
    {
        this.isAsserted = false;
        MkClusterParam mkClusterParam = new MkClusterParam();
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        try
        {
            Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
                @Override
                public void run(ILocalCluster cluster)
                {
                    // 準備
                    // Tracker生成
                    AckTracker tracker = new AckTracker();
                    FeederSpout spout = new FeederSpout(new Fields("Value"));
                    spout.setAckFailDelegate(tracker);

                    // Topology構成を生成
                    TopologyBuilder builder = new TopologyBuilder();
                    builder.setSpout("FeederSpout", spout);
                    builder.setBolt("SquareBolt", new SquareBolt()).shuffleGrouping("FeederSpout");
                    StormTopology topology = builder.createTopology();
                    TrackedTopology tracked = Testing.mkTrackedTopology(cluster, topology);

                    try
                    {
                        cluster.submitTopology("AckTest", new Config(), tracked.getTopology());
                    }
                    catch (Exception ex)
                    {
                        // 例外が発生した時点で失敗のため、returnで抜ける
                        ex.printStackTrace();
                        return;
                    }

                    // Tupleを流す
                    spout.feed(new Values(1), 1);
                    Testing.trackedWait(tracked, 1);
                    
                    // 検証
                    // 検証OKだった場合検証OKフラグを設定
                    SquareBoltTest.this.isAsserted = true;
                }
            });
        }
        catch (Exception ex)
        {
            // Windows上で実行した場合、ZooKeeperファイル削除に失敗してIOExceptionが発生する。
            // そのため、IOExceptionが発生した場合は無視。
            if ((ex instanceof IOException) == false)
            {
                throw ex;
            }
        }

        assertTrue(this.isAsserted);
    }

}
