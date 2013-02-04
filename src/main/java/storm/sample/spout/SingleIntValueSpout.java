package storm.sample.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 0~10000のランダムの自然数を生成してTupleとして流すSpout<br/>
 * 1000msに1Tuple生成する。
 * 
 * @author kimutansk
 */
public class SingleIntValueSpout extends BaseRichSpout
{
    /** serialVersionUID */
    private static final long serialVersionUID = -9151348822421674411L;

    /** OutputCollector */
    SpoutOutputCollector      collector;

    /**
     * デフォルトコンストラクタ
     */
    public SingleIntValueSpout()
    {}

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
    }

    @Override
    public void nextTuple()
    {
        Random rnd = new Random();
        int createdValue = rnd.nextInt(10000);
        this.collector.emit(new Values(createdValue), createdValue);
        Utils.sleep(1000);
    }

    /**
     * {@inheritDoc}
     */
    public void ack(Object msgId)
    {}

    /**
     * {@inheritDoc}
     */
    public void fail(Object msgId)
    {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("Value"));
    }
}
