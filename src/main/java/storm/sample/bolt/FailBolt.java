package storm.sample.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * 常時fail応答を返すBolt
 * 
 * @author kimutansk
 */
public class FailBolt extends BaseRichBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 3693966369855071985L;

    /** OutputCollector */
    OutputCollector           collector;

    /**
     * デフォルトコンストラクタ
     */
    public FailBolt()
    {}

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input)
    {
        this.collector.fail(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // 常時失敗を返すためemitは行わない。そのため不要。
    }
}
