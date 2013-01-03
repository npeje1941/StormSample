package storm.sample.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 受信した数を二乗して次のBoltに流すBolt
 * 
 * @author kimutansk
 */
public class SquareBolt extends BaseRichBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = 3693966369855071985L;

    /** OutputCollector */
    OutputCollector           collector;

    /**
     * デフォルトコンストラクタ
     */
    public SquareBolt()
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
        int inputValue = input.getIntegerByField("Value");
        int resultValue = inputValue * inputValue;
        this.collector.emit(input, new Values(resultValue));
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("Value"));
    }
}
