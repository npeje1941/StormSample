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
 * 受信した２つの数を乗算して次のBoltに流すBolt
 * 
 * @author kimutansk
 */
public class MultiBolt extends BaseRichBolt
{
    /** serialVersionUID */
    private static final long serialVersionUID = -6902164083208850134L;

    /** OutputCollector */
    OutputCollector           collector;

    /**
     * デフォルトコンストラクタ
     */
    public MultiBolt()
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
        int inputValue1 = input.getIntegerByField("Value1");
        int inputValue2 = input.getIntegerByField("Value2");
        int resultValue = inputValue1 * inputValue2;
        this.collector.emit(input, new Values(resultValue));
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("Value"));
    }
}
