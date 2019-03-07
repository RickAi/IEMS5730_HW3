package twitter;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class ReporterBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(ReporterBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");
        Integer actualWindowLengthInSeconds = tuple.getIntegerByField("actualWindowLengthInSeconds");
        LOG.info("Popular HashTag:" + word + ", with frequency:" + count + " in last " + actualWindowLengthInSeconds + "s");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
