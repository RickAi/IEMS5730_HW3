package twitter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Map;

public class HashTagBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //Get the tweet
        final Status status = (Status) tuple.getValueByField("tweet");
        //Loop through the hashtags
        for (HashtagEntity hashtag : status.getHashtagEntities()) {
            //Emit each hashtag
            collector.emit(new Values(1, hashtag.getText()));
        }
        collector.emit(new Values(0, null));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("type", "hashtag"));
    }
}
