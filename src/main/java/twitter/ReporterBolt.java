package twitter;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ReporterBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(ReporterBolt.class);
    private static final String REPORT_PATH = "/home/yongbiaoai/remote/hashtag_report.txt";
    private DateFormat dateFormat;
    private BufferedWriter writer = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        try {
            FileWriter fw = new FileWriter(REPORT_PATH);
            writer = new BufferedWriter(fw);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("ReporterBolt prepare failed:" + e.toString());
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");
        Integer actualWindowLengthInSeconds = tuple.getIntegerByField("actualWindowLengthInSeconds");
        LOG.info("Popular HashTag:" + word + ", with frequency:" + count + " in last " + actualWindowLengthInSeconds + "s");

        try {
            if (writer != null) {
                writer.write("Popular HashTag:" + word + "\tFrequency:" + count + " (" + getDate() + ")");
                writer.newLine();
                writer.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getDate() {
        return "at " + dateFormat.format(new Date());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
