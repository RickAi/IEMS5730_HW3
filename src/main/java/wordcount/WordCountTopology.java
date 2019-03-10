package wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.*;

public class WordCountTopology {

    public static class SplitSentenceBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String line = tuple.getStringByField("value");
            // split with space
            String[] words = line.split(" ");
            for (String word : words) {
                if (!word.isEmpty()) {
                    // your Bolts should perform proper tuple-anchoring and acknowledgement
                    this.collector.emit(tuple, new Values(word));
                }
            }
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    public static class WordCountBolt extends BaseRichBolt {

        private OutputCollector collector;
        private LinkedHashMap<String, Integer> counterMap;
        private static final int TARGET_FAIL_COUNT = 10;
        private int failCounter = 0;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
            this.counterMap = new LinkedHashMap<String, Integer>();
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");

            if (word.equals("the") && failCounter < TARGET_FAIL_COUNT) {
                this.collector.fail(tuple);
                failCounter += 1;
                return ;
            }

            if (counterMap.containsKey(word)) {
                counterMap.put(word, counterMap.get(word) + 1);
            } else {
                counterMap.put(word, 1);
            }
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }

        @Override
        public void cleanup() {
            System.out.println("cleanup, sortByValue counterMap start");
            // sort and save result into local file
            Utils.sortByValue(counterMap, new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o2.compareTo(o1);
                }
            });

            System.out.println("cleanup, start to save counterMap into file");
            FileWriter fw = null;
            BufferedWriter writer = null;
            try {
                fw = new FileWriter(RESULT_PATH);
                writer = new BufferedWriter(fw);
                for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
                    writer.write(entry.getKey() + "\t" + String.valueOf(entry.getValue()));
                    writer.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (writer != null) {
                        writer.close();
                    }
                    if (fw != null) {
                        fw.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("cleanup, end save counterMap into file");
        }
    }

    public static final String KAFKA_SPOUT = "kafka-spout";
    public static final String ID_SPLIT_BOLT = "split-bolt";
    public static final String ID_COUNT_BOLT = "count-bolt";

    public static final String RESULT_PATH = "/home/yongbiaoai/remote/wordcount_result.txt";

    public static final int KAFKA_SPOUT_COUNT = 1;
    public static final int SPLIT_BOLT_COUNT = 1;
    public static final int COUNT_BOLT_COUNT = 3;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        // Required to use KafkaSpout[6] to support the replay of failed tuples.
        KafkaSpoutConfig<String, String> config =
                KafkaSpoutConfig.builder("10.142.0.2:9092", "wordcount").build();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout<String, String>(config), KAFKA_SPOUT_COUNT);
        // You are required to extend BaseRichBolt class to implement your SplitSentenceBolt and WordCountBolt class.
        builder.setBolt(ID_SPLIT_BOLT, new SplitSentenceBolt(), SPLIT_BOLT_COUNT).shuffleGrouping(KAFKA_SPOUT);
        builder.setBolt(ID_COUNT_BOLT, new WordCountBolt(), COUNT_BOLT_COUNT).globalGrouping(ID_SPLIT_BOLT);

        Config conf = new Config();
        conf.setDebug(false);

        try {
            if (args != null && args.length > 0) {
                conf.setNumWorkers(2);
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(3);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("word-count", conf, builder.createTopology());

                // local debug cluster only process 20min
                // make sure the time is long enough until the debug user stop manually
                Thread.sleep(60 * 1000);

                cluster.shutdown();
            }
        } catch (Exception e) {
            System.out.println("submit failed with error:" + e.toString());
        }
    }

}
