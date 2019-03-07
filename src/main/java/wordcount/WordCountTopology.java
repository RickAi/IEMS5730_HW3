package wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.*;

public class WordCountTopology {

    public static class FileReaderSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private boolean processed = false;
        private BufferedReader br = null;

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
            try {
                this.br = new BufferedReader(new FileReader(DATA_PATH));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void nextTuple() {
            // DO NOT emit the entire file in one nextTuple to avoid failure caused by network
            // congestion. You can emit one line for each ​nextTuple( )​ call.
            if (this.br == null || processed) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return ;
            }

            try {
                String buffer;
                if ((buffer = this.br.readLine()) != null) {
                    this.collector.emit(new Values(buffer));
                    //counter(CounterType.EMIT);
                } else {
                    processed = true;
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("line"));
        }
    }

    public static class SplitSentenceBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String line = tuple.getStringByField("line");
            // split with space
            String[] words = line.split(" ");
            for (String word : words) {
                if (!word.isEmpty()) {
                    this.collector.emit(new Values(word));
                    //counter(CounterType.EMIT);
                }
            }
            this.collector.ack(tuple);
            //counter(CounterType.ACK);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    public static class WordCountBolt extends BaseRichBolt {

        private OutputCollector collector;
        private LinkedHashMap<String, Integer> counterMap;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
            this.counterMap = new LinkedHashMap<String, Integer>();
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");
            if (counterMap.containsKey(word)) {
                counterMap.put(word, counterMap.get(word) + 1);
            } else {
                counterMap.put(word, 1);
            }
            this.collector.ack(tuple);
            //counter(CounterType.ACK);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

        @Override
        public void cleanup() {
            // dump counters into the console
            //dumpCounters();

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

    // Counter Code START
    // !!! LOCAL USED ONLY
    public static int emit_counter = 0;
    public static int ack_counter = 0;

    public static enum CounterType {EMIT, ACK};

    public static synchronized void counter(CounterType type) {
        if (type == CounterType.EMIT) {
            emit_counter += 1;
        } else if (type == CounterType.ACK) {
            ack_counter += 1;
        }
    }

    public static void dumpCounters() {
        System.out.println("--------DUMP COUNTERS START--------");
        System.out.println("The number of tuple emitted:" + emit_counter);
        System.out.println("The number of tuple acked:" + ack_counter);
        System.out.println("The number of tuple failed:" + (emit_counter - ack_counter));
        System.out.println("--------DUMP COUNTERS END--------");
    }
    // Counter Code END

    public static final String ID_FILE_READ_SPOUT = "file-reader-spout";
    public static final String ID_SPLIT_BOLT = "split-bolt";
    public static final String ID_COUNT_BOLT = "count-bolt";

    // these two path need to be replaced to local path if debug in local
    public static String DATA_PATH = "/home/yongbiaoai/projects/IEMS5730_HW2/StormData.txt";
    public static final String RESULT_PATH = "/home/yongbiaoai/projects/IEMS5730_HW2/wordcount_result.txt";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(ID_FILE_READ_SPOUT, new FileReaderSpout(), 1);
        builder.setBolt(ID_SPLIT_BOLT, new SplitSentenceBolt(), 8).shuffleGrouping(ID_FILE_READ_SPOUT);
        builder.setBolt(ID_COUNT_BOLT, new WordCountBolt(), 1).globalGrouping(ID_SPLIT_BOLT);

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
