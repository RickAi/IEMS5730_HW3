package twitter;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


// This code is refers from udacity/ud381:
// https://github.com/udacity/ud381/blob/master/lesson3/stage5/src/jvm/udacity/storm/TweetSpout.java
//
// Add filter for trump keyword:
// final FilterQuery query = new FilterQuery();
// query.track("Trump", "trump", "TRUMP");

public class TweetSpout extends BaseRichSpout {
    // Twitter API authentication credentials
    String custkey, custsecret;
    String accesstoken, accesssecret;

    // To output tuples from spout to the next stage bolt
    SpoutOutputCollector collector;

    // Twitter4j - twitter stream to get tweets
    TwitterStream twitterStream;

    // Shared queue for getting buffering tweets received
    LinkedBlockingQueue<Status> queue = null;

    // Class for listening on the tweet stream - for twitter4j
    private class TweetListener implements StatusListener {

        // Implement the callback function when a tweet arrives
        @Override
        public void onStatus(Status status) {
            queue.offer(status);
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice sdn) {
        }

        @Override
        public void onTrackLimitationNotice(int i) {
        }

        @Override
        public void onScrubGeo(long l, long l1) {
        }

        @Override
        public void onStallWarning(StallWarning warning) {
        }

        @Override
        public void onException(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Constructor for tweet spout that accepts the credentials
     */
    public TweetSpout(String key, String secret, String token, String tokensecret) {
        custkey = key;
        custsecret = secret;
        accesstoken = token;
        accesssecret = tokensecret;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // create the buffer to block tweets
        queue = new LinkedBlockingQueue<Status>();

        // save the output collector for emitting tuples
        collector = spoutOutputCollector;


        // build the config with credentials for twitter 4j
        ConfigurationBuilder config =
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(custkey)
                        .setOAuthConsumerSecret(custsecret)
                        .setOAuthAccessToken(accesstoken)
                        .setOAuthAccessTokenSecret(accesssecret);

        // create the twitter stream factory with the config
        TwitterStreamFactory fact =
                new TwitterStreamFactory(config.build());

        // get an instance of twitter stream
        twitterStream = fact.getInstance();

        // provide the handler for twitter stream
        twitterStream.addListener(new TweetListener());

        //Create a filter for the topics we want
        //to find trends for
        final FilterQuery query = new FilterQuery();
        //topics
        query.track("Trump", "trump", "TRUMP");
        //Apply the filter
        twitterStream.filter(query);
    }

    @Override
    public void nextTuple() {
        // try to pick a tweet from the buffer
        Status tweet = queue.poll();

        if (tweet != null) {
            // emit the tweet to next stage bolt
            collector.emit(new Values(tweet));
        } else {
            // if no tweet is available, wait for 50 ms and return
            Utils.sleep(50);
        }
    }

    @Override
    public void close() {
        // shutdown the stream - when we are going to exit
        twitterStream.shutdown();
    }

    /**
     * Component specific configuration
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        // create the component config
        Config ret = new Config();

        // set the parallelism for this spout to be 1
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void declareOutputFields(
            OutputFieldsDeclarer outputFieldsDeclarer) {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet'
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
}

