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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HashTagBolt extends BaseRichBolt {

    // skipWords refers from: udacity/ud381
    // https://github.com/udacity/ud381/blob/master/lesson3/stage5/src/jvm/udacity/storm/ParseTweetBolt.java
    private String[] skipWords = {"rt", "to", "me","la","on","that","que",
            "followers","watch","know","not","have","like","I'm","new","good","do",
            "more","es","te","followers","Followers","las","you","and","de","my","is",
            "en","una","in","for","this","go","en","all","no","don't","up","are",
            "http","http:","https","https:","http://","https://","with","just","your",
            "para","want","your","you're","really","video","it's","when","they","their","much",
            "would","what","them","todo","FOLLOW","retweet","RETWEET","even","right","like",
            "bien","Like","will","Will","pero","Pero","can't","were","Can't","Were","TWITTER",
            "make","take","This","from","about","como","esta","follows","followed"};
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //Get the tweet
        String tweet = tuple.getStringByField("value");
        //Loop through the hashtags
        for (String hashtag : getHashTags(tweet)) {
            //Emit each hashtag
            collector.emit(tuple, new Values(1, hashtag));
        }
        collector.emit(tuple, new Values(0, null));
    }

    private List<String> getHashTags(String tweet) {
        List<String> hashtags = new ArrayList<String>();

        // provide the delimiters for splitting the tweet
        String delims = "[ .,?!]+";

        // now split the tweet into tokens
        String[] tokens = tweet.split(delims);

        // for each token/word, emit it
        for (String token : tokens) {
            //emit only words greater than length 3 and not stopword list
            if (token.length() > 3 && !Arrays.asList(skipWords).contains(token)) {
                if (token.startsWith("#")) {
                    hashtags.add(token.substring(1));
                }
            }
        }

        return hashtags;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("type", "hashtag"));
    }
}
