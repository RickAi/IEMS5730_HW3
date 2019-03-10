package twitter;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Utils {

    public static Properties newProps(final String brokerUrl, final String topicName) {
        return new Properties() {
            {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.CLIENT_ID_CONFIG, topicName);
            }
        };
    }

}
