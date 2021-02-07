package DSP.KafkaKubController;

import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/*
 * Represent the Kafka-Topic Consumer class.
 * This is the Kafka-Topic end-point that consumes each message linked to that Topic.
 * It is a wrapper class for the KafkaConsumer class.
 */
public class Consumer {

    private final KafkaConsumer<Integer, String> consumer;
	private String topic;
    
	Consumer (final String kafkaServerUrl, final String kafkaServerPort, final String topic,
              final String groupId,
              final Optional<String> instanceId,
              final boolean readCommitted) {

        this.topic = topic;
        
		Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerUrl + ":" + kafkaServerPort);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        instanceId.ifPresent(id -> props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        
        if (readCommitted) {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
		
	}
	
	public KafkaConsumer<Integer, String> getConsumer () {
		
		return this.consumer;
		
	}
	
	public String getTopic () {
		
		return this.topic;
		
	}
	
}
