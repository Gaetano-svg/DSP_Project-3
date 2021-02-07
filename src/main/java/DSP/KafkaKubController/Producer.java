package DSP.KafkaKubController;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/*
 * Represent the Kafka-Topic Controller class.
 * This is the Kafka-Topic end-point that produces each message linked to that Topic.
 * It is a wrapper class for the KafkaProducer class.
 */
public class Producer {

    private final KafkaProducer<Integer, String> producer;
	private String topic;
	
	Producer (	final String kafkaServerUrl, final String kafkaServerPort, final String topic,
                final String transactionalId,
                final boolean enableIdempotency,
                final int transactionTimeoutMs) {
		
		this.topic = topic;
        
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerUrl + ":" + kafkaServerPort);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        if (transactionTimeoutMs > 0) {
            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        }
        if (transactionalId != null) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotency);

        producer = new KafkaProducer<Integer, String>(props);
        
	}

	public KafkaProducer<Integer, String> getProducer () {
		
		return this.producer;
		
	}
	
	public String getTopic () {
		
		return this.topic;
		
	}
	
}
