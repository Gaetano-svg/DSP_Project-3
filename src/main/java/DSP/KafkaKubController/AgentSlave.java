package DSP.KafkaKubController;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AgentSlave extends ShutdownableThread {

    // Consumer Object
	private final Consumer consumerObject;
	
	// Controllers Producers Array
	private final ArrayList<Producer> producersArray;

    public AgentSlave(	final String topic,
    					final ArrayList<String> topicControllersArray,
	                    final String groupId,
	                    final Optional<String> instanceId,
	                    final boolean readCommitted,
	                    final int numMessageToConsume) {
    	
        super("KafkaConsumerExample", false);

    	// CONSUMER from the MAIN KAFKA QUEUE
        this.consumerObject = new Consumer(topic, groupId, instanceId, readCommitted);
        
        // list of producer-controllers to manage -> for now only the Security Controller is considered
        this.producersArray = new ArrayList<>();
        
    	// PRODUCERS from the controllers list
        for (String controllerTopic : topicControllersArray) {
        	
        	// create a kafka producer for each topic string received
        	Producer producer = new Producer(controllerTopic, null, false, 10000);
        	producersArray.add(producer);
        	
        }
        
    }

	@Override
	public void execute() {
		
		while(true) {

			consumerObject.getConsumer().subscribe(Collections.singletonList(this.consumerObject.getTopic()));
	        ConsumerRecords<Integer, String> records = consumerObject.getConsumer().poll(Duration.ofSeconds(1));
	        
	        for (ConsumerRecord<Integer, String> record : records) {
	        	
	        	// once the message is received from the slave
	        	// it will redirect the message to all the CONTROLLERS
	        	for(Producer p : producersArray) {
	        		
	        		String topic = p.getTopic();
	        		
	        		// send the message to the TOPIC
            		p.getProducer().send(new ProducerRecord<Integer, String>(topic,
                			0,
                			record.value()));
            		
	        	}
	        	
	        }
	        
		}
		
	}
	
}