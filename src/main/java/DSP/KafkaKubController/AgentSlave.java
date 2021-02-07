package DSP.KafkaKubController;

import java.io.FileReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AgentSlave extends ShutdownableThread {

    // Consumer Object
	private final Consumer consumerObject;
	
	private String kafkaTopic;
	private String kafkaConsumerGroupId;
	private ArrayList<String> controllersList;

	// Controllers Producers Array
	private final ArrayList<Producer> producersArray;

	private String kafkaServerUrl;

	private String kafkaServerPort;
	
    public AgentSlave(	final Optional<String> instanceId,
	                    final boolean readCommitted,
	                    final int numMessageToConsume) {
    	
        super("KafkaConsumerExample", false);
        
        // setup the Slave configuration
        readControllerConfiguration();

    	// CONSUMER from the MAIN KAFKA QUEUE
        this.consumerObject = new Consumer(this.kafkaServerUrl, this.kafkaServerPort, this.kafkaTopic, this.kafkaConsumerGroupId, instanceId, readCommitted);
        
        // list of producer-controllers to manage -> for now only the Security Controller is considered
        this.producersArray = new ArrayList<>();
        
    	// PRODUCERS from the controllers list
        for (String controllerTopic : controllersList) {
        	
        	// create a kafka producer for each topic string received
        	Producer producer = new Producer(this.kafkaServerUrl, this.kafkaServerPort, controllerTopic, null, false, 10000);
        	producersArray.add(producer);
        	
        }
        
    }


	@SuppressWarnings("deprecation")
	private void readControllerConfiguration() {

		// read the controllerConfiguration JSON file
		JsonParser parser = new JsonParser();
		
		try { 
	    	 
			JsonElement jsontree = parser.parse(
	            new FileReader(
	                "./controllerConfiguration.json"
	            )
	        );
			
	        JsonElement je = jsontree.getAsJsonObject();
	        JsonObject jo = je.getAsJsonObject();
	        JsonObject configuration = jo;

            String kafkaTopic = configuration.get("kafkaTopic").getAsString();
            String kafkaConsumerGroupId = configuration.get("kafkaConsumerGroupId").getAsString();
            String kafkaServerUrl = configuration.get("kafkaServerUrl").getAsString();
            String kafkaServerPort = configuration.get("kafkaServerPort").getAsString();
                        
            // allocate all the CONFIGURATION settings
            this.kafkaTopic = kafkaTopic;
            this.kafkaConsumerGroupId = kafkaConsumerGroupId;
            this.kafkaServerUrl = kafkaServerUrl;
            this.kafkaServerPort = kafkaServerPort;
            
            // allocate controllers list
            this.controllersList = new ArrayList<>();
            JsonArray topicControllerList = configuration.get("topicControllerList").getAsJsonArray();
            
            for(JsonElement jet : topicControllerList) {

            	this.controllersList.add(jet.getAsString());
            	
            }
	        
	    } catch (Exception e) {
	    	 
	    	 e.printStackTrace();
	    	 
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