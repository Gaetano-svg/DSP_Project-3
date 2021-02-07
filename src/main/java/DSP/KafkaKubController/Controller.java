package DSP.KafkaKubController;

import java.io.FileReader;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.leansoft.bigqueue.BigQueueImpl;

public abstract class Controller extends ShutdownableThread {

    // Consumer Object
	private Consumer consumerObject;

	// local PERSISTENT queue to save the event received
	private BigQueueImpl bigQueue;
	
	// configuration local settings
	private String kafkaTopic;
	private String kafkaGroupId;
	private String kafkaServerUrl;
	private String kafkaServerPort;
	private String queueDirectory;
	private String queueName;
	
	public Controller(final Optional<String> instanceId, final boolean readCommitted, final int numMessageToConsume) {
		
		super("KafkaControllerConsumer", false);
        
        // read the JSON conf file
        readControllerConfiguration();

    	// CONSUMER from the MAIN KAFKA QUEUE
        this.consumerObject = new Consumer(this.kafkaServerUrl, this.kafkaServerPort, this.kafkaTopic, this.kafkaGroupId, instanceId, readCommitted);
        
		// setup the local queue in order to save the received events locally
		setup();
		
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
            String kafkaGroupId = configuration.get("kafkaGroupId").getAsString();
            String kafkaServerUrl = configuration.get("kafkaServerUrl").getAsString();
            String kafkaServerPort = configuration.get("kafkaServerPort").getAsString();
            String queueDirectory = configuration.get("queueDirectory").getAsString();
            String queueName = configuration.get("queueName").getAsString();
            
            // allocate all the CONFIGURATION settings
            this.kafkaTopic = kafkaTopic;
            this.kafkaGroupId = kafkaGroupId;
            this.kafkaServerUrl = kafkaServerUrl;
            this.kafkaServerPort = kafkaServerPort;
            this.queueDirectory = queueDirectory;
            this.queueName = queueName;
	        
	    } catch (Exception e) {
	    	 
	    	 e.printStackTrace();
	    	 
	    }		
		
	}
	
	private void setup(){
		
	    try {

		    bigQueue = new BigQueueImpl(this.queueDirectory, this.queueName);
		    
	    } catch (Exception e) {
	    	
	    	e.printStackTrace();
	    	
	    }
	    
	}
	
	private void manageEvent () {

		// method used to manage the received event.
		// inside this method the executeOperation task is called.
		// each controller must fill the method with the operations it must perform
		
		boolean isError = false;
		String record = null;
		
    	try {
    		
    		// dequeue from bigQueue
    		record = new String(bigQueue.peek());
            System.out.println("received message : " + record);
            
            // once it receives the message it will call an abstract method that will be filled with the operation relative to the controller
            Gson gson = new Gson();
            Event event = gson.fromJson(record/*.value()*/, Event.class);
            
            executeOperation(event);
            
            // clean all the consumed entries
            bigQueue.dequeue();
            bigQueue.gc();
            
    	} catch (Exception e) {
    		
    		isError = true;
    		
    	}
    	
    	if(isError) {
    		
    		try {
    			
    			System.out.println("An error occured sending event -> " + record + ": go to sleep for 5 seconds");
    			Thread.sleep(5000);
    			
    		} catch (Exception e) {
    			
    			e.printStackTrace();
    			
    		}
    		
    	}
    	
	}

    public void execute() {
		
		while(true) {

			// at the beginning try to send all the events on queue
			// it could be possible that some events weren't sent to the verefoo because of some problems
			// by this way, all the events that weren't consumed, we send the previous events
	        
			System.out.println("QUEUE SIZE: " + bigQueue.size());
			
			while (!bigQueue.isEmpty()){
	        	
	        	manageEvent();
	            
	        }
	        
			// get all the events from the kafka topic
			consumerObject.getConsumer().subscribe(Collections.singletonList(this.consumerObject.getTopic()));
	        ConsumerRecords<Integer, String> records = consumerObject.getConsumer().poll(Duration.ofSeconds(1));
	        
	        // check if the records map is empty
	        if(records.isEmpty()) {
	        	
	        	try {
	        		
	        		System.out.println("no events received -> go to sleep for 30 seconds");
	        		Thread.sleep(30000);
	        		continue;
	        		
	        	} catch (Exception e) {
	        		
	        		e.printStackTrace();
	        		
	        	}
	        	
	        }
	        
	        // save all the events received locally
	        for (ConsumerRecord<Integer, String> record : records) {
	        	
	            try {
	            	
					bigQueue.enqueue(record.value().getBytes());
					
				} catch (Exception e) {
					
					e.printStackTrace();
					
				}
	            
	        }
	        
		}
		
	}
    
    /*
     * this method represent the one that each controller must fill with their operations
     */
    public abstract void executeOperation(Event event) throws Exception;
    
}
