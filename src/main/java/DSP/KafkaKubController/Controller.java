package DSP.KafkaKubController;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import com.google.gson.Gson;
import com.leansoft.bigqueue.BigQueueImpl;

public abstract class Controller extends ShutdownableThread {

    // Consumer Object
	private Consumer consumerObject;

	// local PERSISTENT queue to save the event received
	private BigQueueImpl bigQueue;
	
	public Controller(final String topic, final String groupId, final Optional<String> instanceId, final boolean readCommitted, final int numMessageToConsume) {
		
		super("KafkaControllerConsumer", false);

    	// CONSUMER from the MAIN KAFKA QUEUE
        this.consumerObject = new Consumer(topic, groupId, instanceId, readCommitted);
        				
		// setup the local queue in order to save the received events locally
		setup();
		
	}
	

	private void setup(){
		
		// saves under the dir ./queue the local persistent-queue
	    String queueDir = "./queue";
	    String queueName = "persistent-queue";
	    
	    try {

		    bigQueue = new BigQueueImpl(queueDir, queueName);
		    
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
