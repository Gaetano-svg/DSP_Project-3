package DSP.KafkaKubController;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class AgentMaster extends Thread {

	private final Producer producerObject;
	private final Logger logger = App.logger;

    public AgentMaster(final String topic,
                    final Boolean isAsync,
                    final String transactionalId,
                    final boolean enableIdempotency,
                    final int numRecords,
                    final int transactionTimeoutMs) {
    	
    	// PRODUCER from the MAIN KAFKA QUEUE
        this.producerObject = new Producer(topic, transactionalId, enableIdempotency, transactionTimeoutMs);
    	
    }
    
    @Override
    public void run() {
    	
	    final CountDownLatch closeLatch = new CountDownLatch(1);
	    final KubernetesClient client = new DefaultKubernetesClient();
	    	    
	    // Manage all the Pod Events received from Kubernetes Cluster
	    client.pods().inNamespace("default").watch(new Watcher<Pod>() {
	    	
	        public void eventReceived(Action action, Pod resource) {

	            boolean goOn = true;
	            String topic = producerObject.getTopic();
				Event eventToSend = new Event();
				
				logger.debug("[AM]: received event from " + resource.getMetadata().getName());
				
	            switch (action) {

	            	// ADDED EVENT Received from Kubernetes Cluster
	            	case ADDED:
	            		
	            		// collect all the conteiners running inside the Pod
	            		List<Container> podContainer = resource.getSpec().getContainers();
	            		ArrayList <String> containerNames = new ArrayList<>();
	            		
	            		for(Container cont : podContainer) {
	            			
	            			containerNames.add(cont.getName());
	            			
	            		}
	            		
	            		// set the Event Object to send through the KAFKA QUEUE
	            		eventToSend.setEvent(action.toString());
	            		eventToSend.setEventCode(0);
	            		eventToSend.setResourceName(resource.getMetadata().getName());

	            		// send the event
	            		producerObject.getProducer().send(new ProducerRecord<Integer, String>(topic,
	                			0,
	                			eventToSend.toString()));
	                	
	            		break;

		            // DELETED EVENT Received from Kubernetes Cluster
	            	case DELETED:

	            		// set the Event Object to send through the KAFKA QUEUE
	            		eventToSend.setEvent(action.toString());
	            		eventToSend.setEventCode(2);
	            		eventToSend.setResourceName(resource.getMetadata().getName());

	            		// send the event
	            		producerObject.getProducer().send(new ProducerRecord<Integer, String>(topic,
	                			0,
	                			eventToSend.toString()));
	            			            		
	            		break;

		            // MODIFIED EVENT Received from Kubernetes Cluster	
	            	case MODIFIED:
	                	
	                	// 1. filter all the events related to the DELETE modifications
	                	if(resource.getMetadata().getDeletionTimestamp() != null)
	                		break;
	                	
	                	// 2. filter all the NOT READY CONTAINERS message received
	                	for(PodCondition pCond : resource.getStatus().getConditions()) {
	                		
	                		// check on the Ready Pod Condition
	                		if(pCond.getType().equals("Ready")) {

		                		if(pCond.getReason().equals("ContainersNotReady"))
	                				goOn = false;
	                			
	                		}

	                		// check on the Ready Pod Condition
	                		if(pCond.getType().equals("ContainersReady")){

	                			if(pCond.getReason().equals("ContainersNotReady"))
	                				goOn = false;
	                			
	                		}
	                		
	                	}

	                	// 3. filter the MODIFIED event because of the ADDED once
	                	if(resource.getStatus().getConditions().size() == 1) {
	                		
	                		PodCondition pCond = resource.getStatus().getConditions().get(0);
	                		if(pCond.getType().equals("PodScheduled")){

	                			if(pCond.getReason() == null)
	                				goOn = false;
	                			
	                		}
	                		
	                	}
	                	
	                	if(!goOn)
	                		break;

	            		// set the Event Object to send through the KAFKA QUEUE
	            		eventToSend.setEvent(action.toString());
	            		eventToSend.setEventCode(2);
	            		eventToSend.setResourceName(resource.getMetadata().getName());

	            		// send the event
	            		producerObject.getProducer().send(new ProducerRecord<Integer, String>(topic,
	                			0,
	                			eventToSend.toString()));
	                	
	                    break;
	                    
	                default:
	                	break;

	            }	            	

			    logger.info("[AM]: AgentMaster event handler created");
			    
	        }
	        
	        public void onClose(KubernetesClientException cause) {
	        	
	            closeLatch.countDown();
	            
	        }
	        
	    });
	    
	    /*client.services().inNamespace("default").watch(new Watcher<Service>() {
	    	
	        public void eventReceived(Action action, Service resource) {

	            String topic = producerObject.getTopic();
	            
	            switch (action) {

	            	case ADDED:
	            	case DELETED:
	            		
	            		producerObject.getProducer().send(new ProducerRecord<Integer, String>(topic,
	                			0,
	                			action.toString() + " | serviceName: " + resource.getMetadata().getName() + " | resource: " + resource.toString()));
	            				//, new DemoCallBack(startTime, 0, action.toString() + " " + resource.toString()));
	            		
	            		break;
	            		
	                default:
	                
	                	producerObject.getProducer().send(new ProducerRecord<Integer, String>(topic,
	                			0,
	                			action.toString() + " | serviceName: " + resource.getMetadata().getName() + " | resource: " + resource.toString()));
	                			//, new DemoCallBack(startTime, 0, action.toString() + " " + resource.toString()));
	                    break;
	                    
	            }
				
	        }

	        public void onClose(KubernetesClientException cause) {
	        	
	            closeLatch.countDown();
	            
	        }
	        
	    });*/
	   
	    // Cyclic sleep in order to listen to the received events
	    while(true) {
	    	
	    	try {

	    	    Thread.sleep(60000l);
	    		
	    	} catch (Exception e) {
	    		
	    	}
	    	
	    }
    	
    }
    
}