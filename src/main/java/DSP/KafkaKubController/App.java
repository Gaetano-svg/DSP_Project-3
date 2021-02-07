package DSP.KafkaKubController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;

public class App 
{
	
	public static final Logger logger = LoggerFactory.getLogger("MainLog.log");
	
	public static void main(String[] args) throws InterruptedException {
		
		boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        
        // run the Kafka Master
        AgentMaster producerThread = new AgentMaster(isAsync, null, false, 10000, -1);
        producerThread.start();
        logger.info("[MAIN]: started Agent Master Thread");
        
        // run the Kafka Slave
        AgentSlave consumerThread = new AgentSlave(Optional.empty(), false, 10000);
        consumerThread.start();
        logger.info("[MAIN]: started Agent Slave Thread");

        // run the Security Controller with the relative Kafka Topic
        SecurityController sec = new SecurityController(Optional.empty(), false, 10000);
        sec.start();
        logger.info("[MAIN]: started Security Controller Thread");
        
        // wait for all the threads
        producerThread.join();
        consumerThread.join();
        sec.join();
	 
	}
	  
}