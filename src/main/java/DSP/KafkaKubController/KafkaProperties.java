package DSP.KafkaKubController;

import java.util.ArrayList;

public class KafkaProperties {
	
    public static final String TOPIC = "TestTopic";
    public static final ArrayList<String> TopicControllerList = new ArrayList<>(); // topic for the security controller component
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;

    private KafkaProperties() {}
    
}