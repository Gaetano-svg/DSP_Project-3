package DSP.KafkaKubController;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.leansoft.bigqueue.BigQueueImpl;

public class SecurityController extends Controller {

	// verefoo graph address
	private String ip = "localhost";
	
	// verefoo graph port
	private String port = "8085";
	
	// verefoo graph id
	private String gid = "2";

	// verefoo node counter
	private long nid = 1;

	public SecurityController(String topic, String groupId, Optional<String> instanceId, boolean readCommitted,
			int numMessageToConsume) {
		
		super(topic, groupId, instanceId, readCommitted, numMessageToConsume);
		
		// read JSON graph config file
		readVerefooGraphConfiguration();
		
	}
	
	// this method is used to read all the Graph's infos
	@SuppressWarnings("deprecation")
	private void readVerefooGraphConfiguration () {
		
		JsonParser parser = new JsonParser();
		
	     try { 
	    	 
	        JsonElement jsontree = parser.parse(
	            new FileReader(
	                "./verefooGraphConfiguration.json"
	            )
	        );
	        JsonElement je = jsontree.getAsJsonObject();
	        JsonObject jo = je.getAsJsonObject();
	        
	        JsonObject graph = jo;

            String gid = graph.get("gid").getAsString();
            String ip = graph.get("ip").getAsString();
            String port = graph.get("port").getAsString();
            
            // allocate all the verefoo graph settings
            this.gid = gid;
            this.ip = ip;
            this.port = port;
	        
	     } catch (Exception e) {
	    	 
	    	 e.printStackTrace();
	    	 
	     }
		
	}
		
	private int getNode(String nid) throws Exception {
		
		CloseableHttpClient httpClient = HttpClients.createDefault();
		int returnCode = 400;

        try {
        	
        	String getUrl = "http://" + ip + ":" + port + "/verefoo/adp/graphs/" + gid + "/nodes/" + nid;//localhost:8085/verefoo/adp/graphs/1/nodes/2"; // replace {id} with userId
        	HttpGet request = new HttpGet(getUrl);
        	CloseableHttpResponse response = httpClient.execute(request);

            try {

                returnCode = response.getStatusLine().getStatusCode();
                System.out.println("GET return code " + returnCode);
                
            } finally {
            	
                response.close();
                
            }
            
        } finally {
        	
            httpClient.close();
            
        }
		
        return returnCode;
        
	}

	@Override
	public void executeOperation(Event event) throws Exception {
				
		switch(event.getEventCode()) {
		
			// ADDED
			case 0:

				// check if the node is already inside the verefoo Graph
				try {

					int returnCode = getNode(event.getResourceName());
					
					// if the return code is 200 the node is already inside the verefoo graph
					if(returnCode == 200)
						return;
					
				} catch (Exception e) {
					
					throw e;
					
				}
				
	        	String createUrl = "http://" + ip + ":" + port + "/verefoo/adp/graphs/" + gid + "/nodes?nid=" + event.getResourceName();//localhost:8085/verefoo/adp/graphs/1/nodes/2"; // replace {id} with userId
	        	
	        	// UUID random in order to assign an univoque identificator to the node
	        	long nid = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
	        	
	        	JsonObject jObj = new JsonObject();
	        	jObj.addProperty("nid", event.getResourceName());
	        	jObj.addProperty("id", nid);
	        	jObj.addProperty("functionalType", "ENDHOST");
	        		        	
	        	try (CloseableHttpClient client = HttpClientBuilder.create().build()) {

	        		HttpPost request = new HttpPost(createUrl);
	                request.setHeader("Content-type", "application/json");
	                request.setHeader("Accept", "application/json");

					StringEntity entity = new StringEntity(jObj.toString(), ContentType.APPLICATION_JSON);
					entity.setContentEncoding("application/json");
					entity.setContentType("application/json");
	                request.setEntity(entity);
	                
	                HttpResponse response = client.execute(request);

	                // check response if some errors (timeout/not reachable ...) occured
                    int status = response.getStatusLine().getStatusCode();
                    System.out.println("POST return code: " + status);
                    
	                BufferedReader bufReader = new BufferedReader(new InputStreamReader(
	                        response.getEntity().getContent()));

	                StringBuilder builder = new StringBuilder();

	                String line;

	                while ((line = bufReader.readLine()) != null) {

	                    builder.append(line);
	                    builder.append(System.lineSeparator());
	                    
	                }
	                
	            } catch (Exception e) {

	            	throw e;
	            	
	            }
	        	
				break;
			
			// MODIFIED
			case 1:
				break;
			
			// DELETED
			case 2:
				

				// check if the node is already inside the verefoo Graph
				try {

					int returnCode = getNode(event.getResourceName());
					
					// if the return code is different from 200 the node isn't inside the verefoo graph
					if(returnCode != 200)
						return;
					
				} catch (Exception e) {

	            	throw e;
					
				}
				
				String deleteUrl = "http://" + ip + ":" + port + "/verefoo/adp/graphs/" + gid + "/nodes/" + event.getResourceName();//localhost:8085/verefoo/adp/graphs/1/nodes/2"; // replace {id} with userId
	        	
	        	try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
	        		
	                HttpDelete httpDelete = new HttpDelete(deleteUrl);

	                // Create a custom response handler
	                ResponseHandler<String> responseHandler = response -> {

		                // check response if some errors (timeout/not reachable ...) occured
	                    int status = response.getStatusLine().getStatusCode();
	                    System.out.println("DELETE return code: " + status);
	                    
	                    if (status >= 200 && status < 300) {
	                        HttpEntity entity = response.getEntity();
	                        return entity != null ? EntityUtils.toString(entity) : null;
	                    } else {
	                        throw new ClientProtocolException("Unexpected response status: " + status);
	                    }
	                    
	                };

	                httpclient.execute(httpDelete, responseHandler);
	                	                
	            } catch (Exception e) {

	            	throw e;
	            	
	            }	        	
				
				break;
		
		}
		
	}

}