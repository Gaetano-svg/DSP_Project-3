package DSP.KafkaKubController;

import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Event {
	
	/*EVENT LIST{
		
		ADDED(0),
		MODIFIED(1),
		DELETED(2);
	    
	}*/

	@SerializedName("event")
	@Expose
	private String event;
	
	@SerializedName("eventCode")
	@Expose
	private int eventCode;
	
	@SerializedName("resourceName")
	@Expose
	private String resourceName;
	
	public String getEvent() {
		return event;
	}
	
	public void setEvent(String event) {
		this.event = event;
	}
	
	public int getEventCode() {
		return eventCode;
	}
	
	public void setEventCode(int eventCode) {
		this.eventCode = eventCode;
	}
	
	public String getResourceName() {
		return resourceName;
	}
	
	public void setResourceName(String resourceName) {
		this.resourceName = resourceName;
	}
	
	public String toString() {

		GsonBuilder gson = new GsonBuilder();
		gson.disableHtmlEscaping();
		
		return gson.create().toJson(this, Event.class);
		
	}

}
