package DSP.KafkaKubController;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class NodeGraph {

	@SerializedName("functionalType")
	@Expose
	private String functionalType;
	@SerializedName("id")
	@Expose
	private Integer id;
	@SerializedName("name")
	@Expose
	private String name;
	
	public String getFunctionalType() {
		return functionalType;
	}
	
	public void setFunctionalType(String functionalType) {
		this.functionalType = functionalType;
	}
	
	public Integer getId() {
		return id;
	}
	
	public void setId(Integer id) {
		this.id = id;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

}