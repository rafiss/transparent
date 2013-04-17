package transparent.core;

public class ProductID
{
	private final String moduleProductId;
	
	public ProductID() {
		this.moduleProductId = null;
	}
	
	public ProductID(String moduleId) {
		this.moduleProductId = moduleId;
	}
	
	public String getModuleProductId() {
		return moduleProductId;
	}
}
