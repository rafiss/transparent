package transparent.core;

public class ProductID
{
	private long databaseId;
	private String moduleId;
	
	public ProductID() {
		this.databaseId = -1;
		this.moduleId = null;
	}
	
	public ProductID(String moduleId) {
		this.databaseId = -1;
		this.moduleId = moduleId;
	}
	
	public long getDatabaseId() {
		return databaseId;
	}
	
	public String getModuleId() {
		return moduleId;
	}
	
	public void setDatabaseId(long id) {
		databaseId = id;
	}
	
	public void setModuleId(String id) {
		moduleId = id;
	}
}
