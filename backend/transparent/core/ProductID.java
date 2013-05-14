package transparent.core;

public class ProductID
{
    private final long rowId;
	private final String moduleProductId;

	public ProductID() {
        this.rowId = 0;
		this.moduleProductId = null;
	}
	
	public ProductID(long rowId, String moduleId) {
        this.rowId = rowId;
		this.moduleProductId = moduleId;
	}
	
	public String getModuleProductId() {
		return moduleProductId;
	}

    public long getRowId() {
        return rowId;
    }
}
