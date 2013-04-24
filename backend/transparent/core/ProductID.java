package transparent.core;

public class ProductID
{
    private final int rowId;
	private final String moduleProductId;

	public ProductID() {
        this.rowId = 0;
		this.moduleProductId = null;
	}
	
	public ProductID(int rowId, String moduleId) {
        this.rowId = rowId;
		this.moduleProductId = moduleId;
	}
	
	public String getModuleProductId() {
		return moduleProductId;
	}

    public int getRowId() {
        return rowId;
    }
}
