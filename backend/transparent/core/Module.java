package transparent.core;

public class Module
{
	/* contains the full command to execute the module, or the remote URL */
	private final String path;

    /* Name of source for which this module is a parser */
    private final String sourceName;

    /* Unique name of module */
    private final String moduleName;

	/* specifies whether the module is remote */
	private final boolean remote;
	
	/* specifies whether website should be downloaded in blocks or all at once */
	private final boolean useBlockedDownload;

	public Module(String path, String sourceName, String moduleName, boolean isRemote,
                  boolean blockedDownload)
	{
		this.path = path;
		this.sourceName = sourceName;
        this.moduleName = moduleName;
        this.remote = isRemote;
		this.useBlockedDownload = blockedDownload;
	}
	
	public String getPath() {
		return path;
	}

    public String getSourceName() {
        return sourceName;
    }

    public String getModuleName() {
        return moduleName;
    }
	
	public boolean isRemote() {
		return remote;
	}
	
	public boolean blockedDownload() {
		return useBlockedDownload;
	}
}
