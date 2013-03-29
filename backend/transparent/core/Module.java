package transparent.core;

public class Module
{
	/* contains the full command to execute the module, or the remote URL */
	private final String path;
	
	/* specifies whether the module is remote */
	private final boolean remote;
	
	/* specifies whether website should be downloaded in blocks or all at once */
	private final boolean useBlockedDownload;
	
	public Module(String path, boolean isRemote, boolean blockedDownload)
	{
		this.path = path;
		this.remote = isRemote;
		this.useBlockedDownload = blockedDownload;
	}
	
	public String getPath() {
		return path;
	}
	
	public boolean isRemote() {
		return remote;
	}
	
	public boolean blockedDownload() {
		return useBlockedDownload;
	}
}
