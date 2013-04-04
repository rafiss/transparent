package transparent.core;

import java.io.PrintStream;

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

	/* the output log associated with this module */
	private final PrintStream log;

	public Module(String path, String sourceName,
			String moduleName, PrintStream log,
			boolean isRemote, boolean blockedDownload)
	{
		this.path = path;
		this.sourceName = sourceName;
        this.moduleName = moduleName;
        this.remote = isRemote;
		this.useBlockedDownload = blockedDownload;
		this.log = log;
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
	
	public void logInfo(String className,
			String methodName, String message)
	{
		log.println(className + '.' + methodName + ": " + message);
	}
	
	public void logError(String className,
			String methodName, String message)
	{
		log.println(className + '.' + methodName + " ERROR: " + message);
	}
}
