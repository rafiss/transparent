package transparent.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;

import transparent.core.database.Database;

public class Module
{
	/* contains the full command to execute the module, or the remote URL */
	private final String path;

    /* name of source for which this module is a parser */
    private final String sourceName;

    /* unique name of module */
    private final String moduleName;

	/* specifies whether the module is remote */
	private final boolean remote;
	
	/* specifies whether websites should be downloaded in blocks or all at once */
	private final boolean useBlockedDownload;
	
	/* unique integer identifier for the module */
	private final long id;

	/* the output log associated with this module */
	private final PrintStream log;
	
	/* indicates whether activity should be logged to standard out */
	private boolean logActivity;

	public Module(long id, String path,
			String moduleName, String sourceName,
			PrintStream log, boolean isRemote,
			boolean blockedDownload)
	{
		this.path = path;
		this.sourceName = sourceName;
        this.moduleName = moduleName;
        this.remote = isRemote;
		this.useBlockedDownload = blockedDownload;
		this.id = id;
		this.log = log;
	}
	
	/**
	 * WARNING: do not convert this directly to a string, as Java will
	 * interpret it as a signed value. Use {@link Module#getIdString()}
	 * instead.
	 */
	public long getId() {
		return id;
	}
	
    public String getIdString() {
    	return Core.toUnsignedString(id);
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
	
	public boolean isLoggingActivity() {
		return logActivity;
	}
	
	public void setLoggingActivity(boolean logActivity) {
		this.logActivity = logActivity;
	}
	
	public PrintStream getLogStream() {
		return log;
	}
	
	public void logInfo(String className,
			String methodName, String message)
	{
		log.println(className + '.' + methodName + ": " + message);
		if (logActivity) {
			Core.println("Module " + getIdString() + " (name: '"
					+ moduleName + "') information:" + Core.NEWLINE
					+ className + '.' + methodName + ": " + message);
			Core.flush();
		}
	}
	
	public void logError(String className,
			String methodName, String message)
	{
		log.println(className + '.' + methodName + " ERROR: " + message);
		if (logActivity) {
			Core.lockConsole();
			Core.println("Module " + getIdString() + " (name: '"
					+ moduleName + "') reported error:");
			Core.printError(className, methodName, message);
			Core.unlockConsole();
		}
	}
	
	public void logError(String className, String methodName,
			String message, String exception)
	{
		log.println(className + '.' + methodName + " ERROR: "
				+ message + " Exception thrown: " + exception);
		if (logActivity) {
			Core.lockConsole();
			Core.println("Module " + getIdString() + " (name: '"
					+ moduleName + "') reported error:");
			Core.printError(className, methodName, message, exception);
			Core.unlockConsole();
		}
	}
	
	public void logUserAgentChange(String newUserAgent)
	{
		if (logActivity) {
			Core.lockConsole();
			Core.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') changed user agent to: " + newUserAgent);
			Core.flush();
			Core.unlockConsole();
		}
	}
	
	public void logHttpGetRequest(String url)
	{
		if (logActivity) {
			Core.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') requested HTTP GET: " + url);
			Core.flush();
		}
	}
	
	public void logHttpPostRequest(String url, byte[] post)
	{
		if (logActivity) {
			Core.lockConsole();
			Core.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') requested HTTP POST:");
			Core.println("\tURL: " + url);
			Core.print("\tPOST data: ");
			try {
				Core.write(post);
			} catch (IOException e) {
				Core.print("<unable to write data>");
			}
			Core.println();
			Core.unlockConsole();
			Core.flush();
		}
	}
	
	public void logDownloadProgress(int downloaded)
	{
		if (logActivity) {
			Core.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') downloading: " + downloaded + " bytes");
			Core.flush();
		}
	}
	
	public void logDownloadCompleted(int downloaded)
	{
		if (logActivity) {
			Core.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') completed download: " + downloaded + " bytes");
			Core.flush();
		}
	}
	
	public void logDownloadAborted()
	{
		if (logActivity) {
			Core.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') aborted download due to download size limit.");
			Core.flush();
		}
	}

	public static Module load(Database database, int index)
	{
		long id = -1;
		String name = "<unknown>";
		try {
			id = new BigInteger(database.getMetadata("module." + index + ".id")).longValue();

			String path = database.getMetadata("module." + index + ".path");
			name = database.getMetadata("module." + index + ".name");
			String source = database.getMetadata("module." + index + ".source");
			String blockedString = database.getMetadata("module." + index + ".blocked");
			String remoteString = database.getMetadata("module." + index + ".remote");
			
			boolean blocked = true;
			boolean remote = true;
			if (blockedString.equals("0"))
				blocked = false;
			if (remoteString.equals("0"))
				remote = false;
			
			PrintStream log;
			String filename = "log/" + name + "." + Core.toUnsignedString(id) + ".log";
			File logfile = new File(filename);
			if (!logfile.exists()) {
				log = new PrintStream(new FileOutputStream(filename));
			} else {
				log = new PrintStream(new FileOutputStream(filename, true));	
			}
			return new Module(id, path, name, source, log, remote, blocked);

		} catch (RuntimeException e) {
			Core.printError("Module", "load", "Error loading module id.", e.getMessage());
			return null;
		} catch (IOException e) {
			Core.printError("Module", "load", "Unable to initialize output log. "
					+ "(name = " + name + ", id = " + Core.toUnsignedString(id) + ")",
					e.getMessage());
			return null;
		}
	}
	
	public boolean save(Database database, int index)
	{
		return (database.setMetadata(
				"module." + index + ".id", getIdString())
		 && database.setMetadata(
					"module." + index + "path", path)
		 && database.setMetadata(
				 "module." + index + ".name", moduleName)
		 && database.setMetadata(
				 "module." + index + ".source", sourceName)
		 && database.setMetadata(
				"module." + index + ".remote",
				remote ? "1" : "0")
		 && database.setMetadata(
				"module." + index + ".blocked",
				useBlockedDownload ? "1" : "0"));
	}
}
