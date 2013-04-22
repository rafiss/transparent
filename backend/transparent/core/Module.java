package transparent.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigInteger;

import transparent.core.database.Database;

public class Module
{
	/* contains the full command to execute the module, or the remote URL */
	private String path;

	/* name of source for which this module is a parser */
	private String sourceName;

	/* unique name of module */
	private String moduleName;

	/* specifies whether the module is remote */
	private boolean remote;

	/* specifies whether websites should be downloaded in blocks or all at once */
	private boolean useBlockedDownload;

	/* unique integer identifier for the module */
	private final long id;

	/* the output log associated with this module */
	private final PrintStream log;

	/* indicates whether activity should be logged to standard out */
	private boolean logActivity;

	/* the index of this module as it is stored in the persistent database */
	private int persistentIndex = -1;

	public Module(long id, String moduleName,
			String sourceName, String path,
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
	
	public void setPath(String path) {
		this.path = path;
	}
	
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}
	
	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}
	
	public void setRemote(boolean isRemote) {
		this.remote = isRemote;
	}
	
	public void setBlockedDownload(boolean useBlockedDownload) {
		this.useBlockedDownload = useBlockedDownload;
	}
	
	public void setLoggingActivity(boolean logActivity) {
		this.logActivity = logActivity;
	}
	
	public PrintStream getLogStream() {
		return log;
	}
	
	public int getPersistentIndex() {
		return persistentIndex;
	}
	
	@Override
	public boolean equals(Object that) {
		if (that == null) return false;
		else if (that == this) return true;
		else if (!that.getClass().equals(this.getClass()))
			return false;

		Module other = (Module) that;
		return (other.id == id);
	}

	public boolean deepEquals(Module that) {
		if (that == null) return false;
		else if (that == this) return true;

		return (that.id == this.id
				&& that.path.equals(this.path)
				&& that.moduleName.equals(this.moduleName)
				&& that.sourceName.equals(this.sourceName)
				&& that.remote == this.remote
				&& that.useBlockedDownload == this.useBlockedDownload); 
	}

	@Override
	public int hashCode() {
		return (int) ((id >> 32) ^ id);
	}
	
	public void logInfo(String className,
			String methodName, String message)
	{
		log.println(className + '.' + methodName + ": " + message);
		if (logActivity) {
			Console.println("Module " + getIdString() + " (name: '"
					+ moduleName + "') information:" + Core.NEWLINE
					+ className + '.' + methodName + ": " + message);
			Console.flush();
		}
	}
	
	public void logError(String className,
			String methodName, String message)
	{
		log.println(className + '.' + methodName + " ERROR: " + message);
		if (logActivity) {
			Console.lockConsole();
			Console.println("Module " + getIdString() + " (name: '"
					+ moduleName + "') reported error:");
			Console.printError(className, methodName, message);
			Console.unlockConsole();
		}
	}
	
	public void logError(String className, String methodName,
			String message, String exception)
	{
		log.println(className + '.' + methodName + " ERROR: "
				+ message + " Exception thrown: " + exception);
		if (logActivity) {
			Console.lockConsole();
			Console.println("Module " + getIdString() + " (name: '"
					+ moduleName + "') reported error:");
			Console.printError(className, methodName, message, exception);
			Console.unlockConsole();
		}
	}
	
	public void logUserAgentChange(String newUserAgent)
	{
		if (logActivity) {
			Console.lockConsole();
			Console.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') changed user agent to: " + newUserAgent);
			Console.flush();
			Console.unlockConsole();
		}
	}
	
	public void logHttpGetRequest(String url)
	{
		if (logActivity) {
			Console.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') requested HTTP GET: " + url);
			Console.flush();
		}
	}
	
	public void logHttpPostRequest(String url, byte[] post)
	{
		if (logActivity) {
			Console.lockConsole();
			Console.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') requested HTTP POST:");
			Console.println("\tURL: " + url);
			Console.print("\tPOST data: ");
			try {
				Console.write(post);
			} catch (IOException e) {
				Console.print("<unable to write data>");
			}
			Console.println();
			Console.unlockConsole();
			Console.flush();
		}
	}
	
	public void logDownloadProgress(int downloaded)
	{
		if (logActivity) {
			Console.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') downloading: " + downloaded + " bytes");
			Console.flush();
		}
	}
	
	public void logDownloadCompleted(int downloaded)
	{
		if (logActivity) {
			Console.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') completed download: " + downloaded + " bytes");
			Console.flush();
		}
	}
	
	public void logDownloadAborted()
	{
		if (logActivity) {
			Console.println("Module " + getIdString()
					+ " (name: '" + moduleName
					+ "') aborted download due to download size limit.");
			Console.flush();
		}
	}

	public static Module load(long id,
			String name, String source, String path,
			boolean isRemote, boolean blockedDownload)
	{
		PrintStream log;
		try {
			File logdir = new File("log");
			if (!logdir.exists() && !logdir.mkdir()) {
				Console.printError("Module", "load", "Unable to create log directory."
						+ " Logging is disabled for this module. (name = " + name
						+ ", id = " + Core.toUnsignedString(id) + ")");
				log = new PrintStream(new NullOutputStream());
			} else if (!logdir.isDirectory()) {
				Console.printError("Module", "load", "'log' is not a directory."
						+ " Logging is disabled for this module. (name = " + name
						+ ", id = " + Core.toUnsignedString(id) + ")");
				log = new PrintStream(new NullOutputStream());
			} else {
				String filename = "log/" + name + "." + Core.toUnsignedString(id) + ".log";
				File logfile = new File(filename);
				if (!logfile.exists()) {
					log = new PrintStream(new FileOutputStream(filename));
				} else {
					log = new PrintStream(new FileOutputStream(filename, true));
				}
			}
		} catch (IOException e) {
			Console.printError("Module", "load", "Unable to initialize output log."
					+ " Logging is disabled for this module. (name = " + name
					+ ", id = " + Core.toUnsignedString(id) + ")", e.getMessage());
			log = new PrintStream(new NullOutputStream());
		}

		return new Module(id, name, source, path, log, isRemote, blockedDownload);
	}

	public static Module load(Database database, int index)
	{
		long id = -1;
		String path, source;
		boolean blocked = true;
		boolean remote = true;
		String name = "<unknown>";
		try {
			id = new BigInteger(database.getMetadata("module." + index + ".id")).longValue();

			path = database.getMetadata("module." + index + ".path");
			name = database.getMetadata("module." + index + ".name");
			source = database.getMetadata("module." + index + ".source");
			String blockedString = database.getMetadata("module." + index + ".blocked");
			String remoteString = database.getMetadata("module." + index + ".remote");
			
			if (blockedString.equals("0"))
				blocked = false;
			if (remoteString.equals("0"))
				remote = false;
		} catch (RuntimeException e) {
			Console.printError("Module", "load", "Error loading module id.", e.getMessage());
			return null;
		}

		Module module = load(id, name, source, path, remote, blocked);
		module.persistentIndex = index;
		return module;
	}
	
	public boolean save(Database database, int index)
	{
		if (database.setMetadata(
				"module." + index + ".id", getIdString())
		 && database.setMetadata(
					"module." + index + ".path", path)
		 && database.setMetadata(
				 "module." + index + ".name", moduleName)
		 && database.setMetadata(
				 "module." + index + ".source", sourceName)
		 && database.setMetadata(
				"module." + index + ".remote",
				remote ? "1" : "0")
		 && database.setMetadata(
				"module." + index + ".blocked",
				useBlockedDownload ? "1" : "0"))
		{
			persistentIndex = index;
			return true;
		}
		return false;
	}
}

class NullOutputStream extends OutputStream {
	@Override
	public void write(int b) throws IOException { }
}
