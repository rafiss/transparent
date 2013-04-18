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

	/* needed to print unsigned 64-bit long values */
	private static final BigInteger B64 = BigInteger.ZERO.setBit(64);

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
	
	public long getId() {
		return id;
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
			System.out.println("Module " + toUnsignedString(id) + " (name: '"
					+ moduleName + "') information:");
			System.out.println("\t" + className + '.' + methodName + ": " + message);
			System.out.flush();
		}
	}
	
	public void logError(String className,
			String methodName, String message)
	{
		log.println(className + '.' + methodName + " ERROR: " + message);
		if (logActivity) {
			System.out.println("Module " + toUnsignedString(id) + " (name: '"
					+ moduleName + "') reported error:");
			System.out.println("\t" + className + '.' + methodName + " ERROR: " + message);
			System.out.flush();
		}
	}
	
	public void logUserAgentChange(String newUserAgent)
	{
		if (logActivity) {
			System.out.println("Module " + toUnsignedString(id)
					+ " (name: '" + moduleName
					+ "') changed user agent to: " + newUserAgent);
			System.out.flush();
		}
	}
	
	public void logHttpGetRequest(String url)
	{
		if (logActivity) {
			System.out.println("Module " + toUnsignedString(id)
					+ " (name: '" + moduleName
					+ "') requested HTTP GET: " + url);
			System.out.flush();
		}
	}
	
	public void logHttpPostRequest(String url, byte[] post)
	{
		if (logActivity) {
			System.out.println("Module " + toUnsignedString(id)
					+ " (name: '" + moduleName
					+ "') requested HTTP POST:");
			System.out.println("\tURL: " + url);
			System.out.print("\tPOST data: ");
			try {
				System.out.write(post);
			} catch (IOException e) {
				System.out.print("<unable to write data>");
			}
			System.out.println();
			System.out.flush();
		}
	}
	
	public void logDownloadProgress(int downloaded)
	{
		if (logActivity) {
			System.out.println("Module " + toUnsignedString(id)
					+ " (name: '" + moduleName
					+ "') downloading: " + downloaded + " bytes");
			System.out.flush();
		}
	}
	
	public void logDownloadCompleted(int downloaded)
	{
		if (logActivity) {
			System.out.println("Module " + toUnsignedString(id)
					+ " (name: '" + moduleName
					+ "') completed download: " + downloaded + " bytes");
			System.out.flush();
		}
	}
	
	public void logDownloadAborted()
	{
		if (logActivity) {
			System.out.println("Module " + toUnsignedString(id)
					+ " (name: '" + moduleName
					+ "') aborted download due to download size limit.");
			System.out.flush();
		}
	}

	public static Module load(Database database, int index)
	{
		long id = -1;
		String name = "<unknown>";
		try {
			id = Long.parseLong(database.getMetadata("module." + index + ".id"));

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
			String filename = "log/" + name + "." + id + ".log";
			File logfile = new File(filename);
			if (!logfile.exists()) {
				log = new PrintStream(new FileOutputStream(filename));
			} else {
				log = new PrintStream(new FileOutputStream(filename, true));	
			}
			return new Module(id, path, name, source, log, remote, blocked);

		} catch (RuntimeException e) {
			System.err.println("Module.load ERROR: "
					+ "Error loading module id.");
			return null;
		} catch (IOException e) {
			System.err.println("Module.load ERROR: "
					+ "Unable to initialize output log. "
					+ "(name = " + name + ", id = " + toUnsignedString(id)
					+ ", exception: " + e.getMessage() + ")");
			return null;
		}
	}
	
    public static String toUnsignedString(long num) {
        if (num >= 0) return String.valueOf(num);
        return BigInteger.valueOf(num).add(B64).toString();
    }
	
	public boolean save(Database database, int index)
	{
		return (database.setMetadata(
				"module." + index + ".id", toUnsignedString(id))
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
