package transparent.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.util.Locale;

import transparent.core.database.Database;

public class Module
{
	/* contains the full command to execute the module, or the remote URL */
	private String path;

	/* name of source for which this module is a parser */
	private String sourceName;

	/* unique name of module */
	private String moduleName;

	/* module-specific URL (for example, to a repository) */
	private String moduleUrl;

	/* the URL of the online merchant that this module parses */
	private String sourceUrl;

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

	private static final NullOutputStream NULL_STREAM = new NullOutputStream();

	/**
	 * The index of this module as it is stored in the persistent database.
	 * A value of -1 indicates either this module is not stored in the 
	 * underlying database, or that the stored copy is stale.
	 */
	private int persistentIndex = -1;

	/**
	 * The newly-assigned index of this module as it will be stored
	 * in the persistent database when it is saved.
	 */
	private int index = -1;

	public Module(long id, String moduleName,
			String sourceName, String path,
			String url, String sourceUrl,
			OutputStream log, boolean isRemote,
			boolean blockedDownload)
	{
		this.path = path;
		this.sourceName = sourceName;
		this.moduleName = moduleName;
		this.moduleUrl = url;
		this.sourceUrl = sourceUrl;
		this.remote = isRemote;
		this.useBlockedDownload = blockedDownload;
		this.id = id;
		this.log = new ModuleLogStream(log);
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

	public String getSourceUrl() {
		return sourceUrl;
	}

	public String getModuleUrl() {
		return moduleUrl;
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
		this.persistentIndex = -1;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
		this.persistentIndex = -1;
	}

	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
		this.persistentIndex = -1;
	}

	public void setSourceUrl(String url) {
		this.sourceUrl = url;
		this.persistentIndex = -1;
	}

	public void setModuleUrl(String url) {
		this.moduleUrl = url;
		this.persistentIndex = -1;
	}

	public void setRemote(boolean isRemote) {
		if (this.remote != isRemote)
			this.persistentIndex = -1;
		this.remote = isRemote;
	}

	public void setBlockedDownload(boolean useBlockedDownload) {
		if (this.useBlockedDownload != useBlockedDownload)
			this.persistentIndex = -1;
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

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
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
			String message, Exception exception)
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
			String url, String sourceUrl,
			boolean isRemote, boolean blockedDownload)
	{
		PrintStream log;
		try {
			File logdir = new File("log");
			if (!logdir.exists() && !logdir.mkdir()) {
				Console.printError("Module", "load", "Unable to create log directory."
						+ " Logging is disabled for this module. (name = " + name
						+ ", id = " + Core.toUnsignedString(id) + ")");
				log = new PrintStream(NULL_STREAM);
			} else if (!logdir.isDirectory()) {
				Console.printError("Module", "load", "'log' is not a directory."
						+ " Logging is disabled for this module. (name = " + name
						+ ", id = " + Core.toUnsignedString(id) + ")");
				log = new PrintStream(NULL_STREAM);
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
					+ ", id = " + Core.toUnsignedString(id) + ")", e);
			log = new PrintStream(NULL_STREAM);
		}

		return new Module(id, name, source, path, url, sourceUrl, log, isRemote, blockedDownload);
	}

	public static Module load(Database database, int index)
	{
		long id = -1;
		String path, source, url, sourceUrl;
		boolean blocked = true;
		boolean remote = true;
		String name = "<unknown>";
		try {
			id = new BigInteger(database.getMetadata("module." + index + ".id")).longValue();

			path = database.getMetadata("module." + index + ".path");
			name = database.getMetadata("module." + index + ".name");
			source = database.getMetadata("module." + index + ".source");
			url = database.getMetadata("module." + index + ".url");
			sourceUrl = database.getMetadata("module." + index + ".source_url");
			String blockedString = database.getMetadata("module." + index + ".blocked");
			String remoteString = database.getMetadata("module." + index + ".remote");
			
			if (blockedString.equals("0"))
				blocked = false;
			if (remoteString.equals("0"))
				remote = false;
		} catch (RuntimeException e) {
			Console.printError("Module", "load", "Error loading module id.", e);
			return null;
		}

		Module module = load(id, name, source, path, url, sourceUrl, remote, blocked);
		module.persistentIndex = index;
		module.index = index;
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
				 "module." + index + ".url", moduleUrl)
		 && database.setMetadata(
				 "module." + index + ".source_url", sourceUrl)
		 && database.setMetadata(
				"module." + index + ".remote",
				remote ? "1" : "0")
		 && database.setMetadata(
				"module." + index + ".blocked",
				useBlockedDownload ? "1" : "0"))
		{
			this.persistentIndex = index;
			this.index = index;
			return true;
		}
		return false;
	}
	
	class ModuleLogStream extends PrintStream
	{
		public ModuleLogStream(OutputStream log) {
			super(log == null ? NULL_STREAM : log);
		}

	    @Override
	    public void print(boolean b) {
	        super.print(b);
	    	if (logActivity)
		    	Console.print(b);
	    }

	    @Override
	    public void print(char c) {
	        super.print(c);
	    	if (logActivity)
		    	Console.print(c);
	    }

	    @Override
	    public void print(int i) {
	        super.print(i);
	    	if (logActivity)
		    	Console.print(i);
	    }

	    @Override
	    public void print(long l) {
	        super.print(l);
	    	if (logActivity)
		    	Console.print(l);
	    }

	    @Override
	    public void print(float f) {
	        super.print(f);
	    	if (logActivity)
		    	Console.print(f);
	    }

	    @Override
	    public void print(double d) {
	        super.print(d);
	    	if (logActivity)
		    	Console.print(d);
	    }

	    @Override
	    public void print(char s[]) {
	        super.print(s);
	    	if (logActivity)
		    	Console.print(s);
	    }

	    @Override
	    public void print(String s) {
	        super.print(s);
	    	if (logActivity)
		    	Console.print(s);
	    }

	    @Override
	    public void print(Object obj) {
	        super.print(obj);
	    	if (logActivity)
		    	Console.print(obj);
	    }

	    @Override
	    public void println() {
	        super.println();
	    	if (logActivity)
		    	Console.println();
	    }

	    @Override
	    public void println(boolean x) {
	        super.println(x);
	    	if (logActivity)
		    	Console.println(x);
	    }

	    @Override
	    public void println(char x) {
	        super.println(x);
	    	if (logActivity)
		    	Console.println(x);
	    }

	    @Override
	    public void println(int x) {
	        super.println(x);
	    	if (logActivity)
		    	Console.println(x);
	    }

	    @Override
	    public void println(long x) {
	        super.println(x);
	    	if (logActivity)
		    	Console.println(x);
	    }

	    @Override
	    public void println(float x) {
	        super.println(x);
	    	if (logActivity)
		    	Console.println(x);
	    }

	    @Override
	    public void println(double x) {
	        super.println(x);
	    	if (logActivity)
		    	Console.println(x);
	    }

	    @Override
	    public void println(char x[]) {
	        super.println(x);
	    	if (logActivity)
		    	Console.println(x);
	    }

	    @Override
	    public void println(String x) {
	        super.println(x);
	    	if (logActivity)
		    	Console.println(x);
	    }

	    @Override
	    public void println(Object x) {
	        super.println(x);
	    	if (logActivity)
		    	Console.println(x);
	    }

	    @Override
	    public PrintStream format(String format, Object ... args) {
	    	super.format(format, args);
	    	if (logActivity)
	    		Console.format(format, args);
	    	return this;
	    }

	    @Override
	    public PrintStream format(Locale l, String format, Object ... args) {
	    	super.format(l, format, args);
	    	if (logActivity)
	    		Console.format(l, format, args);
	    	return this;
	    }

	    @Override
	    public PrintStream append(CharSequence csq, int start, int end) {
	    	super.append(csq, start, end);
	    	if (logActivity)
	    		Console.append(csq, start, end);
	    	return this;
	    }
	}
}

class NullOutputStream extends OutputStream {
	@Override
	public void write(int b) throws IOException { }
}

