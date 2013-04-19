package transparent.core;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.Ansi.Erase;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.Ansi.Color;

import transparent.core.database.Database;
import transparent.core.database.MariaDBDriver;

public class Core
{
	public static final byte PRODUCT_LIST_REQUEST = 0;
	public static final byte PRODUCT_INFO_REQUEST = 1;
	
	public static final String NEWLINE = System.getProperty("line.separator");
	
	private static final String SEED_KEY = "seed";
	private static final String MODULE_COUNT = "modules.count";
	private static final String INSTALL_FLAG = "--install";
	private static final String CONSOLE_FLAG = "--console";
	private static final String HELP_FLAG = "--help";
	private static final int THREAD_POOL_SIZE = 64;
	private static final String PROMPT = "$ ";
	
	private static final Sandbox sandbox = new NoSandbox();
	private static HashMap<Long, Module> modules = new HashMap<Long, Module>();
	private static Database database;

	private static ScheduledExecutorService dispatcher =
			Executors.newScheduledThreadPool(THREAD_POOL_SIZE);
	private static Set<Task> queuedJobs =
			Collections.newSetFromMap(new ConcurrentHashMap<Task, Boolean>());
	private static Set<Task> runningJobs =
			Collections.newSetFromMap(new ConcurrentHashMap<Task, Boolean>());
	
	private static long seed = 0;
	private static String input = "";
	private static ReentrantLock consoleLock = new ReentrantLock();
	private static boolean consoleStarted = false;
	private static int nestedLock = 0;

	/* needed to print unsigned 64-bit long values */
	private static final BigInteger B64 = BigInteger.ZERO.setBit(64);
	
	public static String toUnsignedString(long value)
	{
        if (value >= 0) return String.valueOf(value);
        return BigInteger.valueOf(value).add(B64).toString();
	}
	
	/**
	 * Bit-shift random number generator with period 2^64 - 1.
	 * @see http://www.javamex.com/tutorials/random_numbers/xorshift.shtml
	 */
	public static long random()
	{
		seed ^= (seed << 21);
		seed ^= (seed >>> 35);
		seed ^= (seed << 4);
		database.setMetadata("seed", toUnsignedString(seed));
		return seed;
	}
	
	private static void loadSeed()
	{
		String seedValue = database.getMetadata(SEED_KEY);
		if (seedValue == null) {
			Core.printWarning("Core", "loadSeed", "No stored seed, regenerating...");
			while (seed == 0)
				seed = System.nanoTime();
			database.setMetadata(SEED_KEY, Long.toString(seed));
		} else {
			try {
				seed = Long.parseLong(seedValue);
			} catch (NumberFormatException e) {
				Core.printWarning("Core", "loadSeed",
						"Unable to read seed, regenerating...");
				while (seed == 0)
					seed = System.nanoTime();
				database.setMetadata(SEED_KEY, Long.toString(seed));
			}
		}
		Core.flush();
	}
	
	private static boolean loadModules()
	{
		Core.println("Loading modules...");
		String moduleCount = database.getMetadata(MODULE_COUNT);
		if (moduleCount == null) {
			printError("Core", "loadModules",
					"No modules stored. Run with " + INSTALL_FLAG + " flag.");
			return false;
		}
		
		int count;
		try {
			count = Integer.parseInt(moduleCount);
		} catch (NumberFormatException e) {
			printError("Core", "loadModules", "Could not parse module count.");
			return false;
		}
		
		Core.println("Found " + count + " modules.");
		for (int i = 0; i < count; i++) {
			Module module = Module.load(database, i);
			if (module == null) {
				Core.printError("Core", "loadModules",
						"Error loading module at index " + i + ".");
				return false;
			}
			
			Core.println("Loaded module '" + module.getModuleName()
					+ "' (id: " + module.getIdString() + ")");
			modules.put(module.getId(), module);
		}

		Core.println("Done loading modules.");
		return true;
	}

	private static ArrayList<Task> loadQueue(String queue)
	{
		int taskCount = Integer.parseInt(
				database.getMetadata(queue + ".count"));
		
		ArrayList<Task> tasks = new ArrayList<Task>();
		for (int i = 0; i < taskCount; i++) {
			String job = database.getMetadata(queue + "." + i);
			Task task = Task.load(job);
			if (task == null) {
				Core.printError("Core", "loadQueue",
						"Unable to parse task at index " + i + ".");
				continue;
			}
			tasks.add(task);
		}
		return tasks;
	}
	
	private static boolean loadQueue()
	{
		try
		{
			for (Task task : loadQueue("queued"))
				queuedJobs.add(task);
			for (Task task : loadQueue("running"))
				queuedJobs.add(task);
			return true;
		} catch (RuntimeException e) {
			Core.printError("Core", "loadQueue", "Unable to load task queue.");
			return false;
		}
	}
	
	public static Module getModule(long moduleId) {
		return modules.get(moduleId);
	}
	
	public static Database getDatabase() {
		return database;
	}
	
	public static Sandbox getSandbox() {
		return sandbox;
	}
	
	public static void startTask(Task task) {
		queuedJobs.remove(task);
		runningJobs.add(task);
	}
	
	public static void stopTask(Task task) {
		runningJobs.remove(task);
	}
	
	public static void queueTask(Task task) {
		queuedJobs.add(task);
		dispatchTask(task);
	}
	
	private static void dispatchTask(Task task) {
		long delta = task.getTime() - System.currentTimeMillis();
		ScheduledFuture<Object> future =
				dispatcher.schedule(task, Math.max(delta, 0), TimeUnit.MILLISECONDS);
		task.setFuture(future);
	}
	
	private static void printTasks(Collection<Task> tasks)
	{
		Core.println(tasks.size() + " tasks.");
		for (Task task : tasks) {
			String module = "<null>";
			if (task.getModule() != null)
				module = task.getModule().getIdString() + " ("
						+ task.getModule().getModuleName() + ")";
			
			Core.println("Task type: " + task.getType().toString()
					+ ", module: " + module
					+ ", scheduled execution: " + new Date(task.getTime())
					+ ", is running: " + runningJobs.contains(task)
					+ ", reschedules: " + task.reschedules()
					+ ", dummy: " + task.isDummy());
		}
	}
	
	private static void parseCommand(String command)
	{
    	if (command.equals("tasks queued")) {
    		ArrayList<Task> jobs = new ArrayList<Task>();
    		for (Task task : queuedJobs)
    			jobs.add(task);
    		
    		Collections.sort(jobs);
    		printTasks(jobs);
    	} else if (command.equals("tasks running")) {
    		ArrayList<Task> jobs = new ArrayList<Task>();
    		for (Task task : runningJobs)
    			jobs.add(task);
    		
    		Collections.sort(jobs);
    		printTasks(jobs);
    	} else if (command.equals("tasks")) {
    		ArrayList<Task> jobs = new ArrayList<Task>();
    		for (Task task : queuedJobs)
    			jobs.add(task);
    		for (Task task : runningJobs)
    			jobs.add(task);
    		
    		Collections.sort(jobs);
    		printTasks(jobs);
    	} else if (command.equals("modules")) {
    		Core.println(modules.size() + " modules.");
    		for (Module module : modules.values()) {
    			Core.println("Module id: " + module.getIdString()
    					+ ", name: " + module.getModuleName()
    					+ ", source: " + module.getSourceName()
    					+ ", remote: " + module.isRemote()
    					+ ", blockedDownloading: " + module.blockedDownload()
    					+ ", logging: " + module.isLoggingActivity());
    		}
    	} else {
    		Core.println("Unrecognized command '" + command + "'.");
    	}
	}
	
	private static void printPrompt()
	{
		AnsiConsole.out.flush();
    	AnsiConsole.out.print(new Ansi().saveCursorPosition());
		AnsiConsole.out.print(new Ansi().bold());
		AnsiConsole.out.print(PROMPT);
		AnsiConsole.out.print(new Ansi().boldOff());
	}
	
	private static void startConsole()
	{
		try {
			InputStreamReader in = new InputStreamReader(System.in);
	        while (true) {
	        	lockConsole();
	        	consoleStarted = true;
	        	unlockConsole();
	    		Core.flush();
	    		
	        	int c = in.read();
	        	while (c != '\n' && c != -1) {
	        		input += (char) c;
	        		AnsiConsole.out.print((char) c);
	        		c = in.read();
	        	}
	        	
	        	if (input.equals("exit"))
	        		break;
	        	String command = input;
	        	input = "";
	        	parseCommand(command);
	        }
	        in.close();
		} catch (IOException e) {
			printError("Core", "startConsole", "", e.getMessage());
		}
	}
	
	public static void flush() {
		AnsiConsole.out.flush();
	}
	
	public static void lockConsole() {
		boolean held = consoleLock.isHeldByCurrentThread();
		if (!held) {
			consoleLock.lock();
			if (consoleStarted)
				AnsiConsole.out.print(new Ansi().eraseLine(Erase.ALL).restorCursorPosition());
		}
		else nestedLock++;
	}
	
	public static void unlockConsole() {
		if (nestedLock == 0) {
			if (consoleStarted) {
				printPrompt();
				AnsiConsole.out.print(input);
			}
			consoleLock.unlock();
		}
		else nestedLock--;
	}
	
	public static void write(byte[] data) throws IOException
	{
		lockConsole();
		AnsiConsole.out.write(data);
		unlockConsole();
	}
	
	public static void print(String s)
	{
		lockConsole();
		AnsiConsole.out.print(s);
		unlockConsole();
	}
	
	public static void println(String s)
	{
		lockConsole();
		AnsiConsole.out.println(s);
		unlockConsole();
	}
	
	public static void println()
	{
		lockConsole();
		AnsiConsole.out.println();
		unlockConsole();
	}
	
	public static void printWarning(String className,
			String methodName, String message)
	{
		lockConsole();
		AnsiConsole.out.print(new Ansi().bold());
		AnsiConsole.out.print(className + '.' + methodName + " WARNING: ");
		AnsiConsole.out.print(new Ansi().boldOff());
		AnsiConsole.out.println(message);
		unlockConsole();
	}
	
	public static void printError(String className,
			String methodName, String message)
	{
		lockConsole();
		AnsiConsole.out.print(new Ansi().bold().fg(Color.RED));
		AnsiConsole.out.print(className + '.' + methodName + " ERROR: ");
		AnsiConsole.out.print(new Ansi().boldOff().fg(Color.DEFAULT));
		AnsiConsole.out.println(message);
		unlockConsole();
	}
	
	public static void printError(String className,
			String methodName, String message, String exception)
	{
		lockConsole();
		AnsiConsole.out.print(new Ansi().bold().fg(Color.RED));
		AnsiConsole.out.print(className + '.' + methodName + " ERROR: ");
		AnsiConsole.out.print(new Ansi().boldOff().fg(Color.DEFAULT));
		AnsiConsole.out.print(message);
		if (exception != null) {
			if (message.length() > 0)
				AnsiConsole.out.print(' ');
			AnsiConsole.out.print("Exception thrown. ");
			AnsiConsole.out.print(new Ansi().fgBright(Color.BLACK));
			AnsiConsole.out.print(exception);
			AnsiConsole.out.print(new Ansi().fg(Color.DEFAULT));
		}
		AnsiConsole.out.println();
		unlockConsole();
	}
	
	public static void main(String[] args)
	{
		AnsiConsole.systemInstall();
		
		/* parse argument flags */
		boolean install = false;
		boolean console = false;
		if (args.length > 0) {
			if (args[0].equals(INSTALL_FLAG)) {
				install = true;
			} else if (args[0].equals(CONSOLE_FLAG)) {
				console = true;
			} else if (args[0].equals(HELP_FLAG)) {
			} else {
				printError("Core", "main", "Unrecognized flag '" + args[0] + "'.");
			}
		}
		
        try {
            database = new MariaDBDriver();
        } catch (Exception e) {
            printError("Core", "main", "Cannot connect to database.", e.getMessage());
            if (console) {
            	startConsole();
            	return;
            } else System.exit(-1);
        }

		/* load random number generator seed */
        loadSeed();
		
		/* install the modules to the database if we were told to */
		if (install) {
			Core.println("Installing modules...");
			Install.installModules(database);

			Core.println("Installing job queue...");
			Install.installJobQueue(database);

			Core.println("Done.");
			Core.flush();
			return;
		}
		
		/* load the modules */
		if (!loadModules()) {
			printError("Core", "main", "Unable to load modules.");
            if (console)
            	startConsole();
			return;
		}
		
		if (!loadQueue()) {
			Core.printWarning("Core", "main", "Unable to load task queue."
					+ " Creating empty queue...");
		}
		
		ArrayList<String> strings = new ArrayList<String>();
		try {
			Reader r = new InputStreamReader(
					new BufferedInputStream(new FileInputStream("Entity.sql")));
			boolean QUOTE_STATE = false;
			StringBuilder b = new StringBuilder();
			
			boolean done = false;
			while (!done) {
				int c = r.read();
				
				switch (c) {
				case -1:
					if (QUOTE_STATE)
						System.err.println("Should not end in the quote state...");
					done = true;
					break;
				case '\'':
					if (QUOTE_STATE) {
						strings.add(b.toString());
						b = new StringBuilder();
					}
					QUOTE_STATE = !QUOTE_STATE;
					break;
				
				default:
					if (QUOTE_STATE)
					b.append((char) c);
				}
			}
			r.close();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		/* find the newegg module */
		Module newegg = null;
		for (Module m : modules.values()) {
			if (m.getModuleName().equals("NeweggParser"))
				newegg = m;
		}
		String[] strarr = new String[strings.size()];
		strarr = strings.toArray(strarr);
		database.addProductIds(newegg, strarr);


        /* dispatch all tasks in the queue */
		if (!console) {
			for (Task task : queuedJobs)
				dispatchTask(task);
		}
        
		/* start the main loop */
		startConsole();
	}
}

enum TaskType {
	PRODUCT_LIST_PARSE,
	PRODUCT_INFO_PARSE,
	IMAGE_FETCH
}

class Task implements Comparable<Task>, Callable<Object>
{
	private final TaskType type;
	private final Module module;
	private ScheduledFuture<Object> future;
	private long time;
	private boolean reschedules;
	private boolean dummy;
	
	public Task(TaskType type, Module module,
			long time, boolean reschedules, boolean dummy)
	{
		this.type = type;
		this.module = module;
		this.time = time;
		this.reschedules = reschedules;
		this.dummy = dummy;
	}
	
	public TaskType getType() {
		return type;
	}
	
	public Module getModule() {
		return module;
	}
	
	public void setFuture(ScheduledFuture<Object> future) {
		this.future = future;
	}
	
	public ScheduledFuture<Object> getFuture() {
		return future;
	}
	
	public long getTime() {
		return time;
	}
	
	public boolean reschedules() {
		return this.reschedules;
	}
	
	public boolean isDummy() {
		return this.dummy;
	}
	
	public static Task load(String data)
	{
		String[] tokens = data.split("\\.");
		if (tokens.length != 5) {
			Core.printError("Task", "load", "Unable to parse string.");
			return null;
		}
		
		TaskType type;
		if (tokens[0].equals("0"))
			type = TaskType.PRODUCT_LIST_PARSE;
		else if (tokens[0].equals("1"))
			type = TaskType.PRODUCT_INFO_PARSE;
		else if (tokens[0].equals("2"))
			type = TaskType.IMAGE_FETCH;
		else {
			Core.printError("Task", "load", "Unable to parse task type.");
			return null;
		}
		
		long id = Long.parseLong(tokens[1]);
		long time = Long.parseLong(tokens[2]);
		boolean reschedules = !tokens[3].equals("0");
		boolean dummy = !tokens[4].equals("0");
		
		return new Task(type, Core.getModule(id), time, reschedules, dummy);
	}
	
	public String save()
	{
		String typeString;
		switch (type) {
		case PRODUCT_LIST_PARSE:
			typeString = "0";
			break;
		case PRODUCT_INFO_PARSE:
			typeString = "1";
			break;
		case IMAGE_FETCH:
			typeString = "2";
			break;
		default:
			Core.printError("Task", "save", "Unrecognized type.");
			return null;
		}
		
		String reschedulesString = reschedules ? "1" : "0";
		String dummyString = dummy ? "1" : "0";
		return typeString + "." + module.getIdString() + "." + time
				+ "." + reschedulesString + "." + dummyString;
	}

	@Override
	public int compareTo(Task o) {
		if (time < o.time)
			return -1;
		else if (time > o.time)
			return 1;
		else return 0;
	}

	@Override
	public Object call() throws Exception
	{
		/* notify the core that this task has started */
		Core.startTask(this);
		
		ModuleThread wrapper;
		switch (type) {
		case PRODUCT_LIST_PARSE:
			wrapper = new ModuleThread(module, Core.getSandbox(), Core.getDatabase(), dummy);
			wrapper.setRequestType(Core.PRODUCT_LIST_REQUEST);
			wrapper.run();
			Core.stopTask(this);
			return null;
		case PRODUCT_INFO_PARSE:
			wrapper = new ModuleThread(module, Core.getSandbox(), Core.getDatabase(), dummy);
			wrapper.setRequestType(Core.PRODUCT_INFO_REQUEST);
			wrapper.setRequestedProductIds(Core.getDatabase().getProductIds(module));
			wrapper.run();
			Core.stopTask(this);
			return null;
		case IMAGE_FETCH:
			Core.printError("Task", "call", "Image fetching not implemented.");
			Core.stopTask(this);
			return null;
		default:
			Core.printError("Task", "call", "Unrecognized task type.");
			Core.stopTask(this);
			return null;
		}
	}
}
