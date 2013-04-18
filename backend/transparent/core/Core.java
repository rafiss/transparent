package transparent.core;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import transparent.core.database.Database;
import transparent.core.database.MariaDBDriver;

public class Core
{
	public static final byte PRODUCT_LIST_REQUEST = 0;
	public static final byte PRODUCT_INFO_REQUEST = 1;
	
	private static final String SEED_KEY = "seed";
	private static final String MODULE_COUNT = "modules.count";
	private static final String INSTALL_FLAG = "--install";
	private static final int THREAD_POOL_SIZE = 64;
	private static final String PROMPT = "$";
	
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
	
	/**
	 * Bit-shift random number generator with period 2^64 - 1.
	 * @see http://www.javamex.com/tutorials/random_numbers/xorshift.shtml
	 */
	public static long random()
	{
		seed ^= (seed << 21);
		seed ^= (seed >>> 35);
		seed ^= (seed << 4);
		database.setMetadata("seed", Long.toString(seed));
		return seed;
	}
	
	private static void loadSeed()
	{
		String seedValue = database.getMetadata(SEED_KEY);
		if (seedValue == null) {
			System.out.println("Core.loadSeed WARNING: "
					+ "No stored seed, regenerating...");
			while (seed == 0)
				seed = System.nanoTime();
			database.setMetadata(SEED_KEY, Long.toString(seed));
		} else {
			try {
				seed = Long.parseLong(seedValue);
			} catch (NumberFormatException e) {
				System.out.println("Core.loadSeed WARNING: "
						+ "Unable to read seed, regenerating...");
				while (seed == 0)
					seed = System.nanoTime();
				database.setMetadata(SEED_KEY, Long.toString(seed));
			}
		}
		System.out.flush();
	}
	
	private static boolean loadModules()
	{
		System.out.println("Loading modules...");
		String moduleCount = database.getMetadata(MODULE_COUNT);
		if (moduleCount == null) {
			System.err.println("Core.loadModules ERROR: No modules stored."
					+ " Run with " + INSTALL_FLAG + " flag.");
			return false;
		}
		
		int count;
		try {
			count = Integer.parseInt(moduleCount);
		} catch (NumberFormatException e) {
			System.err.println("Core.loadModules ERROR: "
					+ "Could not parse module count.");
			return false;
		}
		
		System.out.println("Found " + count + " modules.");
		for (int i = 0; i < count; i++) {
			Module module = Module.load(database, i);
			if (module == null) {
				System.err.println("Core.loadModules ERROR: "
						+ "Error loading module at index " + i + ".");
				return false;
			}
			
			System.out.println("Loaded module '" + module.getModuleName()
					+ "' (id: " + module.getIdString() + ")");
			modules.put(module.getId(), module);
		}
		
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
				System.err.println("Core.loadQueue ERROR: "
						+ "Unable to parse task at index " + i + ".");
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
			System.err.println("Core.loadQueue ERROR: "
					+ "Unable to load task queue.");
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
	
	private static void printTasks(Iterable<Task> tasks)
	{
		for (Task task : tasks) {
			String module = "<null>";
			if (task.getModule() != null)
				module = task.getModule().getIdString() + " ("
						+ task.getModule().getModuleName() + ")";
			
			System.out.println("Task type: " + task.getType().toString()
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
    		for (Module module : modules.values()) {
    			System.out.println("Module id: " + module.getIdString()
    					+ ", name: " + module.getModuleName()
    					+ ", source: " + module.getSourceName()
    					+ ", remote: " + module.isRemote()
    					+ ", blockedDownloading: " + module.blockedDownload()
    					+ ", logging: " + module.isLoggingActivity());
    		}
    	}
	}
	
	public static void main(String[] args)
	{
        try {
            database = new MariaDBDriver();
        } catch (Exception e) {
            System.err.println("Core.main ERROR: "
            		+ "Cannot connect to database: " + e.getMessage());
            System.exit(-1);
        }

		/* load random number generator seed */
		loadSeed();

		/* parse argument flags */
		if (args.length > 0) {
			if (args[0].equals(INSTALL_FLAG)) {
				System.out.println("Installing modules...");
				Install.installModules(database);

				System.out.println("Installing job queue...");
				Install.installJobQueue(database);

				System.out.println("Done.");
				System.out.flush();
				return;
			} else {
				System.err.println("Core.main ERROR: Unrecognized flag '" + args[0] + "'.");
			}
		}
		
		/* load the modules */
		if (!loadModules()) {
			System.err.println("Core.main ERROR: Unable to load modules.");
			return;
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
        	if (m.getModuleName().equals("Newegg"))
        		newegg = m;
        }
        for (int i = 0; i < strings.size(); i++)
        	database.addProductIds(newegg, strings.get(i));
        
        int i = 2;
        if (i == 2)
        	return;
        
        loadModules();
        loadQueue();

        /* dispatch all tasks in the queue */
		for (Task task : queuedJobs)
			dispatchTask(task);
        
		/* start the main loop */
		Scanner in = new Scanner(System.in);
        while (true) {
        	System.out.println(PROMPT);
        	System.out.flush();
        	String input = in.nextLine();
        	
        	if (input.equals("exit"))
        		break;
        	parseCommand(input);
        }
        in.close();
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
			System.err.println("Task.load ERROR: Unable to parse string.");
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
			System.err.println("Task.load ERROR: Unable to parse task type.");
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
			System.err.println("Task.save ERROR: Unrecognized type.");
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
			System.err.println("Task.call ERROR: Image fetching not implemented.");
			Core.stopTask(this);
			return null;
		default:
			System.err.println("Task.call ERROR: Unrecognized task type.");
			Core.stopTask(this);
			return null;
		}
	}
}
