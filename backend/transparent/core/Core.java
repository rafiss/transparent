package transparent.core;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.fusesource.jansi.AnsiConsole;

import transparent.core.database.Database;
import transparent.core.database.MariaDBDriver;

public class Core
{
	public static final byte PRODUCT_LIST_REQUEST = 0;
	public static final byte PRODUCT_INFO_REQUEST = 1;
	
	public static final String NEWLINE = System.getProperty("line.separator");
	
	private static final String SEED_KEY = "seed";
	private static final String MODULE_COUNT = "modules.count";
	private static final String CONSOLE_FLAG = "--console";
	private static final String SCRIPT_FLAG = "--script";
	private static final String HELP_FLAG = "--help";
	private static final int THREAD_POOL_SIZE = 64;
	private static final String DEFAULT_SCRIPT = "rc.transparent";
	
	private static final Sandbox sandbox = new NoSandbox();
	private static Database database;

	private static ScheduledExecutorService dispatcher =
			Executors.newScheduledThreadPool(THREAD_POOL_SIZE);
	private static Set<Task> queuedJobs =
			Collections.newSetFromMap(new ConcurrentHashMap<Task, Boolean>());
	private static Set<Task> runningJobs =
			Collections.newSetFromMap(new ConcurrentHashMap<Task, Boolean>());
	
	private static long seed = 0;

	/* needed to print unsigned 64-bit long values */
	private static final BigInteger B64 = BigInteger.ZERO.setBit(64);

	/* data structures to keep track of modules */
	private static ReentrantLock modulesLock = new ReentrantLock();
	private static ConcurrentHashMap<Long, Module> modules =
			new ConcurrentHashMap<Long, Module>();
	
	/* represents the list encoding of modules in the database */
	private static HashMap<Module, Integer> indexModuleMap =
			new HashMap<Module, Integer>();
	private static HashMap<Integer, Module> moduleIndexMap =
			new HashMap<Integer, Module>();
	private static ArrayList<Module> moduleList = new ArrayList<Module>();
	
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
		if (database != null && !database.setMetadata("seed", toUnsignedString(seed)))
			Console.printWarning("Core", "random", "Cannot save new seed.");
		return seed;
	}
	
	private static void loadSeed()
	{
		if (database == null) {
			Console.printError("Core", "loadSeed",
					"No database available. Generating temporary seed...");
			while (seed == 0)
				seed = System.nanoTime();
			return;
		}

		String seedValue = database.getMetadata(SEED_KEY);
		if (seedValue == null) {
			Console.printWarning("Core", "loadSeed", "No stored seed, regenerating...");
			while (seed == 0)
				seed = System.nanoTime();
			database.setMetadata(SEED_KEY, Long.toString(seed));
		} else {
			try {
				seed = Long.parseLong(seedValue);
			} catch (NumberFormatException e) {
				Console.printWarning("Core", "loadSeed",
						"Unable to read seed, regenerating...");
				while (seed == 0)
					seed = System.nanoTime();
				database.setMetadata(SEED_KEY, Long.toString(seed));
			}
		}
		Console.flush();
	}
	
	public static boolean loadModules()
	{
		if (database == null) {
			Console.printError("Core", "loadModules",
					"No database available. No modules loaded.");
			return false;
		}

		modulesLock.lock();
		try {
			Console.println("Loading modules...");
			String moduleCountString = database.getMetadata(MODULE_COUNT);
			if (moduleCountString == null) {
				Console.printError("Core", "loadModules", "No modules stored.");
				return false;
			}

			int moduleCount;
			try {
				moduleCount = Integer.parseInt(moduleCountString);
			} catch (NumberFormatException e) {
				Console.printError("Core", "loadModules", "Could not parse module count.");
				return false;
			}

			Console.println("Found " + moduleCount + " modules.");
			for (int i = 0; i < moduleCount; i++) {
				Module module = Module.load(database, i);
				if (module == null) {
					Console.printError("Core", "loadModules",
							"Error loading module at index " + i + ".");
					return false;
				}

				Console.println("Loaded module '" + module.getModuleName()
						+ "' (id: " + module.getIdString() + ")");
				modules.put(module.getId(), module);
				moduleList.add(module);
				indexModuleMap.put(module, i);
				moduleIndexMap.put(i, module);
			}
		} finally {
			modulesLock.unlock();
		}

		Console.println("Done loading modules.");
		return true;
	}
	
	public static boolean addModule(Module module)
	{
		modulesLock.lock();
		try {
			Module old = modules.put(module.getId(), module);
			if (old != null) {
				modules.put(old.getId(), old);
				Console.printError("Core", "addModule", "Module with id "
						+ module.getIdString() + " already exists. "
						+ "(name: '" + old.getModuleName() + "')");
				return false;
			} else {
				moduleIndexMap.put(moduleIndexMap.size(), module);
				indexModuleMap.put(module, indexModuleMap.size());
				return true;
			}
		} finally {
			modulesLock.unlock();
		}
	}
	
	public static boolean removeModule(Module module)
	{
		modulesLock.lock();
		try {
			if (modules.remove(module.getId()) == null) {
				Console.printWarning("Core", "removeModule",
						"Specified module does not exist.");
				return false;
			}
			int index = indexModuleMap.remove(module);

			/* move the last module into the removed module's place */
			Module last = moduleIndexMap.remove(moduleIndexMap.size() - 1);
			moduleIndexMap.put(index, last);
			indexModuleMap.put(last, index);
			return true;
		} finally {
			modulesLock.unlock();
		}
	}

	public static boolean saveModules()
	{
		modulesLock.lock();
		try {
			boolean success = true;
			for (Entry<Integer, Module> pair : moduleIndexMap.entrySet()) {
				int index = pair.getKey();
				Module module = pair.getValue();
				if (index < moduleList.size()) {
					Module current = moduleList.get(index);
					if (!current.deepEquals(module) && !module.save(database, index)) {
						Console.printError("Core", "saveModules", "Unable to save module '"
								+ module.getModuleName() + "' (id: " + module.getIdString()
								+ ") at position " + index + ".");
						success = false;
					} else {
						moduleList.set(index, module);
					}
				} else {
					if (!module.save(database, index)) {
						Console.printError("Core", "saveModules", "Unable to save module '"
								+ module.getModuleName() + "' (id: " + module.getIdString()
								+ ") at position " + index + ".");
						success = false;
					} else {
						moduleList.add(module);
					}
				}
			}

			if (database == null)
				success = false;
			else success &= database.setMetadata(
					MODULE_COUNT, Integer.toString(moduleList.size()));
			return success;
		} finally {
			modulesLock.unlock();
		}
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
				Console.printError("Core", "loadQueue",
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
			Console.printError("Core", "loadQueue", "Unable to load task queue.");
			return false;
		}
	}
	
	public static Module getModule(long moduleId) {
		return modules.get(moduleId);
	}
	
	public static Iterable<Module> getModules()
	{
		/* to ensure we get a thread-safe snapshot of the current modules */
		modulesLock.lock();
		try {
			Iterable<Module> toreturn = new ArrayList<Module>(modules.values());
			return toreturn;
		} finally {
			modulesLock.unlock();
		}
	}
	
	public static int getModuleCount() {
		return modules.size();
	}
	
	public static Set<Task> getQueuedTasks() {
		return queuedJobs;
	}
	
	public static Set<Task> getRunningTasks() {
		return runningJobs;
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
	
	public static void main(String[] args)
	{
		AnsiConsole.systemInstall();
		
		/* parse argument flags */
		String script = DEFAULT_SCRIPT;
		boolean console = false;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(SCRIPT_FLAG)) {
				if (i + 1 < args.length) {
					i++;
					script = args[i];
				} else {
					Console.printError("Core", "main", "Unspecified script filepath.");
				}
			} else if (args[i].equals(CONSOLE_FLAG)) {
				console = true;
			} else if (args[i].equals(HELP_FLAG)) {
				/* TODO: implement this */
			} else {
				Console.printError("Core", "main",
						"Unrecognized flag '" + args[i] + "'.");
			}
		}
		
        try {
            database = new MariaDBDriver();
        } catch (Exception e) {
        	Console.printError("Core", "main", "Cannot connect to database.", e.getMessage());
        }

		/* load random number generator seed */
        loadSeed();

		/* run the user-specified script */
        if (script != null) {
        	BufferedReader reader = null;
        	try {
	        	reader = new BufferedReader(new FileReader(script));
	        	String line;
	        	while ((line = reader.readLine()) != null) {
	        		if (!Console.parseCommand(line))
	        			return;
	        	}
        	} catch (FileNotFoundException e) {
        		Console.printError("Core", "main", "Script file '"
        				+ script + "' not found.");
        	} catch (IOException e) {
        		Console.printError("Core", "main", "Error occured"
        				+ " while reading script.", e.getMessage());
        	} finally {
        		try {
	        		if (reader != null)
	        			reader.close();
        		} catch (IOException e) { }
        	}
        }

        /* dispatch all tasks in the queue */
		if (!console) {
			for (Task task : queuedJobs)
				dispatchTask(task);
		}
        
		/* start the main loop */
		Console.startConsole();
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
			Console.printError("Task", "load", "Unable to parse string.");
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
			Console.printError("Task", "load", "Unable to parse task type.");
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
			Console.printError("Task", "save", "Unrecognized type.");
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
			wrapper = new ModuleThread(module, dummy);
			wrapper.setRequestType(Core.PRODUCT_LIST_REQUEST);
			wrapper.run();
			Core.stopTask(this);
			return null;
		case PRODUCT_INFO_PARSE:
			wrapper = new ModuleThread(module, dummy);
			wrapper.setRequestType(Core.PRODUCT_INFO_REQUEST);
			wrapper.setRequestedProductIds(Core.getDatabase().getProductIds(module));
			wrapper.run();
			Core.stopTask(this);
			return null;
		case IMAGE_FETCH:
			Console.printError("Task", "call", "Image fetching not implemented.");
			Core.stopTask(this);
			return null;
		default:
			Console.printError("Task", "call", "Unrecognized task type.");
			Core.stopTask(this);
			return null;
		}
	}
}
