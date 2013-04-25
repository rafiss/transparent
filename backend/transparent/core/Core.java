package transparent.core;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
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
	private static final String RUNNING_TASKS = "running";
	private static final String QUEUED_TASKS = "queued";
	private static final String SCRIPT_FLAG = "--script";
	private static final String HELP_FLAG = "--help";
	private static final int THREAD_POOL_SIZE = 64;
	private static final String DEFAULT_SCRIPT = "rc.transparent";

	private static final Sandbox sandbox = new NoSandbox();
	private static Database database;

	private static ScheduledExecutorService dispatcher =
			Executors.newScheduledThreadPool(THREAD_POOL_SIZE);
	private static ReentrantLock tasksLock = new ReentrantLock();
	
	private static long seed = 0;

	/* needed to print unsigned 64-bit long values */
	private static final BigInteger B64 = BigInteger.ZERO.setBit(64);

	/* data structures to keep track of modules */
	private static ReentrantLock modulesLock = new ReentrantLock();
	private static ConcurrentHashMap<Long, Module> modules =
			new ConcurrentHashMap<Long, Module>();

	/* represents the list encoding of modules in the database */
	private static ArrayList<Module> moduleList = new ArrayList<Module>();

	/* represents the list encoding of tasks in the database */
	private static ArrayList<Task> queuedList = new ArrayList<Task>();
	private static ArrayList<Task> runningList = new ArrayList<Task>();

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
			moduleList.ensureCapacity(moduleCount);
			for (int i = 0; i < moduleCount; i++) {
				Module module = Module.load(database, i);
				if (module == null) {
					Console.printError("Core", "loadModules",
							"Error loading module at index " + i + ".");
					return false;
				}

				Console.println("Loaded module '" + module.getModuleName()
						+ "' (id: " + module.getIdString() + ")");
				Module old = modules.put(module.getId(), module);
				if (old != null) {
					Console.printError("Core", "loadModules",
							"Module with id " + old.getIdString()
							+ " (name: '" + old.getModuleName() + "')"
							+ " already exists. Skipping...");
					modules.put(module.getId(), old);
					continue;
				}
				moduleList.add(module);
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
				moduleList.add(module);
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
			int index = module.getIndex();
			if (modules.remove(module.getId()) == null || index == -1) {
				Console.printWarning("Core", "removeModule",
						"Specified module does not exist.");
				return false;
			}

			/* move the last module into the removed module's place */
			if (moduleList.isEmpty())
				return true;
			Module last = moduleList.remove(moduleList.size() - 1);
			module.setIndex(-1);
			if (index != moduleList.size()) {
				moduleList.set(index, last);
				last.setIndex(index);
			}
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
			for (int index = 0; index < moduleList.size(); index++) {
				Module module = moduleList.get(index);
				if ((module.getIndex() == -1
						|| module.getPersistentIndex() != module.getIndex())
						&& !module.save(database, index))
				{
					Console.printError("Core", "saveModules", "Unable to save module '"
							+ module.getModuleName() + "' (id: " + module.getIdString()
							+ ") at position " + index + ".");
					moduleList.set(index, null);
					module.setIndex(-1);
					success = false;
				} else {
					moduleList.set(index, module);
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

	private static boolean loadQueue(String queue, ArrayList<Task> list)
	{
		int taskCount;
		try {
			String taskCountString =
					database.getMetadata(queue + ".count");
			if (taskCountString == null) {
				Console.printError("Core", "loadQueue", "No tasks stored.");
				return false;
			}
			taskCount = Integer.parseInt(taskCountString);
		} catch (NumberFormatException e) {
			Console.printError("Core", "loadQueue", "Unable to parse task count.");
			return false;
		}

		list.ensureCapacity(taskCount);
		for (int i = 0; i < taskCount; i++) {
			if (list.size() > i && list.get(i) != null) {
				Console.printError("Core", "loadQueue",
						"A task already exists at index " + i + ".");
				continue;
			}
			Task task = Task.load(database, queue, i);
			list.add(task);
		}
		return true;
	}

	private static boolean saveQueue(
			boolean isRunning, ArrayList<Task> list)
	{
		String queue = getQueueName(isRunning);

		boolean success = true;
		for (int index = 0; index < list.size(); index++) {
			Task task = list.get(index);
			if ((task.getIndex() == -1 || task.isRunning() != isRunning
					|| task.getPersistentIndex() != task.getIndex())
					&& !task.save(database, isRunning, index))
			{
				Module module = task.getModule();
				Console.printError("Core", "saveQueue", "Unable to save task (module name: '"
						+ module.getModuleName() + "', id: " + module.getIdString()
						+ ") at position " + index + ".");
				task.setIndex(-1);
				list.set(index, null);
				success = false;
			} else {
				list.set(index, task);
			}
		}

		if (database == null)
			success = false;
		else success &= database.setMetadata(
				queue + ".count", Integer.toString(list.size()));
		return success;
	}

	public static boolean loadQueue()
	{
		if (database == null) {
			Console.printError("Core", "loadQueue",
					"No database available. No tasks loaded.");
			return false;
		}

		tasksLock.lock();
		try {
			boolean success = (loadQueue("running", runningList)
					&& loadQueue("queued", queuedList));

	        /* dispatch all tasks in the queue */
			for (Task task : runningList) {
				task.setRunning(true);
				dispatchTask(task);
			}
			for (Task task : queuedList)
				dispatchTask(task);

			return success;
		} finally {
			tasksLock.unlock();
		}
	}

	public static boolean saveQueue()
	{
		if (database == null) {
			Console.printError("Core", "loadQueue",
					"No database available. No tasks loaded.");
			return false;
		}

		tasksLock.lock();
		try {
			return (saveQueue(true, runningList)
					&& saveQueue(false, queuedList));
		} finally {
			tasksLock.unlock();
		}
	}
	
	public static String getQueueName(boolean isRunning) {
		return (isRunning ? RUNNING_TASKS : QUEUED_TASKS);
	}

	public static Module getModule(long moduleId) {
		return modules.get(moduleId);
	}

	public static List<Module> getModules()
	{
		/* to ensure we get a thread-safe snapshot of the current modules */
		modulesLock.lock();
		try {
			return new ArrayList<Module>(modules.values());
		} finally {
			modulesLock.unlock();
		}
	}

	public static int getModuleCount() {
		return modules.size();
	}

	public static int getTaskCount()
	{
		modulesLock.lock();
		try {
			return queuedList.size() + runningList.size();
		} finally {
			modulesLock.unlock();
		}
	}

	public static List<Task> getQueuedTasks() {
		/* to ensure we get a thread-safe snapshot of the queued tasks */
		tasksLock.lock();
		try {
			return new ArrayList<Task>(queuedList);
		} finally {
			tasksLock.unlock();
		}
	}

	public static List<Task> getRunningTasks() {
		/* to ensure we get a thread-safe snapshot of the running tasks */
		tasksLock.lock();
		try {
			return new ArrayList<Task>(runningList);
		} finally {
			tasksLock.unlock();
		}
	}

	public static Database getDatabase() {
		return database;
	}

	public static Sandbox getSandbox() {
		return sandbox;
	}

	public static void queueTask(Task task)
	{
		if (task.isRunning() || task.getIndex() != -1)
			return;

		tasksLock.lock();
		try {
			task.setRunning(false);
			task.setIndex(queuedList.size());
			queuedList.add(task);
			dispatchTask(task);
		} finally {
			tasksLock.unlock();
		}
	}

	public static void startTask(Task task)
	{
		if (task.isRunning())
			return;

		tasksLock.lock();
		try {
			int index = task.getIndex();
			if (index < 0 || index >= queuedList.size())
				return;
			queuedList.remove(index);
			task.setRunning(true);
			task.setIndex(runningList.size());
			runningList.add(task);
			if (!saveQueue())
				Console.printError("Core", "startTask", "Unable to save tasks.");
		} finally {
			tasksLock.unlock();
		}
	}

	private static void removeTask(ArrayList<Task> tasks, Task task)
	{
		int index = task.getIndex();
		if (index < 0 || index >= tasks.size())
			return;
		tasks.remove(index);
		task.setRunning(false);

		/* move the last module into the removed module's place */
		if (tasks.isEmpty())
			return;
		Task last = tasks.remove(tasks.size() - 1);
		task.setIndex(-1);
		if (index != tasks.size()) {
			tasks.set(index, last);
			last.setIndex(index);
		}
	}

	public static void stopTask(Task task, boolean cancelRescheduling)
	{
		tasksLock.lock();
		try {
			if (task.isRunning())
				removeTask(runningList, task);
			else removeTask(queuedList, task);
			Task.removeTask(task.getId());

			/* interrupt the thread if it is running */
			ScheduledFuture<Object> thread = task.getFuture();
			task.stop(cancelRescheduling);
			thread.cancel(true);
		} finally {
			tasksLock.unlock();
		}
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
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(SCRIPT_FLAG)) {
				if (i + 1 < args.length) {
					i++;
					script = args[i];
				} else {
					Console.printError("Core", "main", "Unspecified script filepath.");
				}
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
        	Console.printError("Core", "main", "Cannot "
        			+ "connect to database.", e.getMessage());
        }

		/* load random number generator seed */
        loadSeed();

        /* load the script engine */
        boolean consoleReady = Console.initConsole();
        if (!consoleReady)
        	Console.printError("Core", "main", "Unable to initialize script engine.");

		/* run the user-specified script */
        if (consoleReady && script != null) {
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

		/* start the main loop */
		if (consoleReady)
			Console.runConsole();

		/* tell all tasks to end */
		tasksLock.lock();
		try {
			for (Task task : runningList) {
				if (task != null) {
					task.stop(true);
					if (task.getFuture() != null)
						task.getFuture().cancel(true);
				}
			}
		} finally {
			tasksLock.unlock();
		}
		dispatcher.shutdown();
	}
}
