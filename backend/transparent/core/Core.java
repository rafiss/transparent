package transparent.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.fusesource.jansi.AnsiConsole;
import org.simpleframework.http.core.ContainerServer;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import transparent.core.PriceHistory.PriceRecord;
import transparent.core.database.Database;
import transparent.core.database.Database.Relation;
import transparent.core.database.Database.Results;
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
	private static final int HTTP_SERVER_PORT = 16317;
	private static final int MAX_IMAGE_SIZE = 1 << 24;
	private static final BigInteger HUNDRED_QUADRILLION = new BigInteger("100000000000000000");
	private static final BigInteger ONE = new BigInteger("1");
	private static final String IMAGE_PATH = "/var/www/localhost/htdocs/";
	private static final String IMAGE_WEB_PATH = "http://theflowers.us.to/";
	private static final String DEFAULT_SCRIPT = "rc.transparent";

	private static final String SPHINX_PROCESS = "searchd";
	private static final String SPHINX_COMMAND = SPHINX_PROCESS + " --config index/sphinx.conf";

	private static final String REDIS_PROCESS = "redis-server";
	private static final String REDIS_COMMAND = "/usr/sbin/redis-server redis/redis.conf";

	private static final Sandbox sandbox = new NoSandbox();
	private static Database database;

	private static ScheduledExecutorService dispatcher =
			Executors.newScheduledThreadPool(THREAD_POOL_SIZE);
	private static ReentrantLock tasksLock = new ReentrantLock();
	private static ReentrantLock imageQueueLock = new ReentrantLock();
	private static JedisPool pool;
	
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

	public static InetAddress FRONTEND_ADDRESS = null;

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
			while (seed == 0)
				seed = System.nanoTime();
			database.setMetadata(SEED_KEY, Long.toString(seed));
		} else {
			try {
				seed = new BigInteger(seedValue).longValue();
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

	public static void execute(Runnable task) {
		dispatcher.execute(task);
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
			if (task != null)
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
			if (!task.save(database, isRunning, index))
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
		if (index < 0 || index >= tasks.size() || tasks.get(index) != task)
			return;
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

	public static void addPriceRecord(long module, long gid, long price)
	{
		Jedis jedis = pool.getResource();
		PriceHistory history = PriceHistory.load(jedis.get("history." + toUnsignedString(gid)));

		if (history == null)
			history = new PriceHistory();
		history.addRecord(module, new Date().getTime(), price);
		jedis.set("history." + toUnsignedString(gid), history.save());
		pool.returnResource(jedis);
	}

	public static List<PriceRecord> getPriceHistory(long module, long gid)
	{
		Jedis jedis = pool.getResource();
		PriceHistory history = PriceHistory.load(jedis.get("history." + toUnsignedString(gid)));
		pool.returnResource(jedis);
		if (history == null)
			return null;
		return history.getHistory(module);
	}

	public static void addPriceTrack(long gid, Long[] modules, Long price) {
		Jedis jedis = pool.getResource();
		PriceTrigger info = PriceTrigger.load(jedis.get("trigger." + toUnsignedString(gid)));
		if (info == null)
			info = new PriceTrigger();
		if (modules == null)
			info.addTrack(new PriceTrack(price));
		else
			info.addTrack(new PriceTrack(price), modules);
		jedis.set("trigger." + toUnsignedString(gid), info.save());
		pool.returnResource(jedis);
	}

	public static void removePriceTrack(long gid, Long[] modules, Long price) {
		Jedis jedis = pool.getResource();
		PriceTrigger info = PriceTrigger.load(jedis.get("trigger." + toUnsignedString(gid)));
		if (info == null)
			return;
		if (modules == null)
			info.removeTrack(new PriceTrack(price, 0));
		else
			info.removeTrack(new PriceTrack(price, 0), modules);
		jedis.set("trigger." + toUnsignedString(gid), info.save());
		pool.returnResource(jedis);
	}

	public static PriceTrigger getPriceTrigger(long gid) {
		Jedis jedis = pool.getResource();
		PriceTrigger info = PriceTrigger.load(jedis.get("trigger." + toUnsignedString(gid)));
		pool.returnResource(jedis);
		return info;
	}

	public static boolean checkPrice(Module module, long gid, long price) {
		Jedis jedis = pool.getResource();
		PriceTrigger info = PriceTrigger.load(jedis.get("trigger." + toUnsignedString(gid)));
		pool.returnResource(jedis);
		if (info == null)
			return false;

		return info.checkPrice(module, price);
	}

	public static String priceToString(Long price) {
		String cents = String.valueOf(price % 100);
		if (cents.length() == 1)
			cents = "0" + cents;
		return "$" + (price / 100) + "." + cents;
	}

	public static Long parsePrice(String price) {
		try {
			return Long.parseLong(price.replaceAll("\\.", "").replaceAll("\\$", ""));
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static boolean isRunning(String processName) {
		try {
			Process p = Runtime.getRuntime().exec("pidof " + processName);
		    BufferedReader input =
		            new BufferedReader(new InputStreamReader(p.getInputStream()));
			String pid = input.readLine();
		    input.close();
			if (pid == null)
				return false;
			return true;
		} catch (IOException e) {
			Console.printError("Core", "pidof", "", e);
			return false;
		}
	}

	public static void runCommand(String programName, String command)
	{
		try {
			Process process = Runtime.getRuntime().exec(command);
			BufferedReader input =
			            new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = input.readLine();
			while (line != null) {
				Console.println("  " + Console.GRAY + line + Console.DEFAULT);
				line = input.readLine();
			}
		} catch (IOException e) {
			Console.printError("Core", "runCommand", "Error starting " + programName + ".", e);
		}
	}

	private static boolean isLocalImage(String path)
	{
		File local = new File(IMAGE_PATH + path.substring(IMAGE_WEB_PATH.length()));
		return path.startsWith(IMAGE_WEB_PATH) && local.exists();
	}

	private static void setImage(long gid, String image)
	{
		Results results = database.query(null,
								 new String[] { "entity_id", "module_product_id", "module_id" },
								 new String[] { "gid" },
								 new Relation[] { Relation.EQUALS },
								 new Object[] { gid },
								 null, null, false, null, null);
		while (results.next()) {
			Module module = modules.get(results.getLong(3));
			ProductID id = new ProductID(results.getLong(1), results.getString(2));
			database.addProductInfo(module, id, new SimpleEntry<String, Object>("image", image));
		}
	}

	private static void enqueue(long gid, Jedis jedis)
	{
		imageQueueLock.lock();
		try {
			BigInteger end = new BigInteger(jedis.get("images.end"));
			jedis.set("images." + end, toUnsignedString(gid));
			jedis.set("images.end", end.add(ONE).toString());
		} finally {
			imageQueueLock.unlock();
		}
	}

	private static void enqueue(long gid)
	{
		Jedis jedis = pool.getResource();
		enqueue(gid, jedis);
		pool.returnResource(jedis);
	}

	/**
	 * Gets the fetched image address for the given source image. If the source
	 * image address is on the image cache, this method will return the same
	 * address. If no image is found, the given product ID will be enqueued for
	 * later image fetching.
	 */
	public static String getImage(long gid, String image)
	{
		if (image == null || !isLocalImage(image)) {
			/* check to see if image exists */
			Jedis jedis = pool.getResource();
			String stored = jedis.get("imagestore." + image);
			if (stored != null) {
				pool.returnResource(jedis);
				return stored;
			} else {
				stored = jedis.get("imagestore." + gid);
				if (stored != null) {
					pool.returnResource(jedis);
					return stored;
				}

				/* queue this product for later image fetching */
				enqueue(gid);
				pool.returnResource(jedis);
				return null;
			}
		} else {
			return image;
		}
	}

	public static BigInteger getImageQueueStart()
	{
		Jedis jedis = pool.getResource();
		BigInteger start = new BigInteger(jedis.get("images.start"));
		pool.returnResource(jedis);
		return start;
	}

	public static BigInteger getImageQueueEnd()
	{
		Jedis jedis = pool.getResource();
		BigInteger end = new BigInteger(jedis.get("images.end"));
		pool.returnResource(jedis);
		return end;
	}

	public static void fetchImages(Task task, Module authority)
	{
		while (!task.stopped()) {
			/* get the start and end of the queue */
			long gid;
			Jedis jedis = pool.getResource();
			imageQueueLock.lock();
			try {
				Thread.sleep(400);
			} catch (InterruptedException e) {
				return;
			}
			try {
				BigInteger start = new BigInteger(jedis.get("images.start"));
				BigInteger end = new BigInteger(jedis.get("images.end"));
				if (start.equals(end))
					break;
				gid = new BigInteger(jedis.get("images." + start)).longValue();
			} finally {
				imageQueueLock.unlock();
				pool.returnResource(jedis);
			}

			Results results = database.query(null,
											 new String[] { "image" },
											 new String[] { "module_id", "gid" },
											 new Relation[] { Relation.EQUALS, Relation.EQUALS },
											 new Object[] { authority.getId(), gid },
											 null, null, false, null, null);
			if (results == null)
				return; /* database became unavailable, likely because we are exiting */
			String image = null;
			while (results.next()) {
				image = results.getString(1);
			}

			if (image == null) {
				/* the authority does not have an image, so re-add this product to the queue */
				jedis = pool.getResource();
				imageQueueLock.lock();
				try {
					BigInteger start = new BigInteger(jedis.get("images.start"));
					BigInteger end = new BigInteger(jedis.get("images.end"));
					jedis.del("images." + start);
					jedis.set("images.start", start.add(ONE).toString());
					jedis.set("images." + end, toUnsignedString(gid));
					jedis.set("images.end", end.add(ONE).toString());
				} finally {
					imageQueueLock.unlock();
					pool.returnResource(jedis);
				}
			} else if (isLocalImage(image)) {
				/* the image has already been fetched for this GID, so update all associated products */
				setImage(gid, image);

				/* remove this product from the queue */
				jedis = pool.getResource();
				imageQueueLock.lock();
				try {
					BigInteger start = new BigInteger(jedis.get("images.start"));
					jedis.del("images." + start);
					jedis.set("images.start", start.add(ONE).toString());
					jedis.set("imagestore." + gid, image);
				} finally {
					imageQueueLock.unlock();
					pool.returnResource(jedis);
				}
			} else {
				/* download the image from the authority */
				/* ensure that the directory where we wish put the image exists */
				boolean error = false;
				BigInteger bigGid = new BigInteger(toUnsignedString(gid));
				File directory = new File(IMAGE_PATH + bigGid.divide(HUNDRED_QUADRILLION));
				if (!directory.exists()) {
					if (!directory.mkdir()) {
						Console.printError("Core", "fetchImages", "Unable to create directory for cached image.");
						error = true;
					}
				} else if (!directory.isDirectory()) {
					Console.printError("Core", "fetchImages", "Image cache subdirectory is a file!");
					error = true;
				}

				/* download the image into the correct directory */
				String newPath = null;
				if (!error) {
					String extension = image.substring(image.lastIndexOf('.'));
					String suffix = bigGid.divide(HUNDRED_QUADRILLION)
							+ "/" + bigGid.mod(HUNDRED_QUADRILLION) + extension;
					String filename = IMAGE_PATH + suffix;
					newPath = IMAGE_WEB_PATH + suffix;
					try {
						ReadableByteChannel rbc = Channels.newChannel(new URL(image).openStream());
						FileOutputStream fos = new FileOutputStream(filename);
						fos.getChannel().transferFrom(rbc, 0, MAX_IMAGE_SIZE);
						setImage(gid, newPath);
					} catch (IOException e) {
						Console.printError("Core", "fetchImages", "Error occurred while fetching image.", e);
						error = true;
					}
				}

				/* pop the item, and if there is an error, add the product back into the queue */
				jedis = pool.getResource();
				imageQueueLock.lock();
				try {
					BigInteger start = new BigInteger(jedis.get("images.start"));
					BigInteger end = new BigInteger(jedis.get("images.end"));
					jedis.del("images." + start);
					jedis.set("images.start", start.add(ONE).toString());
					if (error) {
						jedis.set("images." + end, toUnsignedString(gid));
						jedis.set("images.end", end.add(ONE).toString());
					} else {
						jedis.set("imagestore." + image, newPath);
						jedis.set("imagestore." + gid, newPath);
					}
				} finally {
					imageQueueLock.unlock();
					pool.returnResource(jedis);
				}
			}
		}
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
        			+ "connect to database.", e);
        }

		/* check to see if Sphinx is running, and if not, start it */
		if (!isRunning(SPHINX_PROCESS)) {
			Console.lockConsole();
			Console.println("Sphinx not running, starting...");
			runCommand("Sphinx", SPHINX_COMMAND);
			Console.unlockConsole();
		}

		/* check to see if Redis is running, and if not, start it */
		if (!isRunning(REDIS_PROCESS)) {
			Console.lockConsole();
			Console.println("Redis not running, starting...");
			runCommand("Redis", REDIS_COMMAND);
			Console.unlockConsole();
		}

		/* load random number generator seed */
        loadSeed();

        /* load the price history/tracking datastore */
        JedisPoolConfig config = new JedisPoolConfig();
        config.maxActive = THREAD_POOL_SIZE;
        pool = new JedisPool(config, "localhost");

		/* setup the image fetching queue */
		Jedis jedis = pool.getResource();
		if (jedis.get("images.start") == null)
			jedis.set("images.start", "0");
		if (jedis.get("images.end") == null)
			jedis.set("images.end", "0");
		pool.returnResource(jedis);
		jedis = pool.getResource();

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
        				+ " while reading script.", e);
        	} finally {
        		try {
	        		if (reader != null)
	        			reader.close();
        		} catch (IOException e) { }
        	}
        }

        /* find the front-end server */
        try {
        	FRONTEND_ADDRESS = InetAddress.getByName("54.244.112.184");
        } catch (UnknownHostException e) {
        	Console.printError("Core", "main", "Cannot find front-end server."
        			+ " Queries will not be processed.");
        }

        /* start the back-end HTTP server */
        Connection connection = null;
        try {
        	connection = new SocketConnection(new ContainerServer(new Server()));
        	connection.connect(new InetSocketAddress(HTTP_SERVER_PORT));
        } catch (IOException e) {
        	Console.printError("Core", "main", "Cannot start HTTP server."
        			+ " Queries will not be processed.", e);
        }

		/* start the main loop */
        BackgroundWorker worker = new BackgroundWorker();
        ScheduledFuture<?> future =
        		dispatcher.scheduleWithFixedDelay(worker, 0, 10, TimeUnit.SECONDS);
		if (consoleReady)
			Console.runConsole();

		/* tell all tasks to end */
		future.cancel(true);
		tasksLock.lock();
		try {
			for (Task task : runningList) {
				if (task != null) {
					task.stop(true);
					if (task.getFuture() != null) {
						task.getFuture().cancel(true);

						/* wait for task to exit */
						try {
							task.getFuture().get();
						} catch (Exception e) { }
					}
				}
			}
		} finally {
			tasksLock.unlock();
		}
		dispatcher.shutdown();

		/* shutdown the HTTP server */
		try {
			if (connection != null)
				connection.close();
		} catch (IOException e) {
			Console.printError("Core", "main", "Unable "
					+ "to shutdown HTTP server.", e);
		}

		if (database != null)
			database.close();
	}
	
	private static class BackgroundWorker implements Runnable
	{
		private static final int INDEXER_PERIOD = 100;
		private static final String INDEXER_COMMAND =
				"indexer -c index/sphinx.conf --all --rotate";

		private int cycles = 0;

		@Override
		public void run()
		{
			if (cycles % INDEXER_PERIOD == 0) {
				Console.lockConsole();
				Console.println("Regenerating search index...");
				runCommand("indexer", INDEXER_COMMAND);
				Console.unlockConsole();
			}

			if (cycles > 0)
				saveQueue();

			cycles++;
		}	
	}
}
