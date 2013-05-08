package transparent.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.fusesource.jansi.AnsiConsole;
import org.simpleframework.http.core.ContainerServer;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;

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
	private static final String PRODUCT_NAME_FIELD = "name";
	private static final String PRODUCT_ID_FIELD = "id";
	private static final String ROW_ID_FIELD = "row";
	private static final String PRICE_FIELD = "price";
	private static final int THREAD_POOL_SIZE = 64;
	private static final int SEARCH_LIMIT = 256;
	private static final int HTTP_SERVER_PORT = 16317;
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

	/* for product name search indexing */
	private static IndexWriter indexWriter = null;
	private static IndexSearcher searcher = null;
	private static StandardQueryParser parser = null;

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

	public static void addToIndex(String productName, ProductID id, int price)
	{
		if (indexWriter == null)
			return;

		Document doc = new Document();
		TextField nameField = new TextField(
				PRODUCT_NAME_FIELD, productName, Field.Store.YES);
		TextField productIdField = new TextField(
				PRODUCT_ID_FIELD, id.getModuleProductId(), Field.Store.YES);
		StoredField rowField = new StoredField(
				ROW_ID_FIELD, id.getRowId());

		doc.add(nameField);
		doc.add(productIdField);
		doc.add(rowField);
//		doc.add(priceField); /* TODO: only the lowest price of a product should be kept */

		try {
			indexWriter.addDocument(doc);
		} catch (IOException e) {
			Console.printError("Core", "addToIndex", "Error adding "
					+ "product name to search index.", e);
		}
	}

	private static HashSet<Character> special = new HashSet<Character>(Arrays.asList(
			'&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', ':', '\\', '/'));

	private static String sanitize(String search) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < search.length(); i++) {
			if (special.contains(search.charAt(i)))
				builder.append('\\');
			builder.append(search.charAt(i));
		}
		return builder.toString();
	}

	public static Iterator<ProductID> searchProductName(
			String term, String sortby, boolean descending)
	{
		Query query = null;
		try {
			query = parser.parse(term, PRODUCT_NAME_FIELD);
		} catch (QueryNodeException e) {
			try {
				query = parser.parse(sanitize(term), PRODUCT_NAME_FIELD);
			} catch (QueryNodeException e2) {
				Console.printError("Core", "searchProductName",
						"Unable to parse search query.", e2);
				return null;
			}
		}

		try {
			Sort sort = new Sort();
			if (sortby != null && sortby.equals("price"))
				sort = new Sort(new SortField(PRICE_FIELD, SortField.Type.INT, descending));
			return new SearchIterator(query, sort);
		} catch (IOException e) {
			Console.printError("Core", "searchProductName",
					"Error occurred during search.", e);
			return null;
		}
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
        			+ "connect to database.", e);
        }

		/* load random number generator seed */
        loadSeed();

        /* load search index for product names */
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_42, analyzer);
        Directory directory = null;
        try {
        	File index = new File("index");
        	if (!index.exists()) {
        		if (!index.mkdir()) {
        			Console.printError("Core", "main",
        					"Unable to create directory 'index'.");
        		} else {
        			directory = FSDirectory.open(index);
        	        indexWriter = new IndexWriter(directory, config);
        	        searcher = new IndexSearcher(DirectoryReader.open(indexWriter, false));
        		}
        	} else if (!index.isDirectory()) {
    			Console.printError("Core", "main",
    					"A file named 'index' already exists.");
        	} else {
        		directory = FSDirectory.open(index);
    	        indexWriter = new IndexWriter(directory, config);
    	        searcher = new IndexSearcher(DirectoryReader.open(indexWriter, false));
        	}
        } catch (IOException e) {
        	Console.printError("Core", "main", "Unable to open "
        			+ "directory 'index'. Using memory index...", e);
        	try {
        		Directory indexDirectory = new RAMDirectory();
                indexWriter = new IndexWriter(indexDirectory, config);
    	        searcher = new IndexSearcher(DirectoryReader.open(indexWriter, false));
        	} catch (IOException e2) {
            	Console.printError("Core", "main", "Unable to "
            			+ "create memory index.", e2);
        	}
        }
        parser = new StandardQueryParser(analyzer);
        parser.setDefaultOperator(Operator.AND);

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
        			+ " Queries will not be processed.");
        }

		/* start the main loop */
        BackgroundWorker worker = new BackgroundWorker();
        ScheduledFuture<?> future =
        		dispatcher.scheduleWithFixedDelay(worker, 10, 10, TimeUnit.SECONDS);
		if (consoleReady)
			Console.runConsole();

		/* tell all tasks to end */
		future.cancel(true);
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

		/* shutdown the HTTP server */
		try {
			if (connection != null)
				connection.close();
		} catch (IOException e) {
			Console.printError("Core", "main", "Unable "
					+ "to shutdown HTTP server.", e);
		}

		/* close the search index */
		try {
			if (indexWriter != null)
				indexWriter.close();
			if (directory != null)
				directory.close();
		} catch (IOException e) {
			Console.printError("Core", "main", "Unable "
					+ "to close search index.", e);
		}

		if (database != null)
			database.close();
	}

	private static class SearchIterator implements Iterator<ProductID>
	{
		private ScoreDoc[] results;
		private Query query;
		private Sort sort;
		private int index;

		public SearchIterator(Query query, Sort sort) throws IOException {
			this.query = query;
			this.sort = sort;
			this.index = 0;

			this.results = searcher.search(query, SEARCH_LIMIT, sort).scoreDocs;
			if (results.length == 0)
				results = null;
		}

		@Override
		public boolean hasNext() {
			return (results != null);
		}

		@Override
		public ProductID next() {
			if (results == null)
				throw new NoSuchElementException();

			ProductID product = null;
			try {
				Document doc = searcher.doc(results[index].doc);
				String moduleProductId = doc.getField(PRODUCT_ID_FIELD).stringValue();
				int row = doc.getField(ROW_ID_FIELD).numericValue().intValue();
				product = new ProductID(row, moduleProductId);
			} catch (IOException e) { }

			index++;
			if (index == results.length) {
				try {
					results = searcher.searchAfter(
							results[index - 1], query, SEARCH_LIMIT, sort).scoreDocs;
				} catch (IOException e) {
					results = null;
				}

				index = 0;
				if (results.length == 0)
					results = null;
			}

			return product;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	private static class BackgroundWorker implements Runnable
	{
		@Override
		public void run()
		{
			try {
				if (indexWriter != null)
					searcher = new IndexSearcher(DirectoryReader.open(indexWriter, false));
			} catch (IOException e) {
				Console.printError("Core", "addToIndex", "Error refreshing "
						+ " search index.", e);
			}

			saveQueue();
		}	
	}
}
