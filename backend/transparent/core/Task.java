package transparent.core;

import java.math.BigInteger;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

import transparent.core.database.Database;

enum TaskType {
	PRODUCT_LIST_PARSE,
	PRODUCT_INFO_PARSE,
	IMAGE_FETCH
}

public class Task implements Comparable<Task>, Callable<Object>
{
	private final TaskType type;
	private final Module module;
	private ModuleThread wrapper;
	private ScheduledFuture<Object> future;
	private long time;
	private boolean reschedules;
	private boolean dummy;
	private String state;

	/* this id is transient and is regenerated every time the task is loaded */
	private int id;

	private static AtomicInteger ID_COUNTER = new AtomicInteger(0);
	private static ConcurrentHashMap<Integer, Task> tasks =
			new ConcurrentHashMap<Integer, Task>();

	private int persistentIndex = -1;
	private int index = -1;
	private boolean running = false;
	private boolean stopped = false;

	public Task(TaskType type, Module module,
			long time, boolean reschedules, boolean dummy)
	{
		this.type = type;
		this.module = module;
		this.time = time;
		this.reschedules = reschedules;
		this.dummy = dummy;
		this.id = ID_COUNTER.getAndIncrement();
		this.state = "";
		tasks.put(id, this);
	}

	public static Task getTask(int id) {
		return tasks.get(id);
	}

	public static Task removeTask(int id) {
		Task task = tasks.remove(id);
		if (task != null)
			task.id = -1;
		return task;
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

	public int getId() {
		return id;
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

	public int getPersistentIndex() {
		return persistentIndex;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public boolean isRunning() {
		return this.running;
	}

	public void setRunning(boolean running) {
		this.running = running;
	}

	public String getState() {
		if (wrapper != null)
			state = wrapper.getState();
		return this.state;
	}

	private static Task load(String data)
	{
		String[] tokens = data.split("\\.");
		if (tokens.length != 5 && tokens.length != 6) {
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

		long id = new BigInteger(tokens[1]).longValue();
		long time = new BigInteger(tokens[2]).longValue();
		boolean reschedules = !tokens[3].equals("0");
		boolean dummy = !tokens[4].equals("0");

		Task task = new Task(type, Core.getModule(id), time, reschedules, dummy);
		if (tokens.length > 5)
			task.state = unescape(tokens[5]);
		return task;
	}

	public static Task load(Database database, String queue, int index)
	{
		String job = database.getMetadata(queue + "." + index);
		Task task = Task.load(job);
		if (task == null) {
			Console.printError("Task", "load",
					"Unable to parse task at index " + index + ".");
			return null;
		}
		task.persistentIndex = index;
		task.index = index;
		return task;
	}

	private String save()
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
				+ "." + reschedulesString + "." + dummyString + "." + escape(state);
	}

	public boolean save(Database database, boolean isRunning, int index)
	{
		String queue = Core.getQueueName(isRunning);

		if (wrapper != null)
			state = wrapper.getState();
		String data = save();
		if (data != null && database.setMetadata(queue + "." + index, data)) {
			this.persistentIndex = index;
			this.index = index;
			return true;
		}
		return false;
	}

	@Override
	public int compareTo(Task o) {
		if (time < o.time)
			return -1;
		else if (time > o.time)
			return 1;
		else return 0;
	}

	public void stop(boolean cancelRescheduling) {
		if (wrapper != null)
			wrapper.stop();
		if (cancelRescheduling)
			this.stopped = true;
	}

	@Override
	public Object call() throws Exception
	{
		/* notify the core that this task has started */
		Core.startTask(this);

		switch (type) {
		case PRODUCT_LIST_PARSE:
			wrapper = new ModuleThread(module, dummy);
			wrapper.setState(state);
			wrapper.setRequestType(Core.PRODUCT_LIST_REQUEST);
			wrapper.run();
			Core.stopTask(this, false);
			if (reschedules && !stopped) {
				Core.queueTask(new Task(TaskType.PRODUCT_INFO_PARSE,
						module, System.currentTimeMillis(), true, dummy));
				if (!Core.saveQueue())
					Console.printError("Task", "call", "Unable to save tasks.");
			}
			return null;
		case PRODUCT_INFO_PARSE:
			wrapper = new ModuleThread(module, dummy);
			wrapper.setState(state);
			wrapper.setRequestType(Core.PRODUCT_INFO_REQUEST);
			wrapper.setRequestedProductIds(Core.getDatabase().getProductIds(module));
			wrapper.run();
			Core.stopTask(this, false);
			if (reschedules && !stopped) {
				Core.queueTask(new Task(TaskType.PRODUCT_LIST_PARSE,
						module, System.currentTimeMillis(), true, dummy));
				if (!Core.saveQueue())
					Console.printError("Task", "call", "Unable to save tasks.");
			}
			return null;
		case IMAGE_FETCH:
			Console.printError("Task", "call", "Image fetching not implemented.");
			Core.stopTask(this, false);
			if (reschedules) {
				Core.queueTask(new Task(TaskType.IMAGE_FETCH,
						module, System.currentTimeMillis(), true, dummy));
				if (!Core.saveQueue())
					Console.printError("Task", "call", "Unable to save tasks.");
			}
			return null;
		default:
			Console.printError("Task", "call", "Unrecognized task type.");
			Core.stopTask(this, true);
			return null;
		}
	}

	private static String escape(String s) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			switch (s.charAt(i)) {
			case '.':
				builder.append("\\d");
				break;
			case '\\':
				builder.append("\\\\");
				break;
			default:
				builder.append(s.charAt(i));
			}
		}
		return builder.toString();
	}

	private static String unescape(String s) {
		StringBuilder builder = new StringBuilder();
		boolean escape = false;
		for (int i = 0; i < s.length(); i++) {
			switch (s.charAt(i)) {
			case '\\':
				if (escape) {
					builder.append('\\');
					escape = false;
				} else
					escape = true;
				break;
			case 'd':
				if (escape) {
					builder.append('.');
					escape = false;
				} else
					builder.append(s.charAt(i));
				break;
			default:
				builder.append(s.charAt(i));
			}
		}
		return builder.toString();
	}
}
