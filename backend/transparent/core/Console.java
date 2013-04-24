package transparent.core;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.Ansi.Erase;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.Ansi.Color;

import jline.CandidateListCompletionHandler;
import jline.Completor;
import jline.ConsoleReader;

public class Console
{
	private static Command root = new Command("",
			new ModulesCommand(),
			new TasksCommand(),
			new HistoryCommand(),
			new ExitCommand(),
			new MigrateCommand());

	private static ReentrantLock consoleLock = new ReentrantLock();
	private static int nestedLock = 0;
	private static boolean isReading = false;

	private static final String PROMPT = "$ ";

	/* ANSI string codes for text formatting */
	private static final String BOLD = new Ansi().bold().toString();
	private static final String UNBOLD = new Ansi().boldOff().toString();
	private static final String RED = new Ansi().fg(Color.RED).toString();
	private static final String BLUE = new Ansi().fg(Color.BLUE).toString();
	private static final String GRAY = new Ansi().fgBright(Color.BLACK).toString();
	private static final String DEFAULT = new Ansi().fg(Color.DEFAULT).toString();
	private static final String ERASE = new Ansi().eraseLine(Erase.ALL).toString();

	private static List<Token> tokens = new ArrayList<Token>();
	
	private static ConsoleReader in = null;
	private static final ConsoleCompletor completor = new ConsoleCompletor();
	private static AtomicBoolean historyEnabled = new AtomicBoolean(true);

	public static Boolean parseBoolean(String token)
	{
		if (token.equals("1"))
			return true;
		else if (token.equals("0"))
			return false;

		String lower = token.toLowerCase();
		if (lower.equals("true") || lower.equals("on")
				|| lower.equals("yes") || lower.equals("y"))
			return true;
		else if (lower.equals("false") || lower.equals("off")
				|| lower.equals("no") || lower.equals("n"))
			return false;
		
		return null;
	}

	public static void flush() {
		AnsiConsole.out.flush();
	}

	public static void lockConsole() {
		if (!consoleLock.isHeldByCurrentThread())
			consoleLock.lock();
		else nestedLock++;
	}

	public static void unlockConsole() {
		if (nestedLock == 0)
			consoleLock.unlock();
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
		if (isReading && in != null) {
			AnsiConsole.out.print(ERASE);
			AnsiConsole.out.print(new Ansi().cursorLeft(
					in.getCursorBuffer().cursor + PROMPT.length()));
		}
		AnsiConsole.out.println(s);
		try {
			if (isReading && in != null)
				in.restoreLine();
		} catch (IOException e) { }
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
		commandWarning(className + '.' + methodName, message);
	}

	public static void printError(String className,
			String methodName, String message)
	{
		commandError(className + '.' + methodName, message);
	}

	public static void printError(String className,
			String methodName, String message, String exception)
	{
		commandError(className + '.' + methodName, message, exception);
	}

	private static void commandWarning(String command, String message)
	{
		lockConsole();
		AnsiConsole.out.print(BOLD);
		AnsiConsole.out.print(command + " WARNING:");
		AnsiConsole.out.print(UNBOLD);
		AnsiConsole.out.print(message);
		AnsiConsole.out.println();
		unlockConsole();
	}

	private static void commandError(String command, String message)
	{
		lockConsole();
		AnsiConsole.out.print(RED + BOLD);
		AnsiConsole.out.print(command + " ERROR: ");
		AnsiConsole.out.print(UNBOLD + DEFAULT);
		AnsiConsole.out.print(message);
		AnsiConsole.out.println();
		unlockConsole();
	}

	private static void commandError(String command, String message, String exception)
	{
		lockConsole();
		AnsiConsole.out.print(RED + BOLD);
		AnsiConsole.out.print(command + " ERROR: ");
		AnsiConsole.out.print(UNBOLD + DEFAULT);
		AnsiConsole.out.print(message);
		if (exception != null) {
			if (message.length() > 0)
				AnsiConsole.out.print(' ');
			AnsiConsole.out.print("Exception thrown. ");
			AnsiConsole.out.print(GRAY);
			AnsiConsole.out.print(exception);
			AnsiConsole.out.print(DEFAULT);
		}
		AnsiConsole.out.println();
		unlockConsole();
	}

	private static void escape(StringBuilder token, char escapeChar)
	{
		switch (escapeChar) {
		case '"':
			token.append('"');
			break;

		case '\'':
			token.append('"');
			break;

		case ' ':
			token.append(' ');
			break;

		case '\n':
			token.append('\n');
			break;

		default:
			token.append('\\');
			token.append(escapeChar);
		}
	}

	private static Cursor lexCommand(String input, int cursor, List<Token> tokens)
	{
		int start = 0;
		StringBuilder token = new StringBuilder();

		Cursor pointer = null;
		LexerState state = LexerState.NORMAL;
		for (int i = 0; i < input.length(); i++)
		{
			if (i == cursor)
				pointer = new Cursor(tokens.size(), token.length());

			switch (state) {
			case NORMAL:
				if (input.charAt(i) == '\'')
					state = LexerState.SINGLE_QUOTE;
				else if (input.charAt(i) == '\"')
					state = LexerState.DOUBLE_QUOTE;
				else if (input.charAt(i) == '\\')
					state = LexerState.NORMAL_ESCAPE;
				else if (Character.isWhitespace(input.charAt(i))) {
					if (token.length() > 0)
						tokens.add(new Token(token.toString(), start, i - start));
					token = new StringBuilder();
					start = i + 1;
				} else
					token.append(input.charAt(i));
				break;

			case SINGLE_QUOTE:
				if (input.charAt(i) == '\'')
					state = LexerState.NORMAL;
				else
					token.append(input.charAt(i));
				break;

			case DOUBLE_QUOTE:
				if (input.charAt(i) == '\"')
					state = LexerState.NORMAL;
				else if (input.charAt(i) == '\\')
					state = LexerState.QUOTE_ESCAPE;
				else
					token.append(input.charAt(i));
				break;

			case NORMAL_ESCAPE:
				escape(token, input.charAt(i));
				state = LexerState.NORMAL;
				break;

			case QUOTE_ESCAPE:
				escape(token, input.charAt(i));
				state = LexerState.DOUBLE_QUOTE;
				break;

			default:
				printError("Console", "parseCommand", "Unrecognized lexer state.");
			}
		}

		if (pointer == null)
			pointer = new Cursor(tokens.size(), token.length());

		if (token.length() > 0)
			tokens.add(new Token(token.toString(), start, input.length() - start));
		
		return pointer;
	}

	private static void printTasks(
			Collection<Task> tasks, boolean isRunning)
	{
		for (Task task : tasks) {
			String module = "<null>";
			if (task.getModule() != null)
				module = task.getModule().getIdString() + " ("
						+ task.getModule().getModuleName() + ")";

			print(BLUE + BOLD + "[" + task.getId() + "]" + UNBOLD + DEFAULT);
			println(" Task type: " + task.getType().toString());
			println(GRAY + "  module: " + DEFAULT + module);
			println(GRAY + "  scheduled execution: " + DEFAULT + new Date(task.getTime()));
			println(GRAY + "  is running: " + DEFAULT + isRunning);
			println(GRAY + "  reschedules: " + DEFAULT + task.reschedules());
			println(GRAY + "  dummy: " + DEFAULT + task.isDummy());
		}
	}

	public static boolean parseCommand(String line)
	{
		lexCommand(line, line.length(), tokens);

		if (tokens.size() > 0) {
    		if (tokens.get(0).getToken().equals("exit"))
    			return false;
    		try {
    			root.run(tokens, 0);
    		} catch (Exception e) {
    			printError("Console", "parseCommand", "", e.getMessage());
    		}

    		in.setUseHistory(historyEnabled.get());
    		if (in.getCompletors().isEmpty())
    			in.addCompletor(completor);
    		if (consoleLock.isHeldByCurrentThread())
    			consoleLock.unlock();
		}
    	tokens.clear();
    	return true;
	}

	public static boolean initConsole()
	{
		try {
			lockConsole();
			in = new ConsoleReader();
			in.addCompletor(completor);

			CandidateListCompletionHandler handler = new CandidateListCompletionHandler();
			handler.setAlwaysIncludeNewline(false);
			in.setCompletionHandler(handler);
			unlockConsole();

			return true;
		} catch (IOException e) {
			printError("Core", "initConsole", "", e.getMessage());
			return false;
		}
	}

	public static void runConsole()
	{
		try {
	        while (true) {
	        	isReading = true;
	    		String input = in.readLine(BOLD + PROMPT + UNBOLD);
	        	isReading = false;

	    		if (!parseCommand(input))
	    			break;
	        }
		} catch (IOException e) {
			printError("Core", "startConsole", "", e.getMessage());
		}
	}

	private static class ConsoleCompletor implements Completor
	{
		private int complete(Command command, List<Token> tokens,
				Cursor cursor, int tokenIndex, List<String> completions)
		{
			String match = "";
			if (tokenIndex < tokens.size())
				match = tokens.get(tokenIndex).getToken();
			if (tokenIndex == cursor.getTokenIndex())
			{
				match = match.substring(0, cursor.getTokenPosition());

				for (Command subcommand : command.getSubcommands()) {
					if (subcommand.getName().startsWith(match))
						completions.add(subcommand.getName() + ' ');
				}

				if (tokenIndex < tokens.size())
					return tokens.get(tokenIndex).getSourcePosition();
				else if (tokens.size() > 0)
					return tokens.get(tokenIndex - 1).getSourcePosition()
						+ tokens.get(tokenIndex - 1).getSourceLength() + 1;
				else return 0;
			} else {
				int found = -1;
				boolean multiple = false;
				for (Command subcommand : command.getSubcommands()) {
					if (subcommand.getName().equals(match)) {
						int newfound = complete(subcommand, tokens, cursor,
								tokenIndex + 1, completions);
						if (newfound != -1)
						{
							if (found == -1) {
								found = newfound;
							} else
								multiple = true;
						}
					}
				}
				
				if (multiple) return tokens.get(tokenIndex).getSourcePosition();
				else return found;
			}
		}

		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public int complete(String input, int pos, List completions)
		{
			List<Token> tokens = new ArrayList<Token>();
			Cursor cursor = lexCommand(input, pos, tokens);
			int position = complete(root, tokens, cursor, 0, completions);
			if (position == -1)
				return 0;
			return position;
		}
	}

	private static class ModulesCommand extends Command
	{
		public ModulesCommand() {
			super("modules",
					new AddModuleCommand(false),
					new AddModuleCommand(true),
					new RemoveModuleCommand(),
					new LoadModulesCommand(),
					new SaveModulesCommand(),
					new GetModuleCommand(),
					new SetModuleCommand());
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (args.size() > 1) {
				super.run(args, index);
				return;
			}

			Console.lockConsole();
    		println(Core.getModuleCount() + " module(s).");
    		for (Module module : Core.getModules()) {
    			println(BOLD + "Module id: " + module.getIdString() + UNBOLD);
    			println(GRAY + "  name: " + DEFAULT + module.getModuleName());
    			println(GRAY + "  source: " + DEFAULT + module.getSourceName());
    			println(GRAY + "  is remote: " + DEFAULT + module.isRemote());
    			println(GRAY + "  blocked downloading: " + DEFAULT + module.blockedDownload());
    			println(GRAY + "  active logging: " + DEFAULT + module.isLoggingActivity());
    			println(GRAY + "  is saved: " + DEFAULT + (module.getPersistentIndex() != -1));
    		}
			Console.unlockConsole();
		}
	}

	private static class AddModuleCommand extends Command
	{
		private final boolean force;

		public AddModuleCommand(boolean force) {
			super(force ? "forceadd" : "add");
			this.force = force;
		}

		private void usage() {
			Console.println("usage: modules add [name] [source]"
					+ " [path] [is remote] [use blocked downloading]");
		}

		private void addModule(String name, String source,
				String path, boolean remote, boolean blocked)
		{
			long id = Core.random();
			Console.lockConsole();
			if (!force) {
				Console.println(BOLD + "Module id: " + Core.toUnsignedString(id) + UNBOLD);
				Console.println(GRAY + "  name: " + DEFAULT + name);
				Console.println(GRAY + "  source: " + DEFAULT + source);
				Console.println(GRAY + "  path: " + DEFAULT + path);
				Console.println(GRAY + "  is remote: " + DEFAULT + remote);
				Console.println(GRAY + "  use blocked downloading: " + DEFAULT + blocked);
			}

			try {
				Boolean response = true;

				if (!force) {
					in.setUseHistory(false);
					response = parseBoolean(in.readLine("Add this module? "));
					while (response == null) {
						Console.println("Must specify a boolean parameter.");
						response = parseBoolean(in.readLine("Add this module? "));
					}
				}
				if (response) {
					Module module = Module.load(id, name, source, path, remote, blocked);
					if (module != null && Core.addModule(module))
						Console.println("Module '" + name + "' added. "
								+ "Use 'modules save' to push changes to database.");
				}
			} catch (IOException e) {
				return;
			} finally {
				Console.unlockConsole();
			}
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (args.size() > 2) {
				/* parse the arguments */
				if (args.size() < 5) {
					Console.lockConsole();
					commandError("modules add", "Too few arguments.");
					usage();
					Console.unlockConsole();
					return;
				} else if (args.size() > 7) {
					Console.lockConsole();
					commandError("modules add", "Too many arguments.");
					usage();
					Console.unlockConsole();
					return;
				}

				String name = args.get(2).getToken();
				String source = args.get(3).getToken();
				String path = args.get(4).getToken();

				Boolean remote = false;
				if (args.size() > 6)
					remote = parseBoolean(args.get(5).getToken());

				Boolean blocked = true;
				if (args.size() > 7)
					blocked = parseBoolean(args.get(6).getToken());

				if (remote == null || blocked == null) {
					Console.println("[is remote] and [use blocked downloading]"
							+ " must be boolean arguments.");
					return;
				}

				addModule(name, source, path, remote, blocked);
			} else {
				try {
					in.setUseHistory(false);
					String name = in.readLine("Enter module name: ");
					String source = in.readLine("Enter module source: ");
					String path = in.readLine("Enter path: ");

					Boolean remote = parseBoolean(
							in.readLine("Is the module remote? "));
					while (remote == null) {
						Console.println("Must specify a boolean parameter.");
						remote = parseBoolean(
								in.readLine("Is the module remote? "));
					}

					Boolean blocked = parseBoolean(
							in.readLine("Use blocked downloading? "));
					while (blocked == null) {
						Console.println("Must specify a boolean parameter.");
						blocked = parseBoolean(
								in.readLine("Use blocked downloading? "));
					}

					addModule(name, source, path, remote, blocked);
				} catch (IOException e) {
					commandError("modules add", "", e.getMessage());
				}
			}
		}
	}

	private static class GetModuleCommand extends Command
	{
		public GetModuleCommand() {
			super("get");
		}

		private void usage() {
			println("usage: modules get [id] [name|source|path|remote|blocked|activelog]");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (args.size() != 4) {
				lockConsole();
				commandError("modules set", "Incorrect number of arguments.");
				usage();
				unlockConsole();
				return;
			}

			long id;
			try {
				id = new BigInteger(args.get(2).getToken()).longValue();
			} catch (NumberFormatException e) {
				commandError("modules set", "Unable to parse module id.");
				return;
			}

			Module module = Core.getModule(id);
			if (module == null) {
				commandError("modules set", "No module found with specified id.");
				return;
			}
			String key = args.get(3).getToken();
			if (key.equals("name"))
				println(module.getModuleName());
			else if (key.equals("source"))
				println(module.getSourceName());
			else if (key.equals("path"))
				println(module.getPath());
			else if (key.equals("remote")) {
				println(Boolean.toString(module.isRemote()));
			} else if (key.equals("blocked")) {
				println(Boolean.toString(module.blockedDownload()));
			} else if (key.equals("activelog")) {
				println(Boolean.toString(module.isLoggingActivity()));
			}
		}
	}

	private static class SetModuleCommand extends Command
	{
		public SetModuleCommand() {
			super("set");
		}

		private void usage() {
			println("usage: modules set [id] [name|source"
					+ "|path|remote|blocked|activelog] [value]");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (args.size() != 5) {
				lockConsole();
				commandError("modules set", "Incorrect number of arguments.");
				usage();
				unlockConsole();
				return;
			}

			long id;
			try {
				id = new BigInteger(args.get(2).getToken()).longValue();
			} catch (NumberFormatException e) {
				commandError("modules set", "Unable to parse module id.");
				return;
			}

			Module module = Core.getModule(id);
			if (module == null) {
				commandError("modules set", "No module found with specified id.");
				return;
			}
			String key = args.get(3).getToken();
			String value = args.get(4).getToken();
			if (key.equals("name"))
				module.setModuleName(value);
			else if (key.equals("source"))
				module.setSourceName(value);
			else if (key.equals("path"))
				module.setPath(value);
			else if (key.equals("remote")) {
				Boolean parsed = parseBoolean(value);
				if (parsed != null)
					module.setRemote(parsed);
				else {
					commandError("modules set", "Unable to parse boolean parameter.");
				}
			} else if (key.equals("blocked")) {
				Boolean parsed = parseBoolean(value);
				if (parsed != null)
					module.setBlockedDownload(parsed);
				else {
					commandError("modules set", "Unable to parse boolean parameter.");
				}
			} else if (key.equals("activelog")) {
				Boolean parsed = parseBoolean(value);
				if (parsed != null)
					module.setLoggingActivity(parsed);
				else {
					commandError("modules set", "Unable to parse boolean parameter.");
				}
			}
		}
	}

	private static class SaveModulesCommand extends Command
	{
		public SaveModulesCommand() {
			super("save");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (Core.saveModules())
				println("Successfully saved " + Core.getModuleCount() + " modules.");
			else
				commandError("modules save", "Error occurred while saving modules.");
		}
	}

	private static class RemoveModuleCommand extends Command
	{
		public RemoveModuleCommand() {
			super("remove");
		}
		
		private void usage() {
			println("usage: modules remove [id]");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (args.size() != 3) {
				lockConsole();
				commandError("modules remove", "Incorrect number of arguments.");
				usage();
				unlockConsole();
				return;
			}

			long id;
			try {
				id = new BigInteger(args.get(2).getToken()).longValue();
			} catch (NumberFormatException e) {
				commandError("modules remove", "Unable to parse module id.");
				return;
			}

			Module module = Core.getModule(id);
			if (module == null) {
				commandError("modules remove", "No module found with specified id.");
				return;
			}

			if (!Core.removeModule(module))
				commandError("modules remove", "Could not remove module.");
		}
	}

	private static class LoadModulesCommand extends Command
	{
		public LoadModulesCommand() {
			super("load");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (!Core.loadModules())
				commandError("modules load", "Error occurred while loading modules.");
		}
	}

	private static class TasksCommand extends Command
	{
		public TasksCommand() {
			super("tasks",
					new LoadTasksCommand(),
					new SaveTasksCommand(),
					new QueuedTasksCommand(),
					new RunningTasksCommand(),
					new AddTaskCommand(true),
					new AddTaskCommand(false),
					new RemoveTaskCommand());
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (args.size() > 1) {
				super.run(args, index);
				return;
			}

			List<Task> queued = Core.getQueuedTasks();
			List<Task> running = Core.getRunningTasks();
    		Collections.sort(queued);
    		Collections.sort(running);

    		Console.lockConsole();
			println((queued.size() + running.size()) + " total task(s).");
    		printTasks(queued, false);
    		printTasks(running, true);
    		Console.unlockConsole();
		}
	}

	private static class LoadTasksCommand extends Command
	{
		public LoadTasksCommand() {
			super("load");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (!Core.loadQueue())
				commandError("tasks load", "Error occurred while loading tasks.");
		}
	}

	private static class SaveTasksCommand extends Command
	{
		public SaveTasksCommand() {
			super("save");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (Core.saveQueue())
				println("Successfully saved " + Core.getTaskCount() + " tasks.");
			else
				commandError("tasks save", "Error occurred while saving tasks.");
		}
	}

	private static class QueuedTasksCommand extends Command
	{
		public QueuedTasksCommand() {
			super("queued");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			List<Task> queued = Core.getQueuedTasks();
    		Collections.sort(queued);

    		Console.lockConsole();
			println(queued.size() + " task(s) queued.");
    		printTasks(queued, false);
    		Console.unlockConsole();
		}
	}

	private static class RunningTasksCommand extends Command
	{
		public RunningTasksCommand() {
			super("running");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			List<Task> running = Core.getRunningTasks();
    		Collections.sort(running);

    		Console.lockConsole();
			println(running.size() + " task(s) running.");
    		printTasks(running, true);
    		Console.unlockConsole();
		}
	}

	private static class AddTaskCommand extends Command
	{
		private final boolean force;

		public AddTaskCommand(boolean force) {
			super(force ? "forceadd" : "add");
			this.force = force;
		}

		private void usage() {
			Console.println("usage: tasks add [parse type] [module id]"
					+ " [start time] [reschedules] [dummy]");
			Console.println("  [parse type] can either be 'info', 'list', or 'image'.");
			Console.println("  [start time] must be an integer indicating milliseconds from now.");
			Console.println("  [dummy] indicates whether information is written to the database.");
			Console.println("  By default, reschedules is false and dummy is true.");
		}

		private void addTask(TaskType type, Module module,
				long time, boolean reschedules, boolean dummy)
		{
			Console.lockConsole();
			if (!force) {
				Console.println(BOLD + "Task type: " + type.toString() + UNBOLD);
				Console.println(GRAY + "  module id: " + DEFAULT + module.getIdString());
				Console.println(GRAY + "  module name: " + DEFAULT + module.getModuleName());
				Console.println(GRAY + "  time: " + DEFAULT + new Date(time).toString());
				Console.println(GRAY + "  reschedules: " + DEFAULT + reschedules);
				Console.println(GRAY + "  is dummy: " + DEFAULT + dummy);
			}

			try {
				Boolean response = true;

				if (!force) {
					response = parseBoolean(in.readLine("Add this task? "));
					in.setUseHistory(false);
					while (response == null) {
						Console.println("Must specify a boolean parameter.");
						response = parseBoolean(in.readLine("Add this task? "));
					}
				}
				if (response) {
					Task task = new Task(type, module, time, reschedules, dummy);
					if (task != null) {
						Core.queueTask(task);
						Console.println("Task queued. "
								+ "Use 'tasks save' to push changes to database.");
					}
				}
			} catch (IOException e) {
				return;
			} finally {
				Console.unlockConsole();
			}
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (args.size() > 2) {
				if (args.size() < 5) {
					lockConsole();
					commandError("tasks add", "Too few arguments.");
					usage();
					unlockConsole();
					return;
				}

				String typeString = args.get(2).getToken().toLowerCase();
				TaskType type;
				if (typeString.equals("list")) {
					type = TaskType.PRODUCT_LIST_PARSE;
				} else if (typeString.equals("info")) {
					type = TaskType.PRODUCT_INFO_PARSE;
				} else if (typeString.equals("image")) {
					type = TaskType.IMAGE_FETCH;
				} else {
					lockConsole();
					commandError("tasks add", "Unable to parse task type.");
					usage();
					unlockConsole();
					return;
				}

				long id;
				try {
					id = new BigInteger(args.get(3).getToken()).longValue();
				} catch (NumberFormatException e) {
					commandError("tasks add", "Unable to parse time.");
					return;
				}

				Module module = Core.getModule(id);
				if (module == null) {
					commandError("tasks add", "No module found with specified id.");
					return;
				}

				int time;
				try {
					time = Integer.parseInt(args.get(4).getToken());
				} catch (NumberFormatException e) {
					commandError("tasks add", "Unable to parse module id.");
					return;
				}

				Boolean reschedules = false;
				if (args.size() > 6)
					reschedules = parseBoolean(args.get(5).getToken());

				Boolean dummy = true;
				if (args.size() > 7)
					dummy = parseBoolean(args.get(6).getToken());

				if (reschedules == null || dummy == null) {
					Console.println("[is remote] and [use blocked downloading]"
							+ " must be boolean arguments.");
					return;
				}

				addTask(type, module, System.currentTimeMillis()
						+ time, reschedules, dummy);
			} else {
				try {
					in.setUseHistory(false);
					String typeString = in.readLine("Enter task type: (list|info|image) ").toLowerCase();
					TaskType type;
					if (typeString.equals("list")) {
						type = TaskType.PRODUCT_LIST_PARSE;
					} else if (typeString.equals("info")) {
						type = TaskType.PRODUCT_INFO_PARSE;
					} else if (typeString.equals("image")) {
						type = TaskType.IMAGE_FETCH;
					} else {
						lockConsole();
						commandError("tasks add", "Unable to parse task type.");
						usage();
						unlockConsole();
						return;
					}					

					long id;
					try {
						id = new BigInteger(in.readLine("Enter module source: ")).longValue();
					} catch (NumberFormatException e) {
						commandError("tasks add", "Unable to parse module id.");
						return;
					}

					Module module = Core.getModule(id);
					if (module == null) {
						commandError("tasks add", "No module found with specified id.");
						return;
					}

					int time;
					try {
						time = Integer.parseInt(in.readLine(
								"Enter scheduled time (in ms from now): "));
					} catch (NumberFormatException e) {
						commandError("tasks add", "Unable to time.");
						return;
					}

					Boolean reschedules = parseBoolean(in.readLine(
							"Does this task automatically reschedule? "));
					while (reschedules == null) {
						Console.println("Must specify a boolean parameter.");
						reschedules = parseBoolean(in.readLine(
								"Does this task automatically reschedule? "));
					}

					Boolean dummy = parseBoolean(in.readLine(
							"Disable writing to the database? "));
					while (dummy == null) {
						Console.println("Must specify a boolean parameter.");
						dummy = parseBoolean(in.readLine(
								"Disable writing to the database? "));
					}

					addTask(type, module, System.currentTimeMillis()
							+ time, reschedules, dummy);
				} catch (IOException e) {
					commandError("modules add", "", e.getMessage());
				}
			}
		}
	}

	private static class RemoveTaskCommand extends Command
	{
		public RemoveTaskCommand() {
			super("remove");
		}
		
		private void usage() {
			println("usage: tasks remove [id]");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (args.size() != 3) {
				lockConsole();
				commandError("tasks remove", "Incorrect number of arguments.");
				usage();
				unlockConsole();
				return;
			}

			int id;
			try {
				id = Integer.parseInt(args.get(2).getToken());
			} catch (NumberFormatException e) {
				commandError("tasks remove", "Unable to parse task id.");
				return;
			}

			Task task = Task.getTask(id);
			if (task == null) {
				commandError("tasks remove", "No task found with specified id.");
				return;
			}

			Core.stopTask(task);
		}
	}

	private static class HistoryCommand extends Command
	{
		public HistoryCommand() {
			super("history");
		}

		private void usage() {
			println("usage: history on [optional:file]");
			println("       history off");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			if (args.size() < 2) {
				commandError("history", "Too few arguments.");
				usage();
				return;
			}

			Boolean on = parseBoolean(args.get(1).getToken());
			if (on == null) {
				commandError("history", "Unable to parse boolean parameter.");
				return;
			}

			if (on) {
				if (args.size() == 3) {
					String filename = args.get(2).getToken();
					File file = new File(filename);
					if (!file.exists()) {
						try {
							if (!file.createNewFile()) {
								commandError("history",
										"Cannot create file '" + filename + "'.");
								return;
							}
						} catch (IOException e) {
							commandError("history", "", e.getMessage());
							return;
						}
					} else if (!file.canRead() || !file.canWrite()) {
						commandError("history", "No read/write access"
								+ " to given file '" + filename + "'.");
						return;
					}

					try {
						in.getHistory().setHistoryFile(file);
					} catch (IOException e) {
						commandError("history", "", e.getMessage());
						return;
					}
				} else if (args.size() > 3) {
					commandError("history", "Too many arguments.");
					usage();
					return;
				}

				/* only turn on history if there are no errors */
				historyEnabled.set(true);
				in.setUseHistory(true);
			} else {
				historyEnabled.set(false);
				in.setUseHistory(false);
			}
		}
	}

	private static class MigrateCommand extends Command
	{
		public MigrateCommand() {
			super("migrate");
		}

		@Override
		public void run(List<Token> args, int index)
		{
			HashSet<String> strings = new HashSet<String>();
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
			for (Module m : Core.getModules()) {
				if (m.getModuleName().equals("Newegg Parser"))
					newegg = m;
			}
			if (newegg == null) {
				commandError("migrate", "Could not find module 'Newegg Parser'.");
				return;
			}
			String[] strarr = new String[strings.size()];
			strarr = strings.toArray(strarr);
			if (Core.getDatabase() != null)
				Core.getDatabase().addProductIds(newegg, strarr);
		}
	}

	private static class ExitCommand extends Command
	{
		public ExitCommand() {
			super("exit");
		}
	}
}

class Cursor
{
	private int tokenIndex;
	private int tokenPos;

	public Cursor(int tokenIndex, int tokenPosition) {
		this.tokenIndex = tokenIndex;
		this.tokenPos = tokenPosition;
	}

	public int getTokenIndex() {
		return this.tokenIndex;
	}

	public int getTokenPosition() {
		return this.tokenPos;
	}
}

class Command
{
	private String name;
	private Command[] subcommands;
	
	public Command(String name) {
		this.name = name;
		this.subcommands = new Command[0];
	}
	
	public Command(String name, Command... subcommands) {
		this.name = name;
		this.subcommands = subcommands;
	}

	public String getName() {
		return name;
	}

	public Command[] getSubcommands() {
		return subcommands;
	}

	private void getCommandPrefix(
			StringBuilder builder, List<Token> args, int index)
	{
		if (index > 0) {
			for (int i = 0; i < index - 1; i++) {
				builder.append(args.get(i).getToken());
				builder.append(" ");
			}
			builder.append(args.get(index - 1).getToken());
			builder.append(": ");
		}
	}

	public void run(List<Token> args, int index)
	{
		String token = "";
		if (index < args.size())
			token = args.get(index).getToken();
		else {
			StringBuilder builder = new StringBuilder();
			getCommandPrefix(builder, args, index - 1);
			builder.append("Command '");
			builder.append(args.get(index - 1).getToken());
			builder.append("' not implemented.");
			Console.println(builder.toString());
			return;
		}

		for (Command subcommand : subcommands) {
			if (subcommand.getName().equals(token)) {
				subcommand.run(args, index + 1);
				return;
			}
		}

		StringBuilder builder = new StringBuilder();
		getCommandPrefix(builder, args, index);
		builder.append("Unrecognized command '");
		builder.append(args.get(index).getToken());
		builder.append("'.");
		Console.println(builder.toString());
	}
}

class Token {
	private String token;
	private int sourcePos;
	private int sourceLength;
	
	public Token (String token, int sourcePosition, int sourceLength) {
		this.token = token;
		this.sourcePos = sourcePosition;
		this.sourceLength = sourceLength;
	}
	
	public String getToken() {
		return token;
	}
	
	public int getSourcePosition() {
		return sourcePos;
	}
	
	public int getSourceLength() {
		return sourceLength;
	}
}

enum LexerState {
	NORMAL,
	SINGLE_QUOTE,
	DOUBLE_QUOTE,
	NORMAL_ESCAPE,
	QUOTE_ESCAPE
}
