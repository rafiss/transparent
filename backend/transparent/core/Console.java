package transparent.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.Ansi.Color;

import jline.Completor;
import jline.ConsoleReader;

public class Console
{
	private static Command root = new Command("",
			new ModulesCommand(),
			new TasksCommand(),
			new ExitCommand());

	private static ReentrantLock consoleLock = new ReentrantLock();
	private static int nestedLock = 0;

	private static final String PROMPT = "$ ";

	/* ANSI string codes for text formatting */
	private static final String BOLD = new Ansi().bold().toString();
	private static final String UNBOLD = new Ansi().boldOff().toString();

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
						tokens.add(new Token(token.toString(), start));
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
			tokens.add(new Token(token.toString(), start));
		
		return pointer;
	}

	private static void printTasks(Collection<Task> tasks, String suffix)
	{
		println(tasks.size() + suffix);
		for (Task task : tasks) {
			String module = "<null>";
			if (task.getModule() != null)
				module = task.getModule().getIdString() + " ("
						+ task.getModule().getModuleName() + ")";

			println("Task type: " + task.getType().toString()
					+ ", module: " + module
					+ ", scheduled execution: " + new Date(task.getTime())
					+ ", is running: " + Core.getRunningTasks().contains(task)
					+ ", reschedules: " + task.reschedules()
					+ ", dummy: " + task.isDummy());
		}
	}

	public static void startConsole()
	{
		try {
			ConsoleReader in = new ConsoleReader();
			in.addCompletor(new ConsoleCompleter());
			ArrayList<Token> tokens = new ArrayList<Token>();
	        while (true) {
	    		String input = in.readLine(BOLD + PROMPT + UNBOLD);
	    		
	    		lexCommand(input, input.length(), tokens);
	    		
	    		if (tokens.size() > 0 && tokens.get(0).getToken().equals("exit"))
	    			break;
	    		root.run(tokens, 0);
	        	tokens.clear();
	        }
		} catch (IOException e) {
			printError("Core", "startConsole", "", e.getMessage());
		}
	}

	private static class ConsoleCompleter implements Completor
	{
		private int complete(Command command, List<Token> tokens,
				Cursor cursor, int tokenIndex, List<String> completions)
		{
			String match = "";
			if (tokenIndex < tokens.size())
				match = tokens.get(tokenIndex).getToken();
			if (tokenIndex == cursor.getTokenIndex()) {
				match = match.substring(0, cursor.getTokenPosition());

				for (Command subcommand : command.getSubcommands()) {
					if (subcommand.getName().startsWith(match))
						completions.add(subcommand.getName());
				}

				if (tokenIndex < tokens.size())
					return tokens.get(tokenIndex).getSourcePosition();
				return 0;
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
			super("modules");
		}
		
		@Override
		public void run(List<Token> args, int index)
		{
    		println(Core.getModules().size() + " module(s).");
    		for (Module module : Core.getModules()) {
    			println("Module id: " + module.getIdString()
    					+ ", name: " + module.getModuleName()
    					+ ", source: " + module.getSourceName()
    					+ ", remote: " + module.isRemote()
    					+ ", blockedDownloading: " + module.blockedDownload()
    					+ ", logging: " + module.isLoggingActivity());
    		}
		}
	}
	
	private static class TasksCommand extends Command
	{
		public TasksCommand() {
			super("tasks",
					new QueuedTasksCommand(),
					new RunningTasksCommand());
		}
		
		@Override
		public void run(List<Token> args, int index)
		{
			if (args.size() > 1) {
				super.run(args, index);
				return;
			}
			
			ArrayList<Task> jobs = new ArrayList<Task>();
    		for (Task task : Core.getQueuedTasks())
    			jobs.add(task);
    		for (Task task : Core.getRunningTasks())
    			jobs.add(task);

    		Collections.sort(jobs);
    		printTasks(jobs, " total task(s).");
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
    		ArrayList<Task> jobs = new ArrayList<Task>();
    		for (Task task : Core.getQueuedTasks())
    			jobs.add(task);
    		
    		Collections.sort(jobs);
    		printTasks(jobs, " task(s) queued.");
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
			ArrayList<Task> jobs = new ArrayList<Task>();
    		for (Task task : Core.getRunningTasks())
    			jobs.add(task);

    		Collections.sort(jobs);
    		printTasks(jobs, " task(s) running.");
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
	private int source;
	
	public Token (String token, int sourcePosition) {
		this.token = token;
		this.source = sourcePosition;
	}
	
	public String getToken() {
		return token;
	}
	
	public int getSourcePosition() {
		return source;
	}
}

enum LexerState {
	NORMAL,
	SINGLE_QUOTE,
	DOUBLE_QUOTE,
	NORMAL_ESCAPE,
	QUOTE_ESCAPE
}
