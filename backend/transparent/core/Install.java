package transparent.core;

import org.fusesource.jansi.AnsiConsole;

import transparent.core.database.Database;

/**
 * Helper class to facilitate installing the metadata
 * necessary for the system to seamlessly restart and
 * recover its previous state.
 */
public class Install
{
	private static int moduleCount = 0;
	private static final String MODULE_COUNT = "modules.count";
	
	private static boolean installModule(Database database,
			String path, String name, String source,
			boolean remote, boolean blockedDownload)
	{
		Core.println("Installing module '" + name + "'...");
		Module module = new Module(Core.random(), path,
				name, source, AnsiConsole.out, remote, blockedDownload);
		if (module.save(database, moduleCount))
		{
			moduleCount++;
			return true;
		} else {
			Core.printError("Install", "installModule",
					"Count not install module '" + name + "'.");
			return false;
		}
	}
	
	public static boolean installModules(Database database)
	{
		moduleCount = 0;
		if (installModule(database, "java -cp transparent/modules/newegg/"
				+ ":transparent/modules/newegg/json-smart-1.1.1.jar"
				+ ":transparent/modules/newegg/jsoup-1.7.2.jar NeweggParser",
				"NeweggParser", "Newegg", false, true)
		 && installModule(database, "java -cp transparent/modules/amazon/"
				+ ":transparent/modules/amazon/jsoup-1.7.2.jar AmazonParser",
				"AmazonParser", "Amazon", false, true))
		{
			return database.setMetadata(MODULE_COUNT,
					Integer.toString(moduleCount));
		} else {
			return false;
		}
	}
	
	public static boolean installJobQueue(Database database)
	{
		return (database.setMetadata("queued.count", "0")
			&& database.setMetadata("running.count", "0"));
	}
}
