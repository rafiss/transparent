package transparent.core;

import transparent.core.database.Database;
import transparent.core.database.MariaDBDriver;
public class Core
{
	public static final byte PRODUCT_LIST_REQUEST = 0;
	public static final byte PRODUCT_INFO_REQUEST = 1;
	
	private static final Sandbox sandbox = new NoSandbox();
	private static Database database;
	
	private static void getProductList(Module module)
	{
		/* TODO: the module should be time-limited */
		ModuleThread wrapper = new ModuleThread(module, sandbox, database);
		wrapper.setRequestType(PRODUCT_LIST_REQUEST);
		Thread thread = new Thread(wrapper);
		thread.start();
		
		/* TODO: make this smarter */
		try {
			thread.join();
		} catch (InterruptedException e) { }
	}
	
	private static void getProductInfo(Module module, String productId)
	{
		/* TODO: the module should be time-limited */
		ModuleThread wrapper = new ModuleThread(module, sandbox, database);
		wrapper.setRequestType(PRODUCT_INFO_REQUEST);
		wrapper.setRequestedProductId(productId);
		Thread thread = new Thread(wrapper);
		thread.start();
		
		/* TODO: make this smarter */
		try {
			thread.join();
		} catch (InterruptedException e) { }
	}
	
	public static void main(String[] args)
	{
        try {
            database = new MariaDBDriver();
        } catch (Exception e) {
            System.err.println("Core.main ERROR: " + "Cannot connect to database: " + e.getMessage());
            System.exit(-1);
        }

		/* for now, just start the Newegg parser */
		Module newegg = new Module(
				"java -cp transparent/modules/newegg/:transparent/modules/newegg/json-smart-1.1.1.jar"
						+ ":transparent/modules/newegg/jsoup-1.7.2.jar NeweggParser",
                "Newegg", "NeweggParser", System.err, false, true);
		getProductList(newegg);
		getProductInfo(newegg, "N82E16819113280");
	}
}
