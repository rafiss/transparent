package transparent.core;

import transparent.core.database.Database;
import transparent.core.database.MariaDBDriver;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.sql.SQLException;

public class Core
{
	private static final byte PRODUCT_LIST_REQUEST = 0;
	private static final byte PRODUCT_INFO_REQUEST = 1;
	private static final byte MODULE_RESPONSE = 0;
	private static final byte MODULE_HTTP_GET_REQUEST = 1;
	private static final byte MODULE_HTTP_POST_REQUEST = 2;
	
	private static final int BUFFER_SIZE = 4096;
	private static final int SLEEP_DURATION = 400;
	private static final int MAX_USHORT = 65535;
	private static final long REQUEST_PERIOD = 1000000000;
	
	private static final Sandbox sandbox = new NoSandbox();
	private static Database database;
	
	private static final Charset ASCII = Charset.forName("US-ASCII");
	private static final Charset UTF8 = Charset.forName("UTF-8");
	
	private static void downloadPage(String contentType,
			InputStream stream, DataOutputStream dest, boolean blocked)
					throws IOException
	{
		/* first pass in the content type field from the HTTP header */
		if (contentType.length() > MAX_USHORT)
			throw new IOException("Content type header field too long.");
		dest.writeShort(contentType.length());
		dest.writeBytes(contentType);
		
		/* TODO: limit the amount of data we download */
		if (blocked) {
			/* download the page in blocks, sending each to the module */
			byte[] buf = new byte[BUFFER_SIZE];
			while (true) {
				int read = stream.read(buf);
				if (read == -1) break;
				else if (read > 0) {
					dest.writeShort(read);
					dest.write(buf, 0, read);
				}
			}
			dest.writeShort(0);
		} else {
			/* download the entire page, and send the whole thing */
			ByteArrayOutputStream page = new ByteArrayOutputStream(BUFFER_SIZE);
			int read = stream.read();
			while (read != -1) {
				page.write(read);
				read = stream.read();
			}
			dest.writeInt(page.size());
			page.writeTo(dest);
		}
		dest.flush();
		stream.close();
	}
	
	private static void httpGetRequest(
			String url, DataOutputStream dest, boolean blocked, long prevRequest)
	{
		try {
			long towait = prevRequest + REQUEST_PERIOD - System.nanoTime();
			if (towait > 0) {
				try {
					Thread.sleep(towait / 1000000);
				} catch (InterruptedException e) { }
			}
			URLConnection http = new URL(url).openConnection();
			downloadPage(http.getContentType(), http.getInputStream(), dest, blocked);
		} catch (IOException e) {
			System.err.println("Core.downloadPage ERROR:"
					+ " Could not download from URL '" + url + "'.");
		}
	}
	
	private static void httpPostRequest(String url, byte[] post,
			DataOutputStream dest, boolean blocked, long prevRequest)
	{
		try {
			long towait = prevRequest + REQUEST_PERIOD - System.nanoTime();
			if (towait > 0) {
				try {
					Thread.sleep(towait / 1000);
				} catch (InterruptedException e) { }
			}

			URLConnection connection = new URL(url).openConnection();
			if (!(connection instanceof HttpURLConnection)) {
				System.err.println("Core.httpPostRequest ERROR:"
						+ " Unrecognized network protocol.");
				return;
			}
			
			HttpURLConnection http = (HttpURLConnection) connection;
			http.setDoInput(true);
			http.setDoOutput(true);
			http.setUseCaches(false);
			http.setRequestMethod("POST");
			http.connect();
			
			http.getOutputStream().write(post);
			http.getOutputStream().flush();
			http.getOutputStream().close();
			
			downloadPage(http.getContentType(), http.getInputStream(), dest, blocked);
		} catch (Exception e) {
			System.err.println("Core.httpPostRequest ERROR:"
					+ " Could not download from URL '" + url + "'.");
			e.printStackTrace();
			return;
		}
	}
	
	private static void getProductListResponse(
			Module module, DataInputStream in) throws IOException, SQLException
	{
		int count = in.readInt();
		for (int i = 0; i < count; i++) {
			int length = in.readUnsignedShort();
			byte[] data = new byte[length];
			in.readFully(data);
			database.addProductId(module, new String(data, UTF8));
		}
	}
	
	private static void getProductList(Module module)
	{
		/* TODO: the module should be time-limited */
		Process process = sandbox.run(module);
		DataOutputStream out = new DataOutputStream(process.getOutputStream());
		DataInputStream in = new DataInputStream(process.getInputStream());

		/* TODO: limit the amount of data we read */
		/* TODO: we need some kind of logging mechanism that keeps track of module errors */
		StreamPipe pipe = new StreamPipe(process.getErrorStream(), System.err);
		Thread piper = new Thread(pipe);
		piper.start();
		
		long prevRequest = System.nanoTime() - REQUEST_PERIOD;
		try {
			out.writeByte(PRODUCT_LIST_REQUEST);
			out.flush();
		
			boolean error = false;
			while (!error) {
				/* wait until input becomes available */
				boolean exited = false;
				while (in.available() == 0) {
					try {
						Thread.sleep(SLEEP_DURATION);
						process.exitValue();
						exited = true;
						break;
					} catch (InterruptedException e) {
						/* TODO: this method should be wrapped in a thread anyway */
					} catch (IllegalThreadStateException e) { }
				}
				
				if (exited)
					break;

				/* read input from the module */
				switch (in.readUnsignedByte()) {
				case MODULE_HTTP_GET_REQUEST:
					if (module.isRemote()) {
						System.err.println("Core.getProductList ERROR:"
								+ " Remote modules cannot make HTTP requests.");
						error = true;
					} else {
						int length = in.readUnsignedShort();
						byte[] data = new byte[length];
						in.readFully(data);
						String url = new String(data, ASCII);
						httpGetRequest(url, out, module.blockedDownload(), prevRequest);
						prevRequest = System.nanoTime();
					}
					break;

				case MODULE_HTTP_POST_REQUEST:
					if (module.isRemote()) {
						System.err.println("Core.getProductList ERROR:"
								+ " Remote modules cannot make HTTP requests.");
						error = true;
					} else {
						int length = in.readUnsignedShort();
						byte[] data = new byte[length];
						in.readFully(data);
						String url = new String(data, ASCII);
						
						/* read the POST data */
						length = in.readInt();
						data = new byte[length];
						in.readFully(data);
						
						httpPostRequest(url, data, out, module.blockedDownload(), prevRequest);
						prevRequest = System.nanoTime();
					}
					break;

				case MODULE_RESPONSE:
					getProductListResponse(module, in);
					break;

				default:
					System.err.println("Core.getProductList ERROR:"
							+ " Unknown module response type.");
					error = true;
				}
			}
		} catch (IOException e) {
			/* we cannot communicate with the module, so just kill it */
			System.err.println("Core.getProductList ERROR:"
					+ " Cannot communicate with module.");
		} catch (SQLException e) {
            /* we cannot communicate with the database */
            System.err.println("Core.getProductList ERROR:"
                                       + " Cannot commit data to database.");
        }

		/* destroy the process and all related threads */
		pipe.setAlive(false);
		piper.interrupt();
		try {
			piper.join();
		} catch (InterruptedException e) { }
		process.destroy();
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
                "Newegg", "NeweggParser", false, true);
		getProductList(newegg);
	}
}
