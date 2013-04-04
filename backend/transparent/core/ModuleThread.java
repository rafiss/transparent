package transparent.core;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

import transparent.core.database.Database;

public class ModuleThread implements Runnable, Interruptable
{
	private static final byte MODULE_RESPONSE = 0;
	private static final byte MODULE_HTTP_GET_REQUEST = 1;
	private static final byte MODULE_HTTP_POST_REQUEST = 2;

	private static final int BUFFER_SIZE = 4096;
	private static final int MAX_USHORT = 65535;
	private static final int MAX_BATCH_SIZE = 10000;
	private static final int MAX_COLUMN_COUNT = 64;
	private static final long REQUEST_PERIOD = 1000000000; /* in nanoseconds */

	private static final Charset ASCII = Charset.forName("US-ASCII");
	private static final Charset UTF8 = Charset.forName("UTF-8");
	
	private final Module module;
	private final Sandbox sandbox;
	private final Database database;
	private byte requestType;
	private boolean alive;
	private Process process;
	private String requestedProductId;
	
	public ModuleThread(Module module, Sandbox sandbox,
			Database database)
	{
		this.sandbox = sandbox;
		this.module = module;
		this.database = database;
		this.requestType = 0;
		this.alive = true;
	}
	
	private void stop() {
		this.alive = false;
	}
	
	private void downloadPage(String contentType,
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
	
	private void httpGetRequest(
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
			module.logError("ModuleThread", "httpGetRequest",
					"Could not download from URL '" + url + "'.");
		}
	}
	
	private void httpPostRequest(String url, byte[] post,
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
	
	private void getProductListResponse(
			Module module, DataInputStream in) throws IOException
	{
		int count = in.readUnsignedShort();
		if (count < 0 || count > MAX_BATCH_SIZE) {
			module.logError("ModuleThread", "getProductListResponse",
					"Invalid product ID count.");
			return;
		}

		String[] productIds = new String[count];
		for (int i = 0; i < count; i++) {
			int length = in.readUnsignedShort();
			byte[] data = new byte[length];
			in.readFully(data);
			productIds[i] = new String(data, UTF8);
		}
		if (!database.addProductIds(module, productIds)) {
			module.logError("ModuleThread", "getProductListResponse",
					"Error occurred while adding product IDs.");
		}
	}
	
	private void getProductInfoResponse(Module module,
			String productId, DataInputStream in) throws IOException
	{
		int count = in.readUnsignedShort();
		if (count < 0 || count > MAX_COLUMN_COUNT) {
			module.logError("ModuleThread", "getProductInfoResponse",
					"Too many key-value pairs.");
			return;
		}

		String[] keys = new String[count];
		String[] values = new String[count];
		for (int i = 0; i < count; i++) {
			int length = in.readUnsignedShort();
			byte[] data = new byte[length];
			in.readFully(data);
			keys[i] = new String(data, UTF8);

			length = in.readUnsignedShort();
			data = new byte[length];
			in.readFully(data);
			values[i] = new String(data, UTF8);
		}
		if (!database.addProductInfo(module, productId, keys, values)) {
			module.logError("ModuleThread", "getProductInfoResponse",
					"Error occurred while adding product information.");
		}
	}
	
	private void cleanup(Process process, StreamPipe pipe, Thread piper)
	{
		pipe.setAlive(false);
		piper.interrupt();
		try {
			piper.join();
		} catch (InterruptedException e) { }
		process.destroy();
	}
	
	public void setRequestType(byte requestType) {
		this.requestType = requestType;
	}
	
	public void setRequestedProductId(String productId) {
		this.requestedProductId = productId;
	}

	@Override
	public boolean interrupted()
	{
		try {
			process.exitValue();
			module.logInfo("ModuleThread", "interrupted",
					"Local process of module exited, interrupting thread...");
			stop();
		} catch (IllegalThreadStateException e) { }
		return !alive;
	}
	
	@Override
	public void run()
	{
		if (requestType != Core.PRODUCT_LIST_REQUEST) {
			if (requestType != Core.PRODUCT_INFO_REQUEST) {
				module.logError("ModuleThread", "run", "requestType not set.");
				return;
			} else if (requestedProductId == null) {
				module.logError("ModuleThread", "run", "requestedProductId not set.");
				return;
			}
		}
		
		/* TODO: the module should be time-limited */
		process = sandbox.run(module);
		DataOutputStream out = new DataOutputStream(process.getOutputStream());
		DataInputStream in = new DataInputStream(
				new InterruptableInputStream(process.getInputStream(), this));

		/* TODO: limit the amount of data we read */
		/* TODO: we need some kind of logging mechanism that keeps track of module errors */
		StreamPipe pipe = new StreamPipe(process.getErrorStream(), System.err);
		Thread piper = new Thread(pipe);
		piper.start();
		
		long prevRequest = System.nanoTime() - REQUEST_PERIOD;
		try {
			out.writeByte(requestType);
			if (requestType == Core.PRODUCT_INFO_REQUEST) {
				out.writeShort(requestedProductId.length());
				out.write(requestedProductId.getBytes(UTF8));
			}
			out.flush();
		
			while (alive) {
				/* read input from the module */
				switch (in.readUnsignedByte()) {
				case MODULE_HTTP_GET_REQUEST:
					if (module.isRemote()) {
						module.logError("ModuleThread", "run",
								"Remote modules cannot make HTTP requests.");
						stop();
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
						module.logError("ModuleThread", "run",
								"Remote modules cannot make HTTP requests.");
						stop();
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
					if (requestType == Core.PRODUCT_LIST_REQUEST)
						getProductListResponse(module, in);
					else if (requestType == Core.PRODUCT_INFO_REQUEST)
						getProductInfoResponse(module, requestedProductId, in);
					break;

				default:
					module.logError("ModuleThread", "run",
							"Unknown module response type.");
					stop();
				}
			}
		} catch (InterruptedStreamException e) {
			/* we have been told to die, so do so gracefully */
			module.logInfo("ModuleThread", "run",
					"Thread interrupted during IO, cleaning up module...");
		} catch (IOException e) {
			/* we cannot communicate with the module, so just kill it */
			module.logError("ModuleThread", "run",
					"Cannot communicate with module; IOException: " + e.getMessage());
		}

		/* destroy the process and all related threads */
		cleanup(process, pipe, piper);
	}
}
