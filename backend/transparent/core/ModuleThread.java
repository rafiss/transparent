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
import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map.Entry;

public class ModuleThread implements Runnable, Interruptable
{
	private static final byte MODULE_RESPONSE = 0;
	private static final byte MODULE_HTTP_GET_REQUEST = 1;
	private static final byte MODULE_HTTP_POST_REQUEST = 2;
	private static final byte MODULE_SET_USER_AGENT = 3;
	
	private static final int DOWNLOAD_OK = 0;
	private static final int DOWNLOAD_ABORTED = 1;

	private static final int BUFFER_SIZE = 4096; /* in bytes */
	private static final int MAX_DOWNLOAD_SIZE = 10485760; /* 10 MB */
	private static final int MAX_USHORT = 65535;
	private static final int MAX_BATCH_SIZE = 10000;
	private static final int MAX_COLUMN_COUNT = 64;
	private static final int INPUT_SLEEP_DURATION = 200;
	private static final int ERROR_SLEEP_DURATION = 400;
	private static final long REQUEST_PERIOD = 1000000000; /* in nanoseconds */

	private static final Charset ASCII = Charset.forName("US-ASCII");
	private static final Charset UTF8 = Charset.forName("UTF-8");
	
	private static final String DEFAULT_USER_AGENT =
			"Mozilla/5.0 (X11; Linux x86_64; rv:20.0) Gecko/20100101 Firefox/20.0";
	
	private final Module module;
	private byte requestType;
	private boolean alive;
	private boolean dummy;
	private Process process;
	private Iterator<ProductID> requestedProductIds;
	private String userAgent;
	
	public ModuleThread(Module module, boolean dummy)
	{
		this.module = module;
		this.requestType = 0;
		this.alive = true;
		this.dummy = dummy;
		this.userAgent = DEFAULT_USER_AGENT;
	}
	
	public void stop() {
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
		
		int total = 0;
		if (blocked) {
			/* download the page in blocks, sending each to the module */
			byte[] buf = new byte[BUFFER_SIZE];
			while (true) {
				int read = stream.read(buf);
				if (read == -1) break;
				else if (read > 0) {
					total += read;
					if (total > MAX_DOWNLOAD_SIZE)
						break;
					dest.writeShort(read);
					dest.write(buf, 0, read);
				}
			}
			dest.writeShort(0);
		} else {
			/* download the entire page, and send the whole thing */
			ByteArrayOutputStream page = new ByteArrayOutputStream(BUFFER_SIZE);
			int read = stream.read();
			total = read;
			while (read != -1 && total < MAX_DOWNLOAD_SIZE) {
				page.write(read);
				read = stream.read();
				total += read;
			}
			dest.writeInt(page.size());
			page.writeTo(dest);
		}

		if (total < MAX_DOWNLOAD_SIZE) {
			dest.writeByte(DOWNLOAD_OK);
			module.logDownloadCompleted(total);
		} else {
			dest.writeByte(DOWNLOAD_ABORTED);
			module.logDownloadAborted();
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
			
			module.logHttpGetRequest(url);
			URLConnection http = new URL(url).openConnection();
			http.setRequestProperty("User-Agent", userAgent);
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
					Thread.sleep(towait / 1000000);
				} catch (InterruptedException e) { }
			}

			module.logHttpPostRequest(url, post);
			URLConnection connection = new URL(url).openConnection();
			if (!(connection instanceof HttpURLConnection)) {
				module.logError("ModuleThread", "httpPostRequest",
						"Unrecognized network protocol.");
				return;
			}
			
			HttpURLConnection http = (HttpURLConnection) connection;
			http.setRequestProperty("User-Agent", userAgent);
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
			module.logError("ModuleThread", "httpPostRequest",
					"Could not download from URL '" + url
					+ "'. Exception: " + e.getMessage());
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

		if (dummy) return;
		if (!Core.getDatabase().addProductIds(module, productIds)) {
			module.logError("ModuleThread", "getProductListResponse",
					"Error occurred while adding product IDs.");
		}
	}
	
	private void getProductInfoResponse(Module module,
			ProductID productId, DataInputStream in) throws IOException
	{
		int count = in.readUnsignedShort();
		if (count < 0 || count > MAX_COLUMN_COUNT) {
			module.logError("ModuleThread", "getProductInfoResponse",
					"Too many key-value pairs.");
			return;
		}

		@SuppressWarnings("unchecked")
		Entry<String, String>[] keyValues = new Entry[count];
		for (int i = 0; i < count; i++) {
			int length = in.readUnsignedShort();
			byte[] data = new byte[length];
			in.readFully(data);
			String key = new String(data, UTF8);

			length = in.readUnsignedShort();
			data = new byte[length];
			in.readFully(data);
			String value = new String(data, UTF8);
			
			keyValues[i] = new SimpleEntry<String, String>(key, value);
		}

		if (dummy) return;
		if (!Core.getDatabase().addProductInfo(module, productId, keyValues)) {
			module.logError("ModuleThread", "getProductInfoResponse",
					"Error occurred while adding product information.");
		}
	}
	
	private void cleanup(Process process, StreamPipe pipe, Thread piper)
	{
		pipe.stop();
		piper.interrupt();
		process.destroy();
		try {
			piper.join();
		} catch (InterruptedException e) { }
	}
	
	public void setRequestType(byte requestType) {
		this.requestType = requestType;
	}
	
	public void setRequestedProductIds(Iterator<ProductID> productIds) {
		this.requestedProductIds = productIds;
	}

	@Override
	public boolean interrupted()
	{
		try {
			process.exitValue();
			if (!alive)
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
			} else if (requestedProductIds == null) {
				module.logError("ModuleThread", "run", "requestedProductIds not set.");
				return;
			}
		}
		
		process = Core.getSandbox().run(module);
		DataOutputStream out = new DataOutputStream(process.getOutputStream());
		DataInputStream in = new DataInputStream(new InterruptableInputStream(
				process.getInputStream(), this, INPUT_SLEEP_DURATION));

		/* TODO: limit the amount of data we read */
		InterruptableInputStream error = new InterruptableInputStream(
				process.getErrorStream(), this, ERROR_SLEEP_DURATION);
		StreamPipe pipe = new StreamPipe(error, module.getLogStream());
		Thread piper = new Thread(pipe);
		piper.start();
		
		long prevRequest = System.nanoTime() - REQUEST_PERIOD;
		try {
			out.writeByte(requestType);
		
			while (alive)
			{
				/* indicate the product ID we are requesting */
				ProductID requestedProductId = null;
				if (requestType == Core.PRODUCT_INFO_REQUEST) {
					if (requestedProductIds.hasNext()) {
						requestedProductId = requestedProductIds.next();
						String moduleProductId = requestedProductId.getModuleProductId();
						out.writeShort(moduleProductId.length());
						out.write(moduleProductId.getBytes(UTF8));
					} else {
						out.writeShort(0);
						break;
					}
				}
				out.flush();
				
				/* read input from the module */
				switch (in.readUnsignedByte()) {
				case MODULE_SET_USER_AGENT:
					if (module.isRemote()) {
						module.logError("ModuleThread", "run",
								"Remote modules cannot make HTTP requests.");
						stop();
					} else {
						int length = in.readUnsignedShort();
						byte[] data = new byte[length];
						in.readFully(data);
						this.userAgent = new String(data, UTF8);
						module.logUserAgentChange(this.userAgent);
					}
					break;
					
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
