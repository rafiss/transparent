package transparent.core;

import transparent.core.database.Database.Relation;
import transparent.core.database.Database.Results;
import transparent.core.database.Database.ResultsIterator;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

public class ModuleThread implements Runnable, Interruptable
{
	private static final byte MODULE_RESPONSE = 0;
	private static final byte MODULE_HTTP_GET_REQUEST = 1;
	private static final byte MODULE_HTTP_POST_REQUEST = 2;
	private static final byte MODULE_SET_USER_AGENT = 3;

	private static final int TYPE_LONG = 0;
	private static final int TYPE_STRING = 1;

	private static final int DOWNLOAD_OK = 0;
	private static final int DOWNLOAD_ABORTED = 1;

	private static final int BUFFER_SIZE = 4096; /* in bytes */
	private static final int CHAR_BUFFER_SIZE = 256; /* in bytes */
	private static final int MAX_DOWNLOAD_SIZE = 10485760; /* 10 MB */
	private static final int MAX_USHORT = 65535;
	private static final int MAX_BATCH_SIZE = 10000;
	private static final int MAX_COLUMN_COUNT = 64;
	private static final int INPUT_SLEEP_DURATION = 200;
	private static final int ERROR_SLEEP_DURATION = 400;
	private static final long REQUEST_PERIOD = 1000000000; /* in nanoseconds */

	private static final Charset ASCII = Charset.forName("US-ASCII");
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private static final Charset ISO_8859_1 = Charset.forName("ISO-8859-1");

	private static final String DEFAULT_USER_AGENT =
			"Mozilla/5.0 (X11; Linux x86_64; rv:20.0) Gecko/20100101 Firefox/20.0";
	private static final String PRICE_ALERT_URL = ""; /* TODO: fill this in */
	private static final Pattern ENCODING_PATTERN =
			Pattern.compile("text/html;\\s+charset=([^\\s]+)\\s*");
	private static final String NEWLINE = System.getProperty("line.separator");

	private static final JSONParser parser =
			new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);

	private final Module module;
	private byte requestType;
	private boolean alive;
	private boolean dummy;
	private Process process;
	private ResultsIterator<ProductID> requestedProductIds;
	private String userAgent;
	private String state;

	public ModuleThread(Module module, boolean dummy)
	{
		this.module = module;
		this.requestType = 0;
		this.alive = true;
		this.dummy = dummy;
		this.userAgent = DEFAULT_USER_AGENT;
		this.state = "";
	}

	public void stop() {
		this.alive = false;
	}

	private void downloadPageChars(String contentType,
			InputStream stream, OutputStream dest, boolean blocked)
					throws IOException
	{
		/* determine the encoding of the web page */
		Charset encoding = UTF8;
		if (contentType != null) {
			Matcher match = ENCODING_PATTERN.matcher(contentType);
			if (match.matches())
				encoding = Charset.forName(match.group(1));
		}

		int total = 0;
		if (blocked) {
			/* download the page in blocks, sending each to the module */
			int start = 0;
			CountingInputStream counter = new CountingInputStream(stream);
			InputStreamReader reader = new InputStreamReader(counter, encoding);
			StringBuilder builder = new StringBuilder(2 * BUFFER_SIZE);
			char[] buf = new char[CHAR_BUFFER_SIZE];
			while (true) {
				int read = reader.read(buf, 0, buf.length);
				if (read == -1) break;
				else if (counter.bytesRead() > MAX_DOWNLOAD_SIZE)
					break;

				builder.append(buf, 0, read);
				if (counter.bytesRead() - start >= BUFFER_SIZE) {
					JSONObject map = new JSONObject();
					map.put("response", builder.toString());
					dest.write(map.toJSONString().getBytes(UTF8));
					builder = new StringBuilder(2 * BUFFER_SIZE);
				}
			}

			/* send the last message */
			JSONObject map = new JSONObject();
			map.put("response", builder.toString());
			map.put("end", "true");
			dest.write(map.toJSONString().getBytes(UTF8));
			total = counter.bytesRead();
		} else {
			/* download the entire page, and send the whole thing */
			ByteArrayOutputStream page = new ByteArrayOutputStream(4 * BUFFER_SIZE);
			byte[] buf = new byte[BUFFER_SIZE];
			int read = stream.read(buf);
			total = read;
			while (read != -1 && total < MAX_DOWNLOAD_SIZE) {
				page.write(buf, 0, read);
				read = stream.read(buf);
				total += read;
			}
			JSONObject map = new JSONObject();
			map.put("response", new String(page.toByteArray(), encoding));
			dest.write(map.toJSONString().getBytes(UTF8));
			dest.write(NEWLINE.getBytes(UTF8));
		}

		if (total < MAX_DOWNLOAD_SIZE)
			module.logDownloadCompleted(total);
		else {
			module.logDownloadAborted();
			JSONObject map = new JSONObject();
			map.put("error", "Exceeded download size limit... aborted.");
			dest.write(map.toJSONString().getBytes(UTF8));
			dest.write(NEWLINE.getBytes(UTF8));
		}

		dest.flush();
		stream.close();
	}

	private void downloadPageBytes(String contentType,
			InputStream stream, DataOutputStream dest, boolean blocked)
					throws IOException
	{
		/* first pass in the content type field from the HTTP header */
		if (contentType == null) {
			dest.writeShort(0);
		} else {
			if (contentType.length() > MAX_USHORT)
				throw new IOException("Content type header field too long.");
			dest.writeShort(contentType.length());
			dest.writeBytes(contentType);
		}

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
			ByteArrayOutputStream page = new ByteArrayOutputStream(4 * BUFFER_SIZE);
			byte[] buf = new byte[BUFFER_SIZE];
			int read = stream.read(buf);
			total = read;
			while (read != -1 && total < MAX_DOWNLOAD_SIZE) {
				page.write(buf);
				read = stream.read(buf);
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

	private void alertPriceChange(long gid, String name, long newPrice)
	{
		try {
			URLConnection connection;
				connection = new URL(PRICE_ALERT_URL).openConnection();
			if (!(connection instanceof HttpURLConnection)) {
				module.logError("ModuleThread", "alertPriceChange",
						"Unrecognized network protocol.");
				return;
			}
	
			HttpURLConnection http = (HttpURLConnection) connection;
			http.setRequestProperty("User-Agent", userAgent);
			http.setDoInput(true);
			http.setDoOutput(true);
			http.setUseCaches(false);
			http.setRequestMethod("GET");
			http.connect();
	
			JSONObject result = new JSONObject();
			result.put("gid", BigInteger.valueOf(gid));
			result.put("name", name);
			result.put("price", Core.priceToString(newPrice));
			result.put("module", BigInteger.valueOf(module.getId()));
			http.getOutputStream().write(result.toJSONString().getBytes(UTF8));
			http.getOutputStream().flush();
			http.getOutputStream().close();
		} catch (MalformedURLException e) {
			module.logError("ModuleThread", "alertPriceChange", "", e);
		} catch (IOException e) {
			module.logError("ModuleThread", "alertPriceChange", "", e);
		}
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
			switch (module.getApi()) {
			case BINARY:
				downloadPageBytes(http.getContentType(), http.getInputStream(), dest, blocked);
				break;
			case JSON:
				downloadPageChars(http.getContentType(), http.getInputStream(), dest, blocked);
				break;
			default:
				module.logError("ModuleThread", "httpGetRequest", "Unrecognized module API field.");
			}
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

			switch (module.getApi()) {
			case BINARY:
				downloadPageBytes(http.getContentType(), http.getInputStream(), dest, blocked);
				break;
			case JSON:
				downloadPageChars(http.getContentType(), http.getInputStream(), dest, blocked);
				break;
			default:
				module.logError("ModuleThread", "httpPostRequest", "Unrecognized module API field.");
			}
		} catch (Exception e) {
			module.logError("ModuleThread", "httpPostRequest",
					"Could not download from URL '" + url
					+ "'. Exception: " + e.getMessage());
			return;
		}
	}

	private void getProductListResponse(
			Module module, DataInputStream in, JSONObject json) throws IOException
	{
		String[] productIds;
		switch (module.getApi()) {
		case BINARY:
			int length = in.readUnsignedShort();
			byte[] data = new byte[length];
			in.readFully(data);
			state = new String(data, UTF8);

			int count = in.readUnsignedShort();
			if (count < 0 || count > MAX_BATCH_SIZE) {
				module.logError("ModuleThread", "getProductListResponse",
						"Invalid product ID count.");
				return;
			}

			productIds = new String[count];
			for (int i = 0; i < count; i++) {
				length = in.readUnsignedShort();
				data = new byte[length];
				in.readFully(data);
				productIds[i] = new String(data, UTF8);
			}
			break;
		case JSON:
			Object parsed = json.get("ids");
			if (!(parsed instanceof JSONArray)) {
				module.logError("ModuleThread", "getProductListResponse",
						"Expected JSON array of product IDs.");
				return;
			}

			JSONArray array = (JSONArray) parsed;
			productIds = new String[array.size()];
			for (int i = 0; i < array.size(); i++)
				productIds[i] = array.get(i).toString();

			Object newState = json.get("state");
			if (newState != null && newState instanceof String)
				state = (String) newState;
			break;
		default:
			module.logError("ModuleThread", "getProductListResponse",
					"Unrecognized module API field.");
			return;
		}

		if (dummy) return;
		if (!Core.getDatabase().addProductIds(module, productIds)) {
			module.logError("ModuleThread", "getProductListResponse",
					"Error occurred while adding product IDs.");
		}
	}

	private void getProductInfoResponse(Module module,
			ProductID productId, DataInputStream in, JSONObject json) throws IOException
	{
		Object brand = null;
		Object model = null;
		Object price = null;
		ArrayList<Entry<String, Object>> keyValues;
		switch (module.getApi()) {
		case BINARY:
			int count = in.readUnsignedShort();
			if (count < 0 || count > MAX_COLUMN_COUNT) {
				module.logError("ModuleThread", "getProductInfoResponse",
						"Too many key-value pairs.");
				return;
			}

			keyValues = new ArrayList<Entry<String, Object>>(count + 1);
			for (int i = 0; i < count; i++)
			{
				int length = in.readUnsignedShort();
				byte[] data = new byte[length];
				in.readFully(data);
				String key = new String(data, UTF8);

				Object value;
				int type = in.readUnsignedByte();
				if (type == TYPE_LONG) {
					value = in.readLong();
				} else if (type == TYPE_STRING) {
					length = in.readUnsignedShort();
					data = new byte[length];
					in.readFully(data);
					value = new String(data, UTF8);
				} else {
					module.logError("ModuleThread", "getProductInfoResponse",
							"Unrecognized value type flag.");
					return;
				}

				if (key.equals("brand")) {
					brand = value;
					keyValues.add(new SimpleEntry<String, Object>(key, value));
				} else if (key.equals("model")) {
					model = value;
					keyValues.add(new SimpleEntry<String, Object>(key, value));
				} else if (key.equals("price")) {
					price = value;
					keyValues.add(new SimpleEntry<String, Object>(key, value));
				} else if (!Core.getDatabase().isReservedKey(key))
					keyValues.add(new SimpleEntry<String, Object>(key, value));
			}
			break;
		case JSON:
			Object parsed = json.get("response");
			if (!(parsed instanceof JSONObject)) {
				module.logError("ModuleThread", "getProductInfoResponse",
						"Expected JSON map of key-value pairs.");
				return;
			}

			JSONObject map = (JSONObject) parsed;
			brand = map.get("brand");
			model = map.get("model");
			price = map.get("price");

			keyValues = new ArrayList<Entry<String, Object>>(map.size());
			if (brand != null) keyValues.add(new SimpleEntry<String, Object>("brand", brand));
			if (model != null) keyValues.add(new SimpleEntry<String, Object>("model", model));
			if (price != null) keyValues.add(new SimpleEntry<String, Object>("price", price));
			for (Entry<String, Object> entry : map.entrySet()) {
				if (!Core.getDatabase().isReservedKey(entry.getKey()))
					keyValues.add(entry);
			}
			break;
		default:
			module.logError("ModuleThread", "getProductInfoResponse",
					"Unrecognized module API field.");
			return;
		}

		if (brand == null || model == null)
			return;
		Long gid = null;
		Long oldPrice = null;
		Results results = Core.getDatabase().query(null,
				new String[] { "gid", "price" },
				new String[] { "model", "brand" },
				new Relation[] { Relation.EQUALS, Relation.EQUALS },
				new Object[] { model, brand },
				null, null, true, null, null);
		while (results != null && results.next()) {
			if (gid == null)
				gid = results.getLong(1);
			if (oldPrice == null)
				oldPrice = results.getLong(2);
			else
				oldPrice = Math.min(oldPrice, results.getLong(2));
		}
		if (gid == null)
			gid = Core.random();
		keyValues.add(new SimpleEntry<String, Object>("gid", gid));

		if (price != null) {
			long parsed = -1;
			if (price instanceof String)
				parsed = Core.parsePrice((String) price);
			else if (price instanceof Long)
				parsed = (Long) price;
			else
				throw new IllegalStateException("Unexpected type for price.");
			if ((oldPrice == null || parsed < oldPrice) && Core.checkPrice(module, gid, parsed)) {
				alertPriceChange(gid, model + " " + brand, parsed);
			}
			Core.addPriceRecord(module.getId(), gid, parsed);
		}

		if (dummy) return;
		@SuppressWarnings("unchecked")
		Entry<String, Object>[] keyValuesArray = new Entry[keyValues.size()];
		keyValuesArray = keyValues.toArray(keyValuesArray);
		if (!Core.getDatabase().addProductInfo(module, productId, keyValuesArray)) {
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

	public void setRequestedProductIds(ResultsIterator<ProductID> productIds) {
		this.requestedProductIds = productIds;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getState() {
		return this.state;
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
		InputStream underlyingStream = new InterruptableInputStream(
				process.getInputStream(), this, INPUT_SLEEP_DURATION);
		DataInputStream in = new DataInputStream(underlyingStream);

		/* TODO: limit the amount of data we read */
		InterruptableInputStream error = new InterruptableInputStream(
				process.getErrorStream(), this, ERROR_SLEEP_DURATION);
		StreamPipe pipe = new StreamPipe(error, module.getLogStream());
		Thread piper = new Thread(pipe);
		piper.start();

		int position = 0;
		boolean responded = true;
		ProductID requestedProductId = null;
		long prevRequest = System.nanoTime() - REQUEST_PERIOD;
		try {
			JSONObject map = null;
			switch (module.getApi()) {
			case BINARY:
				out.writeByte(requestType);
				if (requestType == Core.PRODUCT_LIST_REQUEST) {
					out.writeShort(state.length());
					out.write(state.getBytes(UTF8));
				}
				break;
			case JSON:
				map = new JSONObject();
				if (requestType == Core.PRODUCT_LIST_REQUEST) {
					map.put("type", "list");
					map.put("state", state);
					out.write(map.toJSONString().getBytes(UTF8));
					out.write(NEWLINE.getBytes(UTF8));
				} else {
					map.put("type", "info");
				}
			}

			if (requestType == Core.PRODUCT_INFO_REQUEST && state.length() > 0) {
				position = Integer.parseInt(state);
				requestedProductIds.seekRelative(position);
			}

			while (alive)
			{
				/* indicate the product ID we are requesting */
				if (requestType == Core.PRODUCT_INFO_REQUEST && responded) {
					if (requestedProductIds.hasNext()) {
						requestedProductId = requestedProductIds.next();
						String moduleProductId = requestedProductId.getModuleProductId();
						if (module.getApi() == Module.Api.BINARY) {
							out.writeShort(moduleProductId.length());
							out.write(moduleProductId.getBytes(UTF8));
						} else if (module.getApi() == Module.Api.JSON) {
							map.put("id", moduleProductId);
							out.write(map.toJSONString().getBytes(UTF8));
							out.write(NEWLINE.getBytes(UTF8));
						}
					} else {
						if (module.getApi() == Module.Api.BINARY)
							out.writeShort(0);
						else if (module.getApi() == Module.Api.JSON) {
							out.write(new JSONObject().toJSONString().getBytes(UTF8));
							out.write(NEWLINE.getBytes(UTF8));
						}
						break;
					}
					responded = false;
				}
				out.flush();

				/* read input from the module */
				if (module.getApi() == Module.Api.JSON) {
					Object parsed = null;
					try {
						parsed = parser.parse(in);
					} catch (ParseException e) {
						module.logError("ModuleThread", "run", "Error during JSON parsing.", e);
						stop();
						break;
					}
					if (!(parsed instanceof JSONObject)) {
						module.logError("ModuleThread", "run", "Expected JSON map.");
						stop();
						break;
					}

					JSONObject response = (JSONObject) parsed;
					String type = response.get("type").toString().trim().toLowerCase();
					if (type.equals("get") || type.equals("post")) {
						if (module.isRemote()) {
							module.logError("ModuleThread", "run",
									"Remote modules cannot make HTTP requests.");
							stop();
							break;
						}

						String url = response.get("url").toString();
						Object userAgent = response.get("user_agent");
						if (userAgent != null)
							this.userAgent = userAgent.toString();
						module.logUserAgentChange(this.userAgent);
						if (type.equals("post")) {
							String post = response.get("post").toString();
							httpPostRequest(url, post.getBytes(UTF8), out, module.blockedDownload(), prevRequest);
						} else {
							httpGetRequest(url, out, module.blockedDownload(), prevRequest);
						}
					} else if (type.equals("response")) {
						if (requestType == Core.PRODUCT_LIST_REQUEST)
							getProductListResponse(module, in, response);
						else if (requestType == Core.PRODUCT_INFO_REQUEST) {
							getProductInfoResponse(module, requestedProductId, in, response);
							state = String.valueOf(position);
							position++;
						}
						responded = true;
					}
				} else {
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
							getProductListResponse(module, in, null);
						else if (requestType == Core.PRODUCT_INFO_REQUEST) {
							getProductInfoResponse(module, requestedProductId, in, null);
							state = String.valueOf(position);
							position++;
						}
						responded = true;
						break;

					default:
						module.logError("ModuleThread", "run",
								"Unknown module response type.");
						stop();
					}
				}
			}
			String productIdString = "";
			if (requestedProductId != null)
				productIdString = ", module_product_id: " + requestedProductId.getModuleProductId();
			module.logInfo("ModuleThread", "run",
					"Module exited. (state: '" + state + "'" + productIdString + ")");
		} catch (InterruptedStreamException e) {
			/* we have been told to die, so do so gracefully */
			String productIdString = "";
			if (requestedProductId != null)
				productIdString = ", module_product_id: " + requestedProductId.getModuleProductId();
			module.logInfo("ModuleThread", "run",
					"Thread interrupted during IO, cleaning up module... (state: '"
							+ state + "'" + productIdString + ")");
		} catch (IOException e) {
			/* we cannot communicate with the module, so just kill it */
			String productIdString = "";
			if (requestedProductId != null)
				productIdString = ", module_product_id: " + requestedProductId.getModuleProductId();
			module.logError("ModuleThread", "run",
					"Cannot communicate with module; (state: '"
							+ state + "'" + productIdString + ") IOException: " + e.getMessage());
		}

		/* destroy the process and all related threads */
		cleanup(process, pipe, piper);
	}
}

class CountingInputStream extends InputStream
{
	private InputStream stream;
	private int count = 0;

	public CountingInputStream(InputStream stream) {
		this.stream = stream;
	}

	@Override
	public int read() throws IOException {
		count++;
		return stream.read();
	}

	public int bytesRead() {
		return count;
	}
}

