package transparent.core;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import transparent.core.Module;

public class Core
{
	private static final byte PRODUCT_LIST_REQUEST = 0;
	private static final byte PRODUCT_INFO_REQUEST = 1;
	private static final byte MODULE_RESPONSE = 0;
	private static final byte MODULE_HTTP_GET_REQUEST = 1;
	private static final byte MODULE_HTTP_POST_REQUEST = 2;
	
	private static final int BUFFER_SIZE = 4096;
	
	private static final Sandbox sandbox = new NoSandbox();
	
	private static final Charset ASCII = Charset.forName("US-ASCII");
	
	private static void downloadPage(
			InputStream stream, DataOutputStream dest, boolean blocked)
					throws IOException
	{
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
			String url, DataOutputStream dest, boolean blocked)
	{
		try {
			downloadPage(new URL(url).openStream(), dest, blocked);
		} catch (IOException e) {
			System.err.println("Core.downloadPage ERROR:"
					+ " Could not download from URL '" + url + "'.");
			return;
		}
	}
	
	private static void httpPostRequest(
			String url, byte[] post, DataOutputStream dest, boolean blocked)
	{
		try {
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
			
			downloadPage(http.getInputStream(), dest, blocked);
		} catch (Exception e) {
			System.err.println("Core.httpPostRequest ERROR:"
					+ " Could not download from URL '" + url + "'.");
			e.printStackTrace();
			return;
		}
	}
	
	public static void getProductList(Module module)
	{
		/* TODO: the module should be time-limited */
		Process process = sandbox.run(module);
		DataOutputStream out = new DataOutputStream(process.getOutputStream());
		DataInputStream in = new DataInputStream(process.getInputStream());
		
		try {
			out.writeByte(PRODUCT_LIST_REQUEST);
			out.flush();
		
			boolean error = false;
			while (!error) {
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
						httpGetRequest(url, out, module.blockedDownload());
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
						
						httpPostRequest(url, data, out, module.blockedDownload());
					}
					break;

				case MODULE_RESPONSE:
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
		}

		/* get standard error (TODO: limit the amount of data we read) */
		/* TODO: we need some kind of logging mechanism that keeps track of module errors */
		try {
			if (process.getErrorStream().available() > 0) {
				System.err.println("Core.getProductList: Module printed to standard error.");
				while (process.getErrorStream().available() > 0)
					System.err.write(process.getErrorStream().read());
			}
		} catch (IOException e) { }
		process.destroy();
	}
	
	public static void main(String[] args)
	{		
		/* for now, just start the Newegg parser */
		Module newegg = new Module(
				"java -cp transparent/modules/newegg/:transparent/modules/newegg/json-smart-1.1.1.jar"
						+ " NeweggParser", false, true);
		getProductList(newegg);
	}
}

