package transparent.modules.newegg;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import net.minidev.json.JSONArray;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

public class NeweggParser
{
	private static final byte PRODUCT_LIST_REQUEST = 0;
	private static final byte PRODUCT_INFO_REQUEST = 1;
	private static final byte MODULE_URL_REQUEST = 0;
	private static final byte MODULE_RESPONSE = 1;

	private static final int BUFFER_SIZE = 4096;
	
	private static final Charset ASCII = Charset.forName("US-ASCII");
	
	private static final String ROOT_URL =
			"http://www.ows.newegg.com/Stores.egg/Menus";
	
	private static final JSONParser parser =
			new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);
	
	private static final DataOutputStream out =
			new DataOutputStream(System.out);
	private static final DataInputStream in =
			new DataInputStream(System.in);
	
	private static byte[] request(String url) throws IOException
	{
		out.writeByte(MODULE_URL_REQUEST);
		out.writeShort(url.length());
		out.write(url.getBytes(ASCII));
		out.flush();

		int length = in.readUnsignedShort();
		ByteArrayOutputStream response = new ByteArrayOutputStream(BUFFER_SIZE);
		while (length != 0) {
			byte[] data = new byte[length];
			in.readFully(data);
			response.write(data);
			length = in.readUnsignedShort();
		}
		
		return response.toByteArray();
	}
	
	public static void parseProductList()
	{
		byte[] root;
		try {
			root = request(ROOT_URL);
		} catch (IOException e) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Error requesting URL '" + ROOT_URL + "'.");
			return;
		}
		
		Object parsed;
		try {
			parsed = parser.parse(root);
		} catch (ParseException e) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Error parsing JSON.");
			return;
		}

		if (!(parsed instanceof JSONArray)) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Incorrect parsed object type.");
			return;
		}
		
		JSONArray array = (JSONArray) parsed;
		if (array.size() == 0) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Root array size is zero.");
			return;
		}
		
		if (!(array.get(0) instanceof JSONObject)) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Expected JSONObject.");
			return;
		}
		
		JSONObject map = (JSONObject) array.get(0);
		
	}
	
	public static void main(String[] args)
	{
		System.err.write('0');
		System.err.flush();
		try {
			/* wait for the type of request */
			switch (in.readUnsignedByte()) {
			case PRODUCT_LIST_REQUEST:
				System.err.write('1');
				System.err.flush();
				parseProductList();
				break;
			case PRODUCT_INFO_REQUEST:
				System.err.write('2');
				System.err.flush();
				//parseProductInfo(); TODO: implement this
				break;
			default:
			}
		} catch (IOException e) {
			System.err.println("NeweggParser.main ERROR:"
					+ " Error communicating with core.");
			return;
		}
	}
}
