import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
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
	private static final String ROOT_KEY = "StoreDepa";
	private static final String ROOT_VALUE = "ComputerHardware";
	private static final String STORE_ID = "StoreID";
	
	private static final String STORE_URL =
			"http://www.ows.newegg.com/Stores.egg/Categories/";
	private static final String STORE_KEY = "Description";
	private static final String CATEGORY_ID = "CategoryID";
	private static final String NODE_ID = "NodeId";

	private static final String CATEGORY_URL =
			"http://www.ows.newegg.com/Stores.egg/Navigation/";
	
	private static final HashSet<Object> STORE_DESCRIPTIONS =
			new HashSet<Object>(Arrays.asList(
					"CD \\/ DVD Burners & Media",		"Computer Cases",
					"CPUs \\/ Processors",				"Fans & Heatsinks",
					"Flash Memory & Readers",			"Hard Drives",
					"Keyboards & Mice",					"Memory",
					"Monitors",							"Motherboards",
					"Networking",						"Power Protection",
					"Power Supplies",					"Printers \\/ Scanners & Supplies",
					"Soundcards, Speakers & Headsets",	"Video Cards & Video Devices"
			));
	
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
	
	private static HashMap<Object, JSONObject> findKeyValues(
			Object key, Set<Object> values, Object json)
	{
		HashMap<Object, JSONObject> map = new HashMap<Object, JSONObject>();
		if (json instanceof JSONArray) {
			/* look for the key-value pair in this array */
			JSONArray array = (JSONArray) json;
			for (int i = 0; i < array.size(); i++)
				map.putAll(findKeyValues(key, values, array.get(i)));
			
		} else if (json instanceof JSONObject) {
			/* look for the key-value pair in this map */
			JSONObject jsonMap = (JSONObject) json;
			if (jsonMap.containsKey(key) && values.contains(jsonMap.get(key)))
				map.put(jsonMap.get(key), jsonMap);
			
			/* it could be in its children */
			for (Object child : jsonMap.values())
				map.putAll(findKeyValues(key, values, child));
		} else {
			return map;
		}
		
		return map;
	}
	
	private static JSONObject findKeyValue(Object key, Object value, Object json)
	{
		HashMap<Object, JSONObject> result =
				findKeyValues(key, Collections.singleton(value), json);
		if (result == null || result.size() == 0)
			return null;
		return result.values().iterator().next();
	}
	
	private static void parseCategory(String url)
	{
		byte[] data;
		try {
			data = request(url);
		} catch (IOException e) {
			System.err.println("NeweggParser.parseCategory ERROR:"
					+ " Error requesting URL '" + url + "'.");
			return;
		}
		
		Object parsed;
		try {
			parsed = parser.parse(data);
		} catch (ParseException e) {
			System.err.println("NeweggParser.parseCategory ERROR:"
					+ " Error parsing JSON.");
			return;
		}

		System.err.println(parsed);
		System.err.flush();
		
		System.exit(0);
	}
	
	private static void parseProductList()
	{
		byte[] data;
		try {
			data = request(ROOT_URL);
		} catch (IOException e) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Error requesting URL '" + ROOT_URL + "'.");
			return;
		}
		
		Object parsed;
		try {
			parsed = parser.parse(data);
		} catch (ParseException e) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Error parsing JSON.");
			return;
		}

		JSONObject map = findKeyValue(ROOT_KEY, ROOT_VALUE, parsed);
		if (map == null || !map.containsKey(STORE_ID)) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Could not determine 'ComputerHardware' store ID.");
			return;
		}

		String storeId = map.get(STORE_ID).toString();
		String url = STORE_URL + storeId;
		try {
			data = request(url);
		} catch (IOException e) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Error requesting URL '" + url + "'.");
			return;
		}
		
		try {
			parsed = parser.parse(data);
		} catch (ParseException e) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Error parsing JSON.");
			return;
		}
		
		HashMap<Object, JSONObject> result =
				findKeyValues(STORE_KEY, STORE_DESCRIPTIONS, parsed);
		for (JSONObject category : result.values()) {
			parseCategory(CATEGORY_URL + storeId + '/'
					+ category.get(CATEGORY_ID) + '/' + category.get(NODE_ID));
		}
	}
	
	public static void main(String[] args)
	{
		try {
			/* wait for the type of request */
			switch (in.readUnsignedByte()) {
			case PRODUCT_LIST_REQUEST:
				parseProductList();
				break;
			case PRODUCT_INFO_REQUEST:
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
