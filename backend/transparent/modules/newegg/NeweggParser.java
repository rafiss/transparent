import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

public class NeweggParser
{
	private static final byte PRODUCT_LIST_REQUEST = 0;
	private static final byte PRODUCT_INFO_REQUEST = 1;
	private static final byte MODULE_RESPONSE = 0;
	private static final byte MODULE_HTTP_GET_REQUEST = 1;

	private static final int BUFFER_SIZE = 4096;

	private static final Charset ASCII = Charset.forName("US-ASCII");
	private static final Charset UTF8 = Charset.forName("UTF-8");

	private static final String ROOT_URL =
			"http://www.ows.newegg.com/Stores.egg/Menus";
	private static final String ROOT_KEY = "StoreDepa";
	private static final String ROOT_VALUE = "ComputerHardware";
	private static final String STORE_ID = "StoreID";

	private static final String STORE_URL =
			"http://www.ows.newegg.com/Stores.egg/Categories/";
	private static final String DESCRIPTION_KEY = "Description";
	private static final String CATEGORY_ID = "CategoryID";
	private static final String NODE_ID = "NodeId";

	private static final String CATEGORY_URL =
			"http://www.ows.newegg.com/Stores.egg/Navigation/";

	private static final String SUBCATEGORY_URL =
			"http://m.newegg.com/ProductList?";
	private static final String ITEM_NUMBER = "itemNumber=";

	private static final HashSet<Object> STORE_DESCRIPTIONS =
			new HashSet<Object>(Arrays.asList(
					"CD / DVD Burners & Media",			"Computer Accessories",
					"Computer Cases",					"CPUs / Processors",
					"Fans & Heatsinks",					"Hard Drives",
					"Keyboards & Mice",					"Memory",
					"Monitors",							"Motherboards",
					"Networking",						"Power Supplies",
					"Printers / Scanners & Supplies",	"Soundcards, Speakers & Headsets",
					"Video Cards & Video Devices"
			));

	private static final HashSet<Object> SUBCATEGORY_DESCRIPTIONS =
			new HashSet<Object>(Arrays.asList(
					/* for "CD / DVD Burners & Media" */
					"Blu-Ray Burners",					"Blu-Ray Drives",
					"CD / DVD Burners",					"CD / DVD Drives",
					"Duplicators",						"External CD / DVD / Blu-Ray Drives",
					"CD / DVD / Blu-ray Media",

					/* for "Computer Accessories" */
					"Cables",							"Adapters & Gender Changers",
					"Add-On Cards",						"Cable Management",
					"Card Readers",						"Case Accessories",
					"Controller Panels",				"CPU Accessories",
					"Mouse Pads & Accessories",			"Power Strips",
					"SSD/ HDD Accessory",

					/* for "Computer Cases" */
					"Computer Cases",					"Server Chassis",

					/* for "CPUs / Processors" */
					"Processors - Desktops",			"Processors - Servers",
					"Processors - Mobile",

					/* for "Fans & Heatsinks" */
					"Case Fans",						"CPU Fans & Heatsinks",
					"Hard Drive Cooling",				"Memory & Chipset Cooling",
					"Thermal Compound / Grease",		"VGA Cooling",
					"Water / Liquid Cooling",

					/* for "Hard Drives" */
					"Internal Hard Drives",				"SSD",
					"Laptop Hard Drives",				"Mac Hard Drives",
					"External Hard Drives",				"Controllers / RAID Cards",

					/* for "Keyboards & Mice" */
					"Keyboards",						"Mice",

					/* for "Memory" */
					"Desktop Memory",					"Flash Memory",
					"Laptop Memory",					"Mac Memory",
					"Server Memory",					"System Specific Memory",
					"USB Flash Drives",

					/* for "Monitors" */
					"LCD Monitors",						"Large Format Display",
					"Touchscreen Monitors",				"Monitor Accessories",

					/* for "Motherboards" */
					"AMD Motherboards",					"Intel Motherboards",
					"Motherboard / CPU / VGA Combo",	"Motherboard Accessories",
					"Server Motherboards",

					/* for "Networking" */
					"Wireless Networking",				"Wired Networking",
					"VoIP",								"Firewalls/Security Appliances",
					"Security & Surveillance",			"Modems",
					"Powerline Networking",

					/* for "Power Supplies" */
					"Power Supplies",					"Server Power Supplies",

					/* for "Printers / Scanners & Supplies" */
					"Laser Printers",					"Inkjet Printers",
					"Document Scanners",				"Flatbed Scanners",
					"Fax Machines & Copiers",

					/* for "Soundcards, Speakers & Headsets" */
					"Headsets & Accessories",			"Microphones",
					"Sound Cards",						"Speakers",

					/* for "Video Cards & Video Devices" */
					"Desktop Graphics Cards",			"Professional Graphics Cards"
			));

	private static final JSONParser parser =
			new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);

	private static final DataOutputStream out =
			new DataOutputStream(System.out);
	private static final DataInputStream in =
			new DataInputStream(System.in);

	private static byte[] httpResponse() throws IOException
	{
		/* ignore the HTTP content type field */
		int length = in.readUnsignedShort();
		byte[] data = new byte[length];
		in.readFully(data);

		ByteArrayOutputStream response = new ByteArrayOutputStream(BUFFER_SIZE);
		length = in.readUnsignedShort();
		while (length != 0) {
			data = new byte[length];
			in.readFully(data);
			response.write(data);
			length = in.readUnsignedShort();
		}

		return response.toByteArray();
	}

	private static byte[] httpGetRequest(String url) throws IOException
	{
		out.writeByte(MODULE_HTTP_GET_REQUEST);
		out.writeShort(url.length());
		out.write(url.getBytes(ASCII));
		out.flush();

		return httpResponse();
	}

	private static void respond(ArrayList<String> productIds) throws IOException
	{
		out.writeByte(MODULE_RESPONSE);
		out.writeInt(productIds.size());
		for (String productId : productIds) {
			byte[] data = productId.getBytes(UTF8);
			out.writeShort(data.length);
			out.write(data);
		}
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

	private static HashSet<Subcategory> parseCategory(
			Object storeId, Object categoryId, Object nodeId)
	{
		byte[] data;
		String url = CATEGORY_URL + storeId + '/'
				+ categoryId + '/' + nodeId;
		
		try {
			data = httpGetRequest(url);
		} catch (IOException e) {
			System.err.println("NeweggParser.parseCategory ERROR:"
					+ " Error requesting URL '" + url + "'.");
			return null;
		}

		Object parsed;
		try {
			parsed = parser.parse(data);
		} catch (ParseException e) {
			System.err.println("NeweggParser.parseCategory ERROR:"
					+ " Error parsing JSON.");
			return null;
		}

		HashMap<Object, JSONObject> result =
				findKeyValues(DESCRIPTION_KEY, SUBCATEGORY_DESCRIPTIONS, parsed);
		HashSet<Subcategory> subcategories = new HashSet<Subcategory>();
		for (JSONObject map : result.values()) {
			Subcategory subcategory = new Subcategory(
					storeId, categoryId, map.get(CATEGORY_ID), map.get(NODE_ID));
			subcategories.add(subcategory);
		}
		return subcategories;
	}

	private static void parseSubcategory(Subcategory subcategory)
	{
		ArrayList<String> productIds = new ArrayList<String>(20);
		for (int pageNumber = 1;; pageNumber++) {
			String url = SUBCATEGORY_URL
					+ "categoryId=" + subcategory.getSubcategoryId()
					+ "&storeId=" + subcategory.getStoreId()
					+ "&nodeId=" + subcategory.getNodeId()
					+ "&parentCategoryId=" + subcategory.getCategoryId()
					+ "&isSubCategory=true&Page=" + pageNumber;

			byte[] data;
			try {
				data = httpGetRequest(url);
			} catch (IOException e) {
				System.err.println("NeweggParser.parseSubcategory ERROR:"
						+ " Error requesting URL '" + url + "'.");
				return;
			}

			Document document = Jsoup.parse(new String(data, UTF8));
			Elements elements = document.select("a.listCell");
			if (elements.size() == 0)
				break;
			for (Element element : elements) {
				String link = element.attr("href");
				int index = link.indexOf(ITEM_NUMBER);
				productIds.add(link.substring(index + ITEM_NUMBER.length()));
			}
			
			/* send the product IDs to the core */
			try {
				respond(productIds);
			} catch (IOException e) {
				System.err.println("NeweggParser.parseSubcategory ERROR:"
						+ " Error responding with product ID list.");
				return;
			}
			productIds.clear();
		}
	}

	private static void getProductList()
	{
		/* first get the list of stores from the root JSON document */
		byte[] data;
		try {
			data = httpGetRequest(ROOT_URL);
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

		/* find the computer hardware store ID */
		JSONObject map = findKeyValue(ROOT_KEY, ROOT_VALUE, parsed);
		if (map == null || !map.containsKey(STORE_ID)) {
			System.err.println("NeweggParser.parseProductList ERROR:"
					+ " Could not determine 'ComputerHardware' store ID.");
			return;
		}

		/* get the list of categories in that store */
		Object storeId = map.get(STORE_ID);
		String url = STORE_URL + storeId;
		try {
			data = httpGetRequest(url);
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

		/* for each category, get a list of subcategories */
		HashMap<Object, JSONObject> result =
				findKeyValues(DESCRIPTION_KEY, STORE_DESCRIPTIONS, parsed);
		HashSet<Subcategory> subcategories = new HashSet<Subcategory>();
		for (JSONObject category : result.values()) {
			subcategories.addAll(parseCategory(
					storeId, category.get(CATEGORY_ID), category.get(NODE_ID)));
		}

		/* for each subcategory, get a list of products */
		for (Subcategory subcategory : subcategories) {
			parseSubcategory(subcategory);
		}
	}

	public static void main(String[] args)
	{
		try {
			/* wait for the type of request */
			switch (in.readUnsignedByte()) {
			case PRODUCT_LIST_REQUEST:
				getProductList();
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

class Subcategory {
	private Object storeId;
	private Object categoryId;
	private Object subcategoryId;
	private Object nodeId;

	public Subcategory(Object storeId, Object categoryId,
			Object subcategoryId, Object nodeId)
	{
		this.storeId = storeId;
		this.categoryId = categoryId;
		this.subcategoryId = subcategoryId;
		this.nodeId = nodeId;
	}

	public Object getStoreId() {
		return this.storeId;
	}

	public Object getCategoryId() {
		return this.categoryId;
	}

	public Object getSubcategoryId() {
		return this.subcategoryId;
	}

	public Object getNodeId() {
		return this.nodeId;
	}

	@Override
	public int hashCode() {
		return subcategoryId.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) return false;
		else if (o == this) return true;
		else if (!o.getClass().equals(this.getClass()))
			return false;

		Subcategory other = (Subcategory) o;
		return categoryId.equals(other.categoryId);
	}
}