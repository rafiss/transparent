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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class AmazonParser
{
	private static final byte PRODUCT_LIST_REQUEST = 0;
	private static final byte PRODUCT_INFO_REQUEST = 1;
	private static final byte MODULE_RESPONSE = 0;
	private static final byte MODULE_HTTP_GET_REQUEST = 1;

	private static final int BUFFER_SIZE = 4096;
	private static final int DOWNLOAD_OK = 0;

	private static final Charset ASCII = Charset.forName("US-ASCII");
	private static final Charset UTF8 = Charset.forName("UTF-8");

	private static final String[] CATEGORY_URLS = new String[] {
		"http://www.amazon.com/gp/search/ref=sr_nr_n_0?rh=n%3A572238",
		"http://www.amazon.com/gp/search/ref=sr_nr_n_1?rh=n%3A229189",
		"http://www.amazon.com/gp/search/ref=sr_nr_n_3?rh=n%3A3012290011",
		"http://www.amazon.com/gp/search/ref=sr_nr_n_4?rh=n%3A284822"
	};

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
		
		int errorCode = in.readUnsignedByte();
		if (errorCode != DOWNLOAD_OK) {
			System.err.println("NeweggParser.httpResponse ERROR:"
					+ " Error occurred during download.");
			return null;
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
		out.writeShort(productIds.size());
		for (String productId : productIds) {
			byte[] data = productId.getBytes(UTF8);
			out.writeShort(data.length);
			out.write(data);
		}
	}

	private static void respond(Map<String, String> keyValues) throws IOException
	{
		out.writeByte(MODULE_RESPONSE);
		out.writeShort(keyValues.size());
		for (Entry<String, String> pair : keyValues.entrySet()) {
			byte[] key = pair.getKey().getBytes(UTF8);
			out.writeShort(key.length);
			out.write(key);
			
			byte[] value = pair.getValue().getBytes(UTF8);
			out.writeShort(value.length);
			out.write(value);
		}
	}

	private static void getProductList()
	{
		/* first get the list of stores from the root JSON document */
		byte[] data;
		for (String url : CATEGORY_URLS) {
			try {
				data = httpGetRequest(url);
			} catch (IOException e) {
				System.err.println("NeweggParser.getProductList ERROR:"
						+ " Error requesting URL '" + url + "'.");
				return;
			}
		}

		Object parsed;
		try {
			parsed = parser.parse(data);
		} catch (ParseException e) {
			System.err.println("NeweggParser.getProductList ERROR:"
					+ " Error parsing JSON.");
			return;
		}

		/* find the computer hardware store ID */
		JSONObject map = findKeyValue(ROOT_KEY, ROOT_VALUE, parsed);
		if (map == null || !map.containsKey(STORE_ID)) {
			System.err.println("NeweggParser.getProductList ERROR:"
					+ " Could not determine 'ComputerHardware' store ID.");
			return;
		}

		/* get the list of categories in that store */
		Object storeId = map.get(STORE_ID);
		String url = STORE_URL + storeId;
		try {
			data = httpGetRequest(url);
		} catch (IOException e) {
			System.err.println("NeweggParser.getProductList ERROR:"
					+ " Error requesting URL '" + url + "'.");
			return;
		}

		try {
			parsed = parser.parse(data);
		} catch (ParseException e) {
			System.err.println("NeweggParser.getProductList ERROR:"
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

	private static void parseProductInfo(String productId)
	{
		byte[] data;
		String url = PRODUCT_URL + productId + PRODUCT_URL_SUFFIX;
		
		try {
			data = httpGetRequest(url);
		} catch (IOException e) {
			System.err.println("NeweggParser.parseProductInfo ERROR:"
					+ " Error requesting URL '" + url + "'.");
			return;
		}

		Object parsed;
		try {
			parsed = parser.parse(data);
		} catch (ParseException e) {
			System.err.println("NeweggParser.parseProductInfo ERROR:"
					+ " Error parsing JSON.");
			return;
		}

		HashMap<String, String> keyValues = new HashMap<String, String>();
		findKeyValues(keyValues, parsed);

		try {
			respond(keyValues);
		} catch (IOException e) {
			System.err.println("NeweggParser.parseProductInfo ERROR:"
					+ " Error responding with product information.");
			return;
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
				int length = in.readUnsignedShort();
				byte[] data = new byte[length];
				in.readFully(data);
				parseProductInfo(new String(data, UTF8));
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
