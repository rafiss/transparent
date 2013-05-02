import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
	
	private static final String SORT_BY_PRICE = "&sort=price";
	private static final String LOW_PRICE = "&low-price=";
	private static final String PAGE_NUMBER = "&page=";

	private static final String PRODUCT_URL = "http://www.amazon.com/gp/product/";
	private static final String TECHNICAL_DETAILS_URL = "http://www.amazon.com/dp/tech-data/";

	private static final String[] CATEGORY_URLS = new String[] {
		/* Computer Cases */
		"http://www.amazon.com/s/ref=sr_nr_n_0?rh=n%3A572238",

		/* CPU Processors */
		"http://www.amazon.com/s/ref=sr_nr_n_1?rh=n%3A229189",

		/* Fans & Cooling */
		"http://www.amazon.com/s/ref=sr_nr_n_3?rh=n%3A3012290011",

		/* Graphics Cards */
		"http://www.amazon.com/s/ref=sr_nr_n_4?rh=n%3A284822",

		/* I/O Port Cards */
		"http://www.amazon.com/s/ref=sr_nr_n_4?rh=n%3A3012291011",

		/* Internal Hard Drives */
		"http://www.amazon.com/s/ref=sr_nr_n_4?rh=n%3A1254762011",

		/* Internal Optical Drives */
		"http://www.amazon.com/s/ref=sr_nr_n_8?rh=n%3A172282%2Cn%3A!493964%2Cn%3A541966%2Cn%3A193870011%2Cn%3A1292107011",

		/* Internal Sound Cards */
		"http://www.amazon.com/s/ref=sr_nr_n_9?rh=n%3A172282%2Cn%3A!493964%2Cn%3A541966%2Cn%3A193870011%2Cn%3A284823",

		/* Memory */
		"http://www.amazon.com/s/ref=sr_nr_n_11?rh=n%3A172282%2Cn%3A!493964%2Cn%3A541966%2Cn%3A193870011%2Cn%3A172500",

		/* Motherboards */
		"http://www.amazon.com/s/ref=sr_nr_n_12?rh=n%3A172282%2Cn%3A!493964%2Cn%3A541966%2Cn%3A193870011%2Cn%3A1048424",

		/* Network Cards */
		"http://www.amazon.com/s/ref=sr_nr_n_13?rh=n%3A172282%2Cn%3A!493964%2Cn%3A541966%2Cn%3A193870011%2Cn%3A13983711",

		/* Power Supplies */
		"http://www.amazon.com/s/ref=sr_nr_n_14?rh=n%3A172282%2Cn%3A!493964%2Cn%3A541966%2Cn%3A193870011%2Cn%3A1161760",

		/* Video Capture Cards */
		"http://www.amazon.com/s/ref=sr_nr_n_15?rh=n%3A172282%2Cn%3A!493964%2Cn%3A541966%2Cn%3A193870011%2Cn%3A284824",

		/* Internal Modems */
		"http://www.amazon.com/s/ref=sr_nr_n_16?rh=n%3A172282%2Cn%3A!493964%2Cn%3A541966%2Cn%3A193870011%2Cn%3A172508",

		/* Internal Solid State Drives */
		"http://www.amazon.com/s/ref=sr_nr_n_17?rh=n%3A172282%2Cn%3A!493964%2Cn%3A541966%2Cn%3A193870011%2Cn%3A1292116011",

		/* Internal Memory Card Readers */
		"http://www.amazon.com/s/ref=sr_nr_n_18?rh=n%3A172282%2Cn%3A!493964%2Cn%3A541966%2Cn%3A193870011%2Cn%3A3310626011"
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
			System.err.println("AmazonParser.httpResponse ERROR:"
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
	
	private static byte[] encodeState(int category, int page, String lowPrice)
	{
		String state = category + "|" + page + "|" + lowPrice;
		return state.getBytes(UTF8);
	}
	
	private static State decodeState(byte[] state)
	{
		if (state == null || state.length == 0)
			return null;

		try {
			String[] tokens = new String(state, UTF8).split("\\|");
			int category = Integer.parseInt(tokens[0]);
			int page = Integer.parseInt(tokens[1]);
			String lowPrice = tokens[2];
			if (lowPrice.equals("null"))
				lowPrice = null;
			return new State(category, page, lowPrice);
		} catch (Exception e) {
			System.err.println("AmazonParser.decodeState: Could not decode state. "
					+ e.getClass().getSimpleName() + " thrown. " + e.getMessage());
			return null;
		}
	}


	private static void respond(ArrayList<String> productIds,
			int category, int page, String lowPrice) throws IOException
	{
		byte[] state = encodeState(category, page, lowPrice);

		out.writeByte(MODULE_RESPONSE);
		out.writeShort(state.length);
		out.write(state);
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
	
	private static void parseCategory(int category, int startPage, String lowPrice)
	{
		String url = CATEGORY_URLS[category];

		byte[] data;
		ArrayList<String> productIds = new ArrayList<String>();
		for (int page = startPage; page <= 400; page++)
		{
			/* construct the request URL */
			String request = url + PAGE_NUMBER + page
					+ SORT_BY_PRICE;
			if (lowPrice != null)
				request += LOW_PRICE + lowPrice;

			try {
				data = httpGetRequest(request);
			} catch (IOException e) {
				System.err.println("AmazonParser.parseCategory ERROR:"
						+ " Error requesting URL '" + request + "'.");
				return;
			}

			/* get the current result position and total result count */
			Document document = Jsoup.parse(new String(data, UTF8));
			Elements elements = document.select("#resultCount");
			if (elements.size() != 1) {
				System.err.println("AmazonParser.parseCategory ERROR:"
						+ " Error parsing result count.");
				return;
			}
			
			String[] results = elements.get(0).text().split(" ");
			int currResult = Integer.parseInt(results[3].replace(",", ""));
			int resultCount = Integer.parseInt(results[5].replace(",", ""));
	
			/* parse the Amazon product IDs */
			elements = document.select(".prod");
			for (Element element : elements) {
				productIds.add(element.attr("name"));
			}
			
			/* send the product IDs to the core */
			try {
				respond(productIds, category, page, lowPrice);
			} catch (IOException e) {
				System.err.println("AmazonParser.parseCategory ERROR:"
						+ " Error responding with product ID list.");
				return;
			}
			
			/* check to see if we are done parsing this category */
			if (currResult == resultCount)
				return;
			else if (page == 400) {
				/* parse the price of the last product */
				String price;
				Elements priceElement = elements.last().select(".price");
				if (priceElement.size() == 0)
					price = elements.last().select(".newp").text();
				else price = priceElement.text();
				
				lowPrice = price.trim().replaceAll("\\$", "");
				page = 0;
			}
			productIds.clear();
		}
	}

	private static void getProductList(State state)
	{
		/* first get the list of stores from the root JSON document */
		for (int i = 0; i < CATEGORY_URLS.length; i++) {
			if (state == null)
				parseCategory(i, 1, null);
			else if (state.getCategory() == i) {
				parseCategory(i, state.getPage(), state.getLowPrice());
				state = null;
			}
		}
	}

	private static Integer parsePrice(Object price) {
		if (price == null || !price.getClass().equals(String.class))
			return null;

		try {
			String parsed = (String) price;
			if (parsed.contains("-")) {
				parsed = parsed.split("-")[0];
			}
			return Integer.parseInt(parsed.trim().replaceAll("\\$", "").replaceAll("\\.", ""));
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static void parseProductDetail(
			Map<String, String> keyValues, Element listElement)
	{
		String text = listElement.text();
		String[] tokens = text.split(":", 2);
		if (tokens.length < 2)
			return;

		String key = tokens[0].trim();
		String value = tokens[1].trim().replaceAll(
				"(View shipping rates and policies)", "").trim();
		if (key.equals("Shipping Weight"))
			key = "shipping weight";
		else if (key.equals("Item Weight"))
			key = "weight";
		else if (key.equals("Size")
				|| key.equals("Product Dimensions")
				|| key.equals("Size (LWH)"))
		{
			key = "dimensions";
			String[] subtokens = value.split(";", 2);
			value = subtokens[0].trim();
			if (subtokens.length > 1)
				keyValues.put("weight", subtokens[1].trim());
		} else if (key.equals("Item model number"))
			key = "model";
		else return;

		keyValues.put(key, value);
	}

	private static void parseTechnicalDetail(
			Map<String, String> keyValues, Element listElement)
	{
		if (listElement.select("b").size() == 0)
			return;
		String text = listElement.text();
		String[] tokens = text.split(":", 2);
		if (tokens.length < 2)
			return;

		String key = tokens[0].trim().toLowerCase();
		String value = tokens[1].trim();

		if (key.equals("memory storage capacity"))
			key = "capacity";

		keyValues.put(key, value);
	}

	private static void parseTechnicalDetails(
			Map<String, String> keyValues, String productId)
	{
		String url = TECHNICAL_DETAILS_URL + productId;

		byte[] data;
		try {
			data = httpGetRequest(url);
		} catch (IOException e) {
			System.err.println("AmazonParser.parseTechnicalDetails ERROR:"
					+ " Error requesting URL '" + url + "'.");
			return;
		}

		Document document = Jsoup.parse(new String(data, UTF8));
		Elements elements = document.select(".bucket");

		for (Element element : elements) {
			Elements subelements = element.select("h2");
			if (subelements.size() == 0)
				continue;
	
			String header = subelements.get(0).text().trim();
			if (header.equals("Product Features and Technical Details")) {
				subelements = element.select("li");
				for (Element subelement : subelements) {
					int size = keyValues.size();
					parseProductDetail(keyValues, subelement);
					if (keyValues.size() == size)
						parseTechnicalDetail(keyValues, subelement);
				}
			}
		}
	}

	private static void parseProductInfo(String productId)
	{
		String url = PRODUCT_URL + productId;

		byte[] data;
		try {
			data = httpGetRequest(url);
		} catch (IOException e) {
			System.err.println("AmazonParser.parseProductInfo ERROR:"
					+ " Error requesting URL '" + url + "'.");
			return;
		}

		HashMap<String, String> keyValues = new HashMap<String, String>();
		Document document = Jsoup.parse(new String(data, UTF8));

		/* parse the price and store the price and URL */
		Integer price = null;
		Elements elements = document.select("#olpDivId .olpCondLink");
		for (Element element : elements) {
			if (element.text().contains("new")) {
				Elements subelements = element.select(".price");
				if (subelements.size() > 0)
					price = parsePrice(subelements.get(0).text());
			}
		}
		if (price == null) {
			elements = document.select("#actualPriceValue .priceLarge");
			if (elements.size() == 0) {
				elements = document.select(".price");
				if (elements.size() > 0)
					price = parsePrice(elements.get(0).text());
			} else
				price = parsePrice(elements.get(0).text());
		}
		if (price != null)
			keyValues.put("price", price.toString());
		keyValues.put("url", url);

		/* parse the product name */
		elements = document.select("#btAsinTitle");
		if (elements.size() > 0) {
			String name = elements.get(0).text().trim();
			if (name != null)
				keyValues.put("name", name);
		}

		/* parse the image */
		elements = document.select("#main-image");
		if (elements.size() > 0) {
			String image = elements.get(0).attr("src");
			if (image != null)
				keyValues.put("image", image);
		}

		/* parse brand name */
		elements = document.select(".buying span");
		System.err.println("elements.size(): " + elements.size());
		for (Element element : elements) {
			String brand = element.text().trim();
			if (brand.startsWith("by")) {
				keyValues.put("brand", brand.substring(3).trim());
				break;
			}
		}

		/* parse product details */
		elements = document.select("td.bucket");
		for (Element element : elements) {
			Elements subelements = element.select("h2");
			if (subelements.size() == 0)
				continue;

			String header = subelements.get(0).text().trim();
			if (header.equals("Product Details")) {
				subelements = element.select("li");
				for (Element subelement : subelements)
					parseProductDetail(keyValues, subelement);
			} else if (header.equals("Technical Details")) {
				subelements = element.select("a");
				if (subelements.size() > 0 &&
						subelements.get(0).text().contains("See more technical details"))
				{
					parseTechnicalDetails(keyValues, productId);
				} else {
					subelements = element.select("li");
					for (Element subelement : subelements)
						parseTechnicalDetail(keyValues, subelement);
				}
			}
		}

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
				State previous = null;
				int length = in.readUnsignedShort();
				if (length > 0) {
					byte[] data = new byte[length];
					in.readFully(data);
					previous = decodeState(data);
				}
				getProductList(previous);
				break;
			case PRODUCT_INFO_REQUEST:
				length = in.readUnsignedShort();
				while (length > 0) {
					byte[] data = new byte[length];
					in.readFully(data);
					parseProductInfo(new String(data, UTF8));
					length = in.readUnsignedShort();
				}
				break;
			default:
			}

		} catch (IOException e) {
			System.err.println("AmazonParser.main ERROR:"
					+ " Error communicating with core.");
			return;
		}
	}
}

class State {
	private int category;
	private int page;
	private String lowPrice;

	public State(int category, int page, String lowPrice) {
		this.category = category;
		this.page = page;
		this.lowPrice = lowPrice;
	}

	public int getCategory() {
		return category;
	}

	public int getPage() {
		return page;
	}

	public String getLowPrice() {
		return lowPrice;
	}
}
