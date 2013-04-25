import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
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
	
	private static void parseCategory(String url)
	{
		byte[] data;
		String lowPrice = null;
		ArrayList<String> productIds = new ArrayList<String>();
		for (int page = 1; page <= 400; page++)
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
				respond(productIds);
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

	private static void getProductList()
	{
		/* first get the list of stores from the root JSON document */
		for (String url : CATEGORY_URLS) {
			parseCategory(url);
		}
	}

	private static void parseProductInfo(String productId)
	{
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
