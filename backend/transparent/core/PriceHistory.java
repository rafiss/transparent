package transparent.core;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

public class PriceHistory
{
	private static final JSONParser parser =
			new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);

	private HashMap<Long, List<PriceRecord>> moduleHistory;

	public PriceHistory() {
		this.moduleHistory = new HashMap<Long, List<PriceRecord>>();
	}

	public synchronized void addRecord(long module, long time, long price) {
		List<PriceRecord> prices = moduleHistory.get(module);
		if (prices == null) {
			prices = new ArrayList<PriceRecord>();
			prices.add(new PriceRecord(time, price));
			moduleHistory.put(module, prices);
		} else
			prices.add(new PriceRecord(time, price));
	}

	public List<PriceRecord> getHistory(long module) {
		return moduleHistory.get(module);
	}

	public String save() {
		JSONObject object = new JSONObject();
		for (Entry<Long, List<PriceRecord>> entry : moduleHistory.entrySet()) {
			JSONArray array = new JSONArray();
			for (PriceRecord record : entry.getValue())
				array.add(record.save());
			object.put(Core.toUnsignedString(entry.getKey()), array);
		}
		return object.toJSONString();
	}

	public static PriceHistory load(String serialized)
	{
		if (serialized == null)
			return null;

		JSONObject object;
		try {
			object = (JSONObject) parser.parse(serialized);
		} catch (ParseException e) {
			return null;
		}

		PriceHistory history = new PriceHistory();
		for (Entry<String, Object> entry : object.entrySet()) {
			long key = new BigInteger(entry.getKey()).longValue();
			ArrayList<PriceRecord> records = new ArrayList<PriceRecord>();
			JSONArray array = (JSONArray) entry.getValue();

			for (Object obj : array)
				records.add(PriceRecord.load((String) obj));

			history.moduleHistory.put(key, records);
		}
		return history;
	}

	public static class PriceRecord
	{
		private long time;
		private long price;

		public PriceRecord(long time, long price) {
			this.time = time;
			this.price = price;
		}

		public void setTime(long time) {
			this.time = time;
		}

		public void setPrice(long price) {
			this.price = price;
		}

		public Long getTime() {
			return this.time;
		}

		public Long getPrice() {
			return this.price;
		}

		public String save() {
			return String.valueOf(time) + '.' + price;
		}

		public static PriceRecord load(String serialized) {
			String[] tokens = serialized.split("\\.");
			long time = Long.parseLong(tokens[0]);
			long price = Long.parseLong(tokens[1]);

			return new PriceRecord(time, price);
		}
	}
}

