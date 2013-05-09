package transparent.core;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import redis.clients.johm.Attribute;
import redis.clients.johm.CollectionList;
import redis.clients.johm.CollectionMap;
import redis.clients.johm.Id;
import redis.clients.johm.Indexed;
import redis.clients.johm.Model;
import transparent.core.PriceHistory.Record;

@Model
public class PriceHistory {
	@Id
	private Long gid;

	@Indexed
	@CollectionMap(key = Long.class, value = Prices.class)
	private HashMap<Long, Prices> moduleHistory = new HashMap<Long, Prices>();

	public PriceHistory() {
		this.gid = 0L;
	}

	public PriceHistory(long gid) {
		this.gid = gid;
	}

	public synchronized void addRecord(long module, long time, long price) {
		Prices prices = moduleHistory.get(module);
		if (prices == null) {
			prices = new Prices(Core.nextJOhmId(Prices.class));
			prices.addRecord(time, price);
			moduleHistory.put(module, prices);
		} else {
			prices.addRecord(time, price);
		}
	}

	public List<Record> getHistory(long module) {
		Prices prices = moduleHistory.get(module);
		if (prices == null)
			return null;
		return prices.getHistory();
	}
	
	@Model
	public static class Record {
		@Id
		private Long id;

		@Indexed
		@Attribute
		private Long time;

		@Attribute
		private Long price;

		public Record() {
			this.id = 0L;
			this.time = 0L;
			this.price = 0L;
		}

		public Record(long id, long time, long price) {
			this.id = id;
			this.time = time;
			this.price = price;
		}
	}
}

@Model
class Prices {
	@Id
	private Long id;

	@CollectionList(of = Record.class)
	private List<Record> history = new ArrayList<Record>();

	public Prices() {
		this.id = 0L;
	}

	public Prices(long id) {
		this.id = id;
	}

	public void addRecord(long time, long price) {
		history.add(new Record(Core.nextJOhmId(Record.class), time, price));
	}

	public List<Record> getHistory() {
		return history;
	}
}
