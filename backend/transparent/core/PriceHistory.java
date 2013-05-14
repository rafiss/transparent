package transparent.core;
import java.util.HashMap;
import java.util.List;

import redis.clients.johm.Attribute;
import redis.clients.johm.CollectionList;
import redis.clients.johm.CollectionMap;
import redis.clients.johm.Id;
import redis.clients.johm.Indexed;
import redis.clients.johm.JOhm;
import redis.clients.johm.Model;

@Model
public class PriceHistory {
	@Id
	private Long gid;

	@Attribute
	private Byte dummy;

	@Indexed
	@CollectionMap(key = Long.class, value = PriceRecords.class)
	private HashMap<Long, PriceRecords> moduleHistory;

	public PriceHistory() {
		this.gid = 0L;
		this.dummy = 0;
		this.moduleHistory = new HashMap<Long, PriceRecords>();
	}

	public PriceHistory(long gid) {
		this.gid = gid;
		this.dummy = 0;
		this.moduleHistory = new HashMap<Long, PriceRecords>();
	}

	public synchronized void addRecord(long module, long time, long price) {
		PriceRecords prices = moduleHistory.get(module);
		if (prices == null) {
			prices = new PriceRecords(Core.nextJOhmId(PriceRecords.class), module);
			prices.addRecord(time, price);
			moduleHistory.put(module, prices);
		} else {
			prices.addRecord(time, price);
		}
	}

	public List<PriceRecord> getHistory(long module) {
		PriceRecords prices = moduleHistory.get(module);
		if (prices == null)
			return null;
		return prices.getHistory();
	}
}

