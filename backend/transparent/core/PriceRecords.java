package transparent.core;
import java.util.ArrayList;
import java.util.List;

import redis.clients.johm.Attribute;
import redis.clients.johm.CollectionList;
import redis.clients.johm.Id;
import redis.clients.johm.Model;

@Model
public class PriceRecords {
	@Id
	private Long id;

	@Attribute
	private Long module;

	@CollectionList(of = PriceRecord.class)
	private List<PriceRecord> history;

	public PriceRecords() {
		this.id = 0L;
		this.module = 0L;
		this.history = new ArrayList<PriceRecord>();
	}

	public PriceRecords(long id, long module) {
		this.id = id;
		this.module = 0L;
		this.history = new ArrayList<PriceRecord>();
	}

	public void addRecord(long time, long price) {
		history.add(new PriceRecord(Core.nextJOhmId(PriceRecord.class), time, price));
	}

	public List<PriceRecord> getHistory() {
		return history;
	}
}

