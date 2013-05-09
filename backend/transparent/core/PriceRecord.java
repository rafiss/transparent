package transparent.core;

import redis.clients.johm.Attribute;
import redis.clients.johm.Id;
import redis.clients.johm.Indexed;
import redis.clients.johm.Model;

@Model
public class PriceRecord {
	@Id
	private Long id;

	@Indexed
	@Attribute
	private Long time;

	@Attribute
	private Long price;

	public PriceRecord() {
		this.id = 0L;
		this.time = 0L;
		this.price = 0L;
	}

	public PriceRecord(long id, long time, long price) {
		this.id = id;
		this.time = time;
		this.price = price;
	}
}

