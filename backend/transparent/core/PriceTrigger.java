package transparent.core;

import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import redis.clients.johm.Attribute;
import redis.clients.johm.CollectionMap;
import redis.clients.johm.CollectionSortedSet;
import redis.clients.johm.Id;
import redis.clients.johm.Indexed;
import redis.clients.johm.Model;

@Model
public class PriceTrigger {
	@Id
	private Long gid;

	@Attribute
	private Integer numTracks;

	@Indexed
	@CollectionMap(key = Long.class, value = PriceTrack.class)
	private HashMap<Long, PriceTrack> moduleTracks;

	@Indexed
	@CollectionSortedSet(of = PriceTrack.class, by = "price")
	private TreeSet<PriceTrack> thresholdTracks;

	@Indexed
	@CollectionMap(key = Long.class, value = PriceTrackSet.class)
	private HashMap<Long, PriceTrackSet> moduleThresholdTracks;

	public PriceTrigger() {
		this.gid = 0L;
		this.numTracks = 0;
		this.moduleTracks = new HashMap<Long, PriceTrack>();
		this.thresholdTracks = new TreeSet<PriceTrack>();
		this.moduleThresholdTracks = new HashMap<Long, PriceTrackSet>();
	}

	public PriceTrigger(long gid) {
		this.gid = gid;
		this.numTracks = 0;
		this.moduleTracks = new HashMap<Long, PriceTrack>();
		this.thresholdTracks = new TreeSet<PriceTrack>();
		this.moduleThresholdTracks = new HashMap<Long, PriceTrackSet>();
	}

	public Long getGid() {
		return gid;
	}

	public Integer getNumTracks() {
		return numTracks;
	}

	public HashMap<Long, PriceTrack> getModuleTracks() {
		return moduleTracks;
	}

	public TreeSet<PriceTrack> getThresholdTracks() {
		return thresholdTracks;
	}

	public HashMap<Long, PriceTrackSet> getModuleThresholdTracks() {
		return moduleThresholdTracks;
	}

	public synchronized void incrementTracks() {
		numTracks++;
	}

	public synchronized void decrementTracks() {
		numTracks--;
	}

	public synchronized void addTrack(PriceTrack track) {
		if (track.getPrice() == null)
			incrementTracks();
		else {
			PriceTrack returned = thresholdTracks.floor(track);
			if (returned == null || !returned.getPrice().equals(track.getPrice())) {
				thresholdTracks.add(track);
				track.setNumTracks(1);
			} else {
				returned.incrementTracks();
			}
		}
	}

	public synchronized void addTrack(PriceTrack track, Long[] modules) {
		if (track.getPrice() == null) {
			for (Long moduleId : modules) {
				PriceTrack returned = moduleTracks.get(moduleId);
				if (returned == null) {
					moduleTracks.put(moduleId, track);
					track.setNumTracks(1);
				} else
					returned.incrementTracks();
			}
		} else {
			for (Long moduleId : modules) {
				PriceTrackSet set = moduleThresholdTracks.get(moduleId);
				if (set == null)
					set = new PriceTrackSet(Core.nextJOhmId(PriceTrackSet.class));
				set.addTrack(track);
			}
		}
	}

	public synchronized void removeTrack(PriceTrack track) {
		if (track.getPrice() == null)
			decrementTracks();
		else {
			PriceTrack returned = thresholdTracks.floor(track);
			if (returned != null && returned.getPrice().equals(track.getPrice())) {
				returned.decrementTracks();
				if (returned.getNumTracks() == 0)
					thresholdTracks.remove(returned);
			}
		}
	}

	public synchronized void removeTrack(PriceTrack track, Long[] modules) {
		if (track.getPrice() == null) {
			for (Long module : modules) {
				PriceTrack returned = moduleTracks.get(module);
				if (returned != null && returned.getPrice().equals(track.getPrice())) {
					returned.decrementTracks();
					if (returned.getNumTracks() == 0)
						moduleTracks.remove(returned);
				}
			}
		} else {
			for (Long module : modules) {
				PriceTrackSet set = moduleThresholdTracks.get(module);
				if (set != null) {
					set.removeTrack(track);
					if (set.empty())
						moduleThresholdTracks.remove(module);
				}
			}
		}
	}

	public synchronized boolean checkPrice(Module module, long price) {
		if (numTracks > 0)
			return true;

		if (moduleTracks.get(module.getId()).checkPrice(module, price))
			return true;

		if (thresholdTracks.ceiling(new PriceTrack(0, price)) != null)
			return true;

		return moduleThresholdTracks.get(module.getId()).checkPrice(price);
	}

	public static void printInfo(Set<PriceTrack> thresholds, String prefix) {
		if (thresholds.size() > 0) {
			Console.println(Console.GRAY + prefix + "thresholds: " + Console.DEFAULT);
			for (PriceTrack track : thresholds)
				Console.println(Console.GRAY + prefix + "  price = " + track.getPrice()
						+ ", count: " + Console.DEFAULT + track.getNumTracks());
		}
	}
}

@Model
class PriceTrackSet {
	@Id
	private Long id;

	@Indexed
	@CollectionSortedSet(of = PriceTrack.class, by = "price")
	private TreeSet<PriceTrack> thresholdTracks;

	public PriceTrackSet() {
		this.id = 0L;
		this.thresholdTracks = new TreeSet<PriceTrack>();
	}

	public PriceTrackSet(long id) {
		this.id = id;
		this.thresholdTracks = new TreeSet<PriceTrack>();
	}

	public TreeSet<PriceTrack> getThresholdTracks() {
		return thresholdTracks;
	}

	public void addTrack(PriceTrack track) {
		PriceTrack returned = thresholdTracks.floor(track);
		if (returned == null || !returned.getPrice().equals(track.getPrice())) {
			thresholdTracks.add(track);
			track.setNumTracks(1);
		} else {
			returned.incrementTracks();
		}
	}

	public void removeTrack(PriceTrack track) {
		PriceTrack returned = thresholdTracks.floor(track);
		if (returned != null && returned.getPrice().equals(track.getPrice())) {
			returned.decrementTracks();
			if (returned.getNumTracks() == 0)
				thresholdTracks.remove(returned);
		}
	}

	public boolean empty() {
		return thresholdTracks.isEmpty();
	}

	public boolean checkPrice(long price) {
		return (thresholdTracks.ceiling(new PriceTrack(0, price)) != null);
	}
}

@Model
class PriceTrack {
	@Id
	private Long id;

	@Attribute
	private Long price;

	@Attribute
	private Integer numTracks;

	public Long getPrice() {
		return price;
	}

	public PriceTrack() {
		this.id = 0L;
		this.price = 0L;
		this.numTracks = 0;
	}

	public PriceTrack(long id, Long price) {
		this.id = id;
		this.price = price;
		this.numTracks = 0;
	}

	public synchronized void incrementTracks() {
		numTracks++;
	}

	public synchronized void decrementTracks() {
		numTracks--;
	}

	public Integer getNumTracks() {
		return numTracks;
	}

	public void setNumTracks(int numTracks) {
		this.numTracks = numTracks;
	}

	public synchronized boolean checkPrice(Module module, long price) {
		return (numTracks > 0);
	}
}
