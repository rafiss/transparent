package transparent.core;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

public class PriceTrigger
{
	private static final JSONParser parser =
			new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);

	private int numTracks;
	private HashMap<Long, PriceTrack> moduleTracks;
	private TreeSet<PriceTrack> thresholdTracks;
	private HashMap<Long, TreeSet<PriceTrack>> moduleThresholdTracks;

	public PriceTrigger() {
		this.numTracks = 0;
		this.moduleTracks = new HashMap<Long, PriceTrack>();
		this.thresholdTracks = new TreeSet<PriceTrack>();
		this.moduleThresholdTracks = new HashMap<Long, TreeSet<PriceTrack>>();
	}

	public int getNumTracks() {
		return numTracks;
	}

	public HashMap<Long, PriceTrack> getModuleTracks() {
		return moduleTracks;
	}

	public TreeSet<PriceTrack> getThresholdTracks() {
		return thresholdTracks;
	}

	public HashMap<Long, TreeSet<PriceTrack>> getModuleThresholdTracks() {
		return moduleThresholdTracks;
	}

	public synchronized void incrementTracks() {
		numTracks++;
	}

	public synchronized void decrementTracks() {
		if (numTracks > 0)
			numTracks--;
	}

	private static void addTrack(TreeSet<PriceTrack> thresholdTracks, PriceTrack track) {
		PriceTrack returned = thresholdTracks.floor(track);
		if (returned == null || !returned.getPrice().equals(track.getPrice())) {
			thresholdTracks.add(track);
			track.setNumTracks(1);
		} else {
			returned.incrementTracks();
		}
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
				TreeSet<PriceTrack> set = moduleThresholdTracks.get(moduleId);
				if (set == null)
					set = new TreeSet<PriceTrack>();
				addTrack(set, track);
			}
		}
	}

	private static void removeTrack(TreeSet<PriceTrack> thresholdTracks, PriceTrack track) {
		PriceTrack returned = thresholdTracks.floor(track);
		if (returned != null && returned.getPrice().equals(track.getPrice())) {
			returned.decrementTracks();
			if (returned.getNumTracks() == 0)
				thresholdTracks.remove(returned);
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
				if (returned != null && returned.getPrice() == (long) track.getPrice()) {
					returned.decrementTracks();
					if (returned.getNumTracks() == 0)
						moduleTracks.remove(returned);
				}
			}
		} else {
			for (Long module : modules) {
				TreeSet<PriceTrack> set = moduleThresholdTracks.get(module);
				if (set != null) {
					removeTrack(set, track);
					if (set.isEmpty())
						moduleThresholdTracks.remove(module);
				}
			}
		}
	}

	private static boolean checkPrice(TreeSet<PriceTrack> thresholdTracks, long price) {
		if (thresholdTracks == null)
			return false;
		return (thresholdTracks.ceiling(new PriceTrack(price, 0)) != null);
	}

	public synchronized boolean checkPrice(Module module, long price) {
		if (numTracks > 0)
			return true;

		PriceTrack track = moduleTracks.get(module.getId());
		if (track != null && track.checkPrice(module, price))
			return true;

		if (thresholdTracks.ceiling(new PriceTrack(price, 0)) != null)
			return true;

		return checkPrice(moduleThresholdTracks.get(module.getId()), price);
	}

	public static void printInfo(Set<PriceTrack> thresholds, String prefix) {
		if (thresholds.size() > 0) {
			Console.println(Console.GRAY + prefix + "thresholds: " + Console.DEFAULT);
			for (PriceTrack track : thresholds)
				Console.println(Console.GRAY + prefix + "  price = " + track.getPrice()
						+ ", count: " + Console.DEFAULT + track.getNumTracks());
		}
	}

	public String save()
	{
		JSONArray array = new JSONArray();
		array.add(numTracks);

		JSONObject object = new JSONObject();
		for (Entry<Long, PriceTrack> entry : moduleTracks.entrySet()) {
			String key = Core.toUnsignedString(entry.getKey());
			String value = entry.getValue().save();
			object.put(key, value);
		}
		array.add(object);

		JSONArray inner = new JSONArray();
		for (PriceTrack track : thresholdTracks)
			inner.add(track.save());
		array.add(inner);

		object = new JSONObject();
		for (Entry<Long, TreeSet<PriceTrack>> entry : moduleThresholdTracks.entrySet()) {
			String key = Core.toUnsignedString(entry.getKey());
			inner = new JSONArray();
			for (PriceTrack track : entry.getValue())
				inner.add(track.save());
			object.put(key, inner);
		}
		array.add(object);

		return array.toJSONString();
	}

	public static PriceTrigger load(String serialized)
	{
		if (serialized == null)
			return null;

		JSONArray array;
		try {
			array = (JSONArray) parser.parse(serialized);
		} catch (ParseException e) {
			return null;
		}

		PriceTrigger trigger = new PriceTrigger();
		trigger.numTracks = (int) array.get(0);
		JSONObject object = (JSONObject) array.get(1);
		for (Entry<String, Object> entry : object.entrySet()) {
			long key = new BigInteger(entry.getKey()).longValue();
			PriceTrack value = PriceTrack.load((String) entry.getValue());
			trigger.moduleTracks.put(key, value);
		}

		JSONArray inner = (JSONArray) array.get(2);
		for (Object obj : inner)
			trigger.thresholdTracks.add(PriceTrack.load((String) obj));

		object = (JSONObject) array.get(3);
		for (Entry<String, Object> entry : object.entrySet()) {
			long key = new BigInteger(entry.getKey()).longValue();
			TreeSet<PriceTrack> value = new TreeSet<PriceTrack>();
			inner = (JSONArray) entry.getValue();
			for (Object obj : inner)
				value.add(PriceTrack.load((String) obj));
			trigger.moduleThresholdTracks.put(key, value);
		}

		return trigger;
	}
}

class PriceTrack implements Comparable<PriceTrack>
{
	private Long price;
	private int numTracks;

	public PriceTrack(Long price) {
		this.price = price;
		this.numTracks = 0;
	}

	public PriceTrack(Long price, int numTracks) {
		this.price = price;
		this.numTracks = numTracks;
	}

	public Long getPrice() {
		return price;
	}

	public synchronized void incrementTracks() {
		numTracks++;
	}

	public synchronized void decrementTracks() {
		if (numTracks > 0)
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

	public String save() {
		return String.valueOf(price) + '.' + numTracks;
	}

	public static PriceTrack load(String serialized) {
		String[] tokens = serialized.split("\\.");
		long price = Long.parseLong(tokens[0]);
		int numTracks = Integer.parseInt(tokens[1]);
		return new PriceTrack(price, numTracks);
	}

	@Override
	public int compareTo(PriceTrack other) {
		if (price < other.price) return -1;
		else if (price > other.price) return 1;
		else return 0;
	}

	@Override
	public boolean equals(Object other) {
		if (other == this) return true;
		else if (other == null) return false;
		else if (!other.getClass().equals(this.getClass())) return false;

		return ((PriceTrack) other).price == this.price;
	}
}
