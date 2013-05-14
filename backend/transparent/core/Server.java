package transparent.core;

import transparent.core.PriceHistory.PriceRecord;
import transparent.core.database.Database.Relation;
import transparent.core.database.Database.Results;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;

public class Server implements Container
{
	private static final JSONParser parser =
			new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);

	private static class QueryProcessor implements Runnable
	{
		private final Request request;
		private final Response response;

		public QueryProcessor(Request request, Response response) {
			this.request = request;
			this.response = response;
		}

		public JSONObject error(String message) {
			JSONObject map = new JSONObject();
			map.put("error", message);
			return map;
		}

		private Long parseJsonLong(Object json) {
			if (json instanceof String) {
				return new BigInteger((String) json).longValue();
			} else if (json instanceof BigInteger) {
				return ((BigInteger) json).longValue();
			} else if (json instanceof Long) {
				return (long) json;
			} else if (json instanceof Integer) {
				return Long.valueOf((int) json);
			} else {
				return null;
			}
		}

		private void parseModules(PrintStream body) throws IOException, ParseException
		{
			String content = request.getContent();
			if (content.isEmpty()) {
				JSONObject result = new JSONObject();
				for (Module module : Core.getModules()) {
					JSONObject subresult = moduleInfo(module);
					result.put(module.getIdString(), subresult);
				}
				body.println(result.toJSONString());
				return;
			}

			Object object = parser.parse(content);
			if (!(object instanceof JSONObject)) {
				body.println(error("Root structure must be a map."));
				body.close();
				return;
			}

			JSONObject map = (JSONObject) object;
			Object idObject = map.get("id");
			if (idObject instanceof JSONArray) {
				JSONArray array = (JSONArray) idObject;
				String[] modules = new String[array.size()];
				for (int i = 0; i < array.size(); i++) {
					if (!(array.get(i) instanceof String)) {
						body.println(error("'id' key must map to a string "
								+ "or a list of strings."));
						body.close();
						return;
					}
					modules[i] = (String) array.get(i);
				}

				JSONObject result = new JSONObject();
				for (int i = 0; i < modules.length; i++) {
					JSONObject subresult = moduleInfo(modules[i]);
					if (subresult == null)
						result.put(modules[i], error("No such module with id."));
					else
						result.put(modules[i], subresult);
				}
				body.println(result.toJSONString());
			} else if (idObject instanceof String) {
				JSONObject subresult = moduleInfo((String) idObject);
				if (subresult == null)
					body.println(error("No such module with id."));
				else
					body.println(subresult.toJSONString());
			} else {
				body.println(error("'id' key must map to a string "
						+ "or a list of strings."));
				body.close();
				return;
			}
		}
		
		private Long[] parseModules(Object modulesObject) {
			if (modulesObject != null) {
				if (modulesObject instanceof JSONArray) {
					JSONArray modulesArray = (JSONArray) modulesObject;
					Long[] modules = new Long[modulesArray.size()];
					for (int i = 0; i < modulesArray.size(); i++) {
						modules[i] = parseJsonLong(modulesArray.get(i));
						if (modules[i] == null)
							return null;
					}
					return modules;
				} else {
					Long[] modules = new Long[1];
					modules[0] = parseJsonLong(modulesObject);
					if (modules[0] == null)
						return null;
					return modules;
				}
			} else {
				return null;
			}
		}

		private void parseProductQuery(PrintStream body) throws IOException, ParseException
		{
			Object object = parser.parse(request.getContent());
			if (!(object instanceof JSONObject)) {
				body.println(error("Root structure must be a map."));
				body.close();
				return;
			}

			JSONObject map = (JSONObject) object;
			Long gid = parseJsonLong(map.get("gid"));
			if (gid == null) {
				body.println(error("Unable to parse 'gid' key."));
				body.close();
				return;
			}

			Object modulesObject = map.get("modules");
			Long[] modules = null;
			if (modulesObject != null) {
				modules = parseModules(modulesObject);
				if (modules == null) {
					body.println(error("Unable to parse 'modules' key."));
					body.close();
					return;
				}
			}

			String brand = null;
			String model = null;
			String image = null;
			String name = null;
			String url = null;
			Long module = null;
			Long price = null;
			Results results;
			if (modules == null) {
				results = Core.getDatabase().query(
						null, null,
						new String[] { "gid" },
						new Relation[] { Relation.EQUALS },
						new Object[] { gid },
						null, null, true, null, null);
			} else {
				results = Core.getDatabase().query(
						null, null,
						new String[] { "gid", "module_id" },
						new Relation[] { Relation.EQUALS, Relation.EQUALS },
						new Object[] { gid, modules },
						null, null, true, null, null);
			}
			JSONObject rows = new JSONObject();
			HashMap<Long, Long> prices = new HashMap<Long, Long>();
			while (results.next()) {
				long module_id = results.getLong(2);
				String module_product_id = results.getString(3);
				String module_product_name = results.getString(5);
				if (name == null)
					name = module_product_name;

				JSONObject json = (JSONObject) parser.parse(results.getString(6));
				Object priceObject = json.get("price");
				if (priceObject != null) {
					/* check that we are picking the lowest price from repeated results */
					Long oldPrice = prices.get(module_id);
					long priceValue = ((Number) priceObject).longValue();
					if (oldPrice != null && oldPrice <= priceValue)
						continue;
					prices.put(module_id, priceValue);
					module = module_id;
					Object urlObject = json.get("url");
					if (urlObject != null && urlObject instanceof String)
						url = (String) urlObject;
					else
						url = null;

					json.put("price", Core.priceToString(priceValue));
					if (price == null || priceValue < price)
						price = priceValue;
				}
				if (brand == null)
					brand = (String) json.get("brand");
				if (model == null)
					model = (String) json.get("model");
				if (image == null)
					image = (String) json.get("image");

				JSONObject row = new JSONObject();
				row.putAll(json);
				row.put("module", new BigInteger(Core.toUnsignedString(module_id)));
				row.put("module_product_id", module_product_id);
				row.put("name", module_product_name);
				rows.put(Core.toUnsignedString(module_id), row);
			}

			if (brand != null && model != null)
				rows.put("name", brand + " " + model);
			else if (name != null)
				rows.put("name", name);

			if (module != null)
				rows.put("module", new BigInteger(Core.toUnsignedString(module)));
			if (url != null)
				rows.put("url", url);
			if (image != null)
				rows.put("image", image);
			if (price != null)
				rows.put("price", Core.priceToString(price));

			body.println(rows.toJSONString());
		}
		
		private void parseSearch(PrintStream body) throws IOException, ParseException
		{
			Object object = parser.parse(request.getContent());
			if (!(object instanceof JSONObject)) {
				body.println(error("Root structure must be a map."));
				body.close();
				return;
			}

			JSONObject map = (JSONObject) object;
			Object selectObject = map.get("select");
			if (!(selectObject instanceof JSONArray)) {
				body.println(error("'select' key must map to a list of strings."));
				body.close();
				return;
			}

			JSONArray selectList = (JSONArray) selectObject;
			String[] select = new String[selectList.size()];
			for (int i = 0; i < selectList.size(); i++) {
				if (!(selectList.get(i) instanceof String)) {
					body.println(error("'select' key must map to a list of strings."));
					body.close();
					return;
				}

				select[i] = (String) selectList.get(i);
			}

			Object whereObject = map.get("where");
			String[] whereClause = null;
			Relation[] whereRelation = null;
			Object[] whereArgs = null;
			if (whereObject != null) {
				if (!(whereObject instanceof JSONObject)) {
					body.println(error("'where' key must map to a map of string pairs."));
					body.close();
					return;
				}

				int i = 0;
				JSONObject whereMap = (JSONObject) whereObject;
				whereClause = new String[whereMap.size()];
				whereRelation = new Relation[whereMap.size()];
				whereArgs = new Object[whereMap.size()];
				for (Entry<String, Object> pair : whereMap.entrySet()) {
					String key = pair.getKey();
					if (!(pair.getValue() instanceof String)) {
						body.println(error("Key in the 'where' entry must be string."));
						body.close();
						return;
					}

					String value = (String) pair.getValue();
					if (value.length() == 0) {
						body.println(error("Value in the 'where' entry"
								+ " must have non-zero length."));
						body.close();
						return;
					}
					Relation relation = Relation.parse(value.charAt(0));
					if (relation == null) {
						body.println(error("Unable to parse relation operator"));
						body.close();
						return;
					}

					whereClause[i] = key;
					whereRelation[i] = relation;
					whereArgs[i] = value.substring(1);
				}
			}

			String name = null;
			Object nameObject = map.get("name");
			if (nameObject != null) {
				if (!(nameObject instanceof String)) {
					body.println(error("'name' key must map to a string."));
					body.close();
					return;
				}

				name = (String) nameObject;
			}

			String sort = null;
			Object sortObject = map.get("sort");
			if (sortObject != null) {
				if (!(sortObject instanceof String)) {
					body.println(error("'sort' key must map to a string."));
					body.close();
					return;
				}

				sort = (String) sortObject;
			}

			Boolean ascending = true;
			Object ascendingObject = map.get("ascending");
			if (ascendingObject != null) {
				if (ascendingObject instanceof String) {
					ascending = ((String) ascendingObject)
							.toLowerCase().trim().equals("true");
				} else if (ascendingObject instanceof Integer) {
					ascending = !ascendingObject.equals(0);
				}
			}

			Integer page = 1;
			Object pageObject = map.get("page");
			if (pageObject != null) {
				if (pageObject instanceof Number) {
					page = ((Number) pageObject).intValue();
				} else if (pageObject instanceof String) {
					page = Integer.parseInt((String) pageObject);
				} else {
					body.println(error("'page' key must map to an integer or string."));
					body.close();
					return;
				}
			}

			Integer limit = 15;
			Object limitObject = map.get("pagesize");
			if (limitObject != null) {
				if (limitObject instanceof Number) {
					limit = ((Number) limitObject).intValue();
				} else if (limitObject instanceof String) {
					limit = Integer.parseInt((String) limitObject);
				} else {
					body.println(error("'pagesize' key must map to an integer or string."));
					body.close();
					return;
				}
			}

			Map<Long, JSONArray> returned = query(name, select,
					whereClause, whereRelation, whereArgs,
					sort, ascending, page, limit);
			if (returned != null) {
				JSONArray results = new JSONArray();
				results.addAll(returned.values());
				body.println(results.toJSONString());
			} else
				body.println(error("Internal error occurred during query."));

		}

		private void parseSubscribe(PrintStream body) throws ParseException, IOException
		{
			Object object = parser.parse(request.getContent());
			if (!(object instanceof JSONObject)) {
				body.println(error("Root structure must be a map."));
				body.close();
				return;
			}

			JSONObject map = (JSONObject) object;
			Long gid = parseJsonLong(map.get("gid"));
			if (gid == null) {
				body.println(error("Unable to parse 'gid'."));
				body.close();
				return;
			}

			Long price = null;
			Object priceObject = map.get("price");
			if (priceObject != null) {
				if (priceObject instanceof String)
					price = Core.parsePrice((String) priceObject);
				else if (priceObject instanceof Number)
					price = ((Number) priceObject).longValue();
				else {
					body.println(error("Unable to parse 'price'."));
					body.close();
					return;
				}
			}

			Long[] modules = null;
			Object modulesObject = map.get("modules");
			if (modulesObject != null) {
				modules = parseModules(modulesObject);
				if (modules == null) {
					body.println(error("Unable to parse 'modules' key."));
					body.close();
					return;
				}
			}

			Core.addPriceTrack(gid, modules, price);
			JSONObject result = new JSONObject();
			result.put("success", "true");
			body.println(result.toJSONString());
		}

		private void parseUnsubscribe(PrintStream body) throws ParseException, IOException
		{
			Object object = parser.parse(request.getContent());
			if (!(object instanceof JSONObject)) {
				body.println(error("Root structure must be a map."));
				body.close();
				return;
			}

			JSONObject map = (JSONObject) object;
			Long gid = parseJsonLong(map.get("gid"));
			if (gid == null) {
				body.println(error("Unable to parse 'gid'."));
				body.close();
				return;
			}

			Long price = null;
			Object priceObject = map.get("price");
			if (priceObject != null) {
				if (priceObject instanceof String)
					price = Core.parsePrice((String) priceObject);
				else if (priceObject instanceof Number)
					price = ((Number) priceObject).longValue();
				else {
					body.println(error("Unable to parse 'price'."));
					body.close();
					return;
				}
			}

			Long[] modules = null;
			Object modulesObject = map.get("modules");
			if (modulesObject != null) {
				modules = parseModules(modulesObject);
				if (modules == null) {
					body.println(error("Unable to parse 'modules' key."));
					body.close();
					return;
				}
			}

			Core.removePriceTrack(gid, modules, price);
			JSONObject result = new JSONObject();
			result.put("success", "true");
			body.println(result.toJSONString());
		}

		private void parseHistory(PrintStream body) throws ParseException, IOException
		{
			Object object = parser.parse(request.getContent());
			if (!(object instanceof JSONObject)) {
				body.println(error("Root structure must be a map."));
				body.close();
				return;
			}

			JSONObject map = (JSONObject) object;
			Long gid = parseJsonLong(map.get("gid"));
			if (gid == null) {
				body.println(error("Unable to parse 'gid'."));
				body.close();
				return;
			}

			Long[] modules = null;
			Object modulesObject = map.get("modules");
			if (modulesObject != null) {
				modules = parseModules(modulesObject);
				if (modules == null) {
					body.println(error("Unable to parse 'modules' key."));
					body.close();
					return;
				}
			}

			JSONArray result = new JSONArray();
			if (modules == null) {
				for (Module module : Core.getModules()) {
					JSONObject row = new JSONObject();
					row.put("name", module.getSourceName()); /* TODO: do something smarter with overlapping source names */
					row.put("data", serializeHistory(Core.getPriceHistory(module.getId(), gid)));
					result.add(row);
				}
			} else {
				for (Long moduleId : modules) {
					JSONObject row = new JSONObject();
					row.put("name", Core.getModule(moduleId).getSourceName());
					row.put("data", serializeHistory(Core.getPriceHistory(moduleId, gid)));
					result.add(row);
				}
			}

			body.println(result.toJSONString());
		}

		@Override
		public void run() {
			PrintStream body = null;
			try {
				body = response.getPrintStream();
				response.setValue("Content-Type", "text/plain");
				String url = request.getPath().getPath();
				if (url.equals("/search") || url.equals("/search/"))
					parseSearch(body);
				else if (url.equals("/product") || url.equals("/product/"))
					parseProductQuery(body);
				else if (url.equals("/modules") || url.equals("/modules/"))
					parseModules(body);
				else if (url.equals("/subscribe") || url.equals("/subscribe/"))
					parseSubscribe(body);
				else if (url.equals("/unsubscribe") || url.equals("/unsubscribe/"))
					parseUnsubscribe(body);
				else if (url.equals("/history") || url.equals("/history/"))
					parseHistory(body);
				else
					body.println(error("Page not found."));
				body.close();
			} catch (Exception e) {
				Console.printError("Server.QueryProcessor", "run", "", e);
				if (body != null) {
					StringWriter message = new StringWriter();
					message.write(
							e.getClass().getSimpleName() + " thrown.");
					if (e.getMessage() != null)
						message.write(" " + e.getMessage());
					PrintWriter writer = new PrintWriter(message);
					e.printStackTrace(writer);
					writer.flush();
					body.println(error(message.toString()));
					body.close();
					writer.close();
				}
			}
		}
	}

	private static JSONObject serializeHistory(List<PriceRecord> history) {
		JSONObject map = new JSONObject();
		if (history == null)
			return map;
		SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
		for (PriceRecord record : history) {
			String key = sd.format(new Date(record.getTime()));
			double value = record.getPrice() / 100.0;
			map.put(key, value);
		}
		return map;
	}

	private static JSONObject moduleInfo(Module module)
	{
		if (module == null)
			return null;

		JSONObject map = new JSONObject();
		map.put("name", module.getModuleName());
		map.put("source", module.getSourceName());
		map.put("url", module.getModuleUrl());
		map.put("sourceurl", module.getSourceUrl());
		map.put("api", module.getApi().save());
		return map;
	}

	private static JSONObject moduleInfo(String moduleId)
	{
		if (moduleId == null)
			return null;

		try {
			return moduleInfo(Core.getModule(new BigInteger(moduleId).longValue()));
		} catch (NumberFormatException e) {
			return null;
		}
	}
	
	private static void mergeRows(JSONArray mergeInto, JSONArray from)
	{
		for (int i = 0; i < mergeInto.size(); i++) {
			if (mergeInto.get(i) == null)
				mergeInto.set(i, from.get(i));
		}
	}

	private static Map<Long, JSONArray> query(String name, String[] select,
			String[] whereClause, Relation[] whereRelation, Object[] whereArgs,
			String sort, boolean ascending, Integer page, Integer pageSize)
	{
		/* construct the select array */
		int selectCount = select.length;
		LinkedHashMap<String, Integer> selectIndices = new LinkedHashMap<String, Integer>();
		for (int i = 0; i < select.length; i++)
			selectIndices.put(select[i], i);

		Integer gidIndex = selectIndices.get("gid");
		Integer priceIndex = selectIndices.get("price");
		Integer moduleIndex = selectIndices.get("module_id");
		Integer nameIndex = selectIndices.get("name");
		Integer brandIndex = selectIndices.get("brand");
		Integer modelIndex = selectIndices.get("model");
		if (gidIndex == null) {
			gidIndex = selectIndices.size();
			selectIndices.put("gid", selectIndices.size());
		}
		if (nameIndex != null) {
			if (brandIndex == null) {
				brandIndex = selectIndices.size();
				selectIndices.put("brand", selectIndices.size());
			}
			if (modelIndex == null) {
				modelIndex = selectIndices.size();
				selectIndices.put("model", selectIndices.size());
			}
		}
		if (priceIndex != null && moduleIndex == null) {
			moduleIndex = selectIndices.size();
			selectIndices.put("module_id", selectIndices.size());
		}

		HashMap<Long, JSONArray> json = new HashMap<Long, JSONArray>();

		Results dbresults = Core.getDatabase().query(
				name, new String[] { "gid" },
				whereClause, whereRelation, whereArgs,
				"gid", sort, ascending, (page - 1) * pageSize, pageSize);

		ArrayList<Long> gid_ids = new ArrayList<Long>();
		while (dbresults.next())
			gid_ids.add(dbresults.getLong(1));
		Long[] gidArg = new Long[gid_ids.size()];
		gidArg = gid_ids.toArray(gidArg);
		if (gid_ids.size() == 0)
			return json;

		String[] newSelect = new String[selectIndices.size()];
		newSelect = selectIndices.keySet().toArray(newSelect);
		dbresults = Core.getDatabase().query(
				null, newSelect,
				new String[] { "gid" },
				new Relation[] { Relation.EQUALS },
				new Object[] { gidArg },
				null, sort, ascending, null, null);

		HashMap<Entry<Long, Long>, Long> prices =
				new HashMap<Entry<Long, Long>, Long>();
		HashMap<Long, Entry<Long, Long>> priceRanges =
				new HashMap<Long, Entry<Long, Long>>();
		while (dbresults.next()) {
			JSONArray row = new JSONArray();
			row.ensureCapacity(select.length);
			for (int i = 0; i < selectCount; i++)
				row.add(dbresults.get(i + 1));
			Long gid = dbresults.getLong(gidIndex + 1);
			row.set(gidIndex, new BigInteger(Core.toUnsignedString(gid)));

			Long module = null;
			if (moduleIndex != null) {
				module = dbresults.getLong(moduleIndex + 1);
				if (moduleIndex < row.size())
					row.set(moduleIndex, new BigInteger(Core.toUnsignedString(module)));
			}

			if (priceIndex != null) {
				/* check that we are picking the lowest price from repeated results */
				Entry<Long, Long> moduleGid = new SimpleEntry<Long, Long>(module, gid);
				Long oldPrice = prices.get(moduleGid);
				Long price = dbresults.getLong(priceIndex + 1);
				if (oldPrice != null && oldPrice <= price)
					continue;
				prices.put(moduleGid, price);

				row.set(priceIndex, Core.priceToString(price));
				Entry<Long, Long> range = priceRanges.get(gid);
				if (range == null)
					range = new SimpleEntry<Long, Long>(price, price);
				else {
					if (price < range.getKey())
						range = new SimpleEntry<Long, Long>(price, range.getValue());
					if (price > range.getValue())
						range = new SimpleEntry<Long, Long>(range.getKey(), price);
				}
				priceRanges.put(gid, range);
			}

			if (nameIndex != null) {
				String brand = dbresults.getString(brandIndex + 1);
				String model = dbresults.getString(modelIndex + 1);
				if (brand != null && model != null)
					row.set(nameIndex, brand + " " + model);
			}

			if (json.containsKey(gid))
				mergeRows(json.get(gid), row);
			else
				json.put(gid, row);
		}

		if (priceIndex != null) {
			for (Entry<Long, JSONArray> entry : json.entrySet())
			{
				Long gid = entry.getKey();
				JSONArray row = entry.getValue();
				Entry<Long, Long> range = priceRanges.get(gid);
				Long lowPrice = range.getKey();
				Long highPrice = range.getValue();
				if (lowPrice != highPrice) {
					row.set(priceIndex, Core.priceToString(range.getKey())
							+ " - " + Core.priceToString(range.getValue()));
				} else {
					row.set(priceIndex, Core.priceToString(range.getKey()));
				}
			}
		}

		return json;
	}

	@Override
	public void handle(Request request, Response response) {
		/* TODO: uncomment this */
		/*if (!request.getClientAddress().getAddress().equals(Core.FRONTEND_ADDRESS))
			return;*/

		Core.execute(new QueryProcessor(request, response));
	}
}

