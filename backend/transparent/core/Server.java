package transparent.core;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;

import transparent.core.database.Database.Results;

public class Server implements Container
{
	private static final JSONParser parser =
			new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);
	private static final int QUERY_LIMIT = 128;

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

		@Override
		public void run() {
			PrintStream body = null;
			try {
				body = response.getPrintStream();
				response.setValue("Content-Type", "text/plain");

				Object object = parser.parse(request.getContent());
				if (!(object instanceof JSONObject)) {
					body.close();
					body.println(error("Root structure must be a map."));
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
				Condition[] where = new Condition[0];
				Condition name = null;
				if (whereObject != null) {
					if (!(whereObject instanceof JSONObject)) {
						body.println(error("'where' key must map to a map of string pairs."));
						body.close();
						return;
					}

					int i = 0;
					JSONObject whereMap = (JSONObject) whereObject;
					where = new Condition[whereMap.size()];
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
						Relation relation = parse(value.charAt(0));
						if (relation == null) {
							body.println(error("Unable to parse relation operator"));
							body.close();
							return;
						}

						if (key.equals("name")) {
							name = new Condition(key, relation, value.substring(1));
							where[i] = null;
						} else
							where[i] = new Condition(key, relation, value.substring(1));
						i++;
					}
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

				int page = -1;
				Object pageObject = map.get("page");
				if (pageObject != null) {
					if (pageObject instanceof Integer) {
						page = (int) pageObject;
					} else if (pageObject instanceof String) {
						page = Integer.parseInt((String) pageObject);
					} else {
						body.println(error("'page' key must map to an integer or string."));
						body.close();
						return;
					}

					page = Integer.parseInt((String) pageObject);
				}

				int limit = -1;
				Object limitObject = map.get("limit");
				if (limitObject != null) {
					if (limitObject instanceof Integer) {
						limit = (int) limitObject;
					} else if (limitObject instanceof String) {
						limit = Integer.parseInt((String) limitObject);
					} else {
						body.println(error("'limit' key must map to an integer or string."));
						body.close();
						return;
					}
				}

				JSONArray results = query(select, name, where, sort, page, limit);
				if (results != null)
					body.println(results.toJSONString());
				else
					body.println(error("Internal error occurred during query."));
				body.close();
			} catch (Exception e) {
				if (body != null) {
					body.println(error("Exception thrown: " + e.getMessage()));
					body.close();
				}
			}
		}
	}

	private static JSONArray query(String[] select, Condition name,
			Condition[] where, String sort, int page, int limit)
	{ /* TODO: implement sort and pagination */
		if (name != null) {
			/* search by name */
			Iterator<ProductID> results = Core.searchProductName(name.getValue());

			/* construct a map of where conditions */
			HashMap<String, Condition> conditions = new HashMap<String, Condition>();
			for (Condition condition : where)
				if (condition != null)
					conditions.put(condition.getKey(), condition);

			/* construct a map of select keys */
			HashMap<String, Integer> selectMap = new HashMap<String, Integer>();
			for (int i = 0; i < select.length; i++)
				selectMap.put(select[i], i);

			HashSet<String> properties = new HashSet<String>();
			for (String s : select)
				properties.add(s);
			for (Condition condition : where)
				if (condition != null)
					properties.add(condition.getKey());
			String[] propertiesArray = new String[properties.size()];
			propertiesArray = properties.toArray(propertiesArray);

			JSONArray json = new JSONArray();
			ArrayList<ProductID> rowIds = new ArrayList<ProductID>();
			while ((json.size() < limit || limit < 0) && results.hasNext())
			{
				for (int i = 0; i < QUERY_LIMIT; i++) {
					if (!results.hasNext())
						break;
					rowIds.add(results.next());
				}

				ProductID[] rowArray = new ProductID[rowIds.size()];
				rowArray = rowIds.toArray(rowArray);
				Results dbresults = Core.getDatabase().query(rowArray, propertiesArray);

				/* process the where clauses */
				long prevId = -1;
				boolean discard = true;
				JSONArray row = new JSONArray();
				row.ensureCapacity(select.length);
				while (row.size() < select.length)
					row.add(null);
				while (dbresults.next()) {
					long id = dbresults.getLong(1);
					if (id != prevId) {
						/* push the last row into our list of results */
						if (!discard) {
							json.add(row);
							if (json.size() == limit)
								return json;
							row = new JSONArray();
							row.ensureCapacity(select.length);
						}

						while (row.size() < select.length)
							row.add(null);

						prevId = id;
						discard = false;
					} else if (discard)
						continue;

					/* ensure the key-value passes our where clauses */
					String key = dbresults.getString(2);
					String value = dbresults.getString(3);
					Condition condition = conditions.get(key);
					if (condition != null && !condition.evaluate(value)) {
						discard = true;
						continue;
					}

					/* add this value to our current row */
					Integer index = selectMap.get(key);
					if (index != null)
						row.set(index, value);
				}

				rowIds.clear();
			}

			return json;
		} else {
			/* TODO: implement this */
			return null;
		}
	}

	@Override
	public void handle(Request request, Response response) {
		/* TODO: uncomment this */
		/*if (!request.getClientAddress().getAddress().equals(Core.FRONTEND_ADDRESS))
			return;*/

		Core.execute(new QueryProcessor(request, response));
	}

	private static enum Relation {
		EQUALS,
		LESS_THAN,
		GREATER_THAN
	}

	public static Relation parse(char operator) {
		switch (operator) {
		case '=':
			return Relation.EQUALS;
		case '<':
			return Relation.LESS_THAN;
		case '>':
			return Relation.GREATER_THAN;
		default:
			return null;
		}
	}

	private static class Condition {
		private String key;
		private String value;
		private Relation relation;

		private double doubleValue;

		public Condition(String key, Relation relation, String value) {
			this.key = key;
			this.value = value;
			this.relation = relation;

			try {
				doubleValue = Double.parseDouble(value);
			} catch (NumberFormatException e) {
				doubleValue = Double.NaN;
			}
		}

		public String getKey() {
			return key;
		}

		public String getValue() {
			return value;
		}

		public Relation getRelation() {
			return relation;
		}

		public boolean evaluate(String input) {
			switch (relation) {
			case EQUALS:
				return value.equals(input);
			case LESS_THAN:
				try {
					return Double.parseDouble(input) < doubleValue;
				} catch (NumberFormatException e) {
					return false;
				}
			case GREATER_THAN:
				try {
					return Double.parseDouble(input) > doubleValue;
				} catch (NumberFormatException e) {
					return false;
				}
			default:
				return false;
			}
		}
	}
}
