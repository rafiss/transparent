package transparent.core;

import java.io.PrintStream;
import java.util.Map.Entry;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

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

		@Override
		public void run() {
			try {
				PrintStream body = response.getPrintStream();
				response.setValue("Content-Type", "text/plain");

				Object object = parser.parse(request.getContent());
				if (!(object instanceof JSONObject)) {
					body.close();
					return;
				}

				JSONObject map = (JSONObject) object;
				Object selectObject = map.get("select");
				if (!(selectObject instanceof JSONArray)) {
					body.close();
					return;
				}

				JSONArray selectList = (JSONArray) selectObject;
				String[] select = new String[selectList.size()];
				for (int i = 0; i < selectList.size(); i++) {
					if (!(selectList.get(i) instanceof String)) {
						body.close();
						return;
					}

					select[i] = (String) selectList.get(i);
				}

				Object whereObject = map.get("where");
				Condition[] where = new Condition[0];
				if (whereObject == null) {
					if (!(whereObject instanceof JSONObject)) {
						body.close();
						return;
					}

					int i = 0;
					JSONObject whereMap = (JSONObject) whereObject;
					where = new Condition[whereMap.size()];
					for (Entry<String, Object> pair : whereMap.entrySet()) {
						String key = pair.getKey();
						if (!(pair.getValue() instanceof String)) {
							body.close();
							return;
						}

						String value = (String) pair.getValue();
						if (value.length() == 0) {
							body.close();
							return;
						}
						Relation relation = parse(value.charAt(0));
						if (relation == null) {
							body.close();
							return;
						}

						where[i] = new Condition(key, relation, value.substring(1));
						i++;
					}
				}

				String sort = null;
				Object sortObject = map.get("sort");
				if (sortObject != null) {
					if (!(sortObject instanceof String)) {
						body.close();
						return;
					}

					sort = (String) sortObject;
				}

				int page = -1;
				Object pageObject = map.get("page");
				if (pageObject != null) {
					if (!(pageObject instanceof String)) {
						body.close();
						return;
					}

					page = Integer.parseInt((String) pageObject);
				}

				int limit = -1;
				Object limitObject = map.get("limit");
				if (limitObject != null) {
					if (!(limitObject instanceof String)) {
						body.close();
						return;
					}

					limit = Integer.parseInt((String) limitObject);
				}

				query(select, where, sort, page, limit);
			} catch (Exception e) { }
		}
	}

	private static void query(String[] select,
			Condition[] where, String sort, int page, int limit)
	{
		
	}

	@Override
	public void handle(Request request, Response response) {
		if (!request.getClientAddress().getAddress().equals(Core.FRONTEND_ADDRESS))
			return;

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

		public Condition(String key, Relation relation, String value) {
			this.key = key;
			this.value = value;
			this.relation = relation;
		}
	}
}
