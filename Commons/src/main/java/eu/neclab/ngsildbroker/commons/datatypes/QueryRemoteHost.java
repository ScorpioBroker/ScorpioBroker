package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;

import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;

public record QueryRemoteHost(String host, String tenant, MultiMap headers, String cSourceId, boolean canDoSingleOp,
		boolean canDoBatchOp, int regMode, String queryString, boolean canDoIdQuery, boolean canDoZip,
		String remoteToken) {

	public JsonObject toJson() {
		JsonObject result = new JsonObject();
		List<String[]> headersToStore;
		if (headers == null) {
			headersToStore = null;
		} else {
			headersToStore = Lists.newArrayList();
			for (Entry<String, String> header : headers.entries()) {
				headersToStore.add(new String[] { header.getKey(), header.getValue() });
			}
		}
		result.put("h", host);
		result.put("t", tenant);
		result.put("k", headersToStore); // k for kopf german for header since h is taken
		result.put("c", cSourceId);
		result.put("s", canDoSingleOp);
		result.put("b", canDoBatchOp);
		result.put("r", regMode);
		result.put("q", queryString);
		result.put("i", canDoIdQuery);
		result.put("z", canDoZip);
		result.put("o", remoteToken); // o because r and t are taken and i can't think of something sensible
		return result;
	}

	public boolean isLocal() {
		return host == null;
	}

	public static List<QueryRemoteHost> getRemoteHostsFromJson(List list) {
		List<QueryRemoteHost> result = new ArrayList<>(list.size());
		for (Object obj : list) {
			Map<String, Object> map = (Map<String, Object>) obj;
			String host = null;
			String tenant = null;
			MultiMap headers = null;
			String cSourceId = null;
			boolean canDoSingleOp = false;
			boolean canDoBatchOp = false;
			int regMode = -1;
			String queryString = null;
			boolean canDoIdQuery = false;
			boolean canDoZip = false;
			String remoteToken = null;
			for (Entry<String, Object> entry : map.entrySet()) {
				Object value = entry.getValue();
				switch (entry.getKey()) {
				case "h":
					host = value == null ? null : (String) value;
					break;
				case "t":
					tenant = (String) entry.getValue();
					break;
				case "k":
					if (value != null) {
						List<Map<String, String>> tmp = (List<Map<String, String>>) value;
						headers = new MultiMap(null);
						for (Map<String, String> header : tmp) {
							Entry<String, String> headerEntry = header.entrySet().iterator().next();
							headers.add(headerEntry.getKey(), headerEntry.getValue());
						}
					}
				case "c":
					cSourceId = value == null ? null : (String) value;
					break;
				case "s":
					canDoSingleOp = value == null ? false : (Boolean) value;
					break;
				case "b":
					canDoBatchOp = value == null ? false : (Boolean) value;
					break;
				case "r":
					regMode = value == null ? -1 : (Integer) value;
					break;
				case "q":
					queryString = value == null ? null : (String) value;
					break;
				case "i":
					canDoIdQuery = value == null ? false : (Boolean) value;
					break;
				case "z":
					canDoZip = value == null ? false : (Boolean) value;
					break;
				case "o":
					remoteToken = value == null ? null : (String) value;
					break;
				default:
					break;
				}
			}
			result.add(new QueryRemoteHost(host, tenant, headers, cSourceId, canDoSingleOp, canDoBatchOp, regMode,
					queryString, canDoIdQuery, canDoZip, remoteToken));
		}
		return result;
	}

	public static QueryRemoteHost fromRemoteHost(RemoteHost remoteHost, String queryString, boolean canDoIdQuery,
			boolean canDoZip, String remoteToken) {
		return new QueryRemoteHost(remoteHost.host(), remoteHost.tenant(), remoteHost.headers(), remoteHost.cSourceId(),
				remoteHost.canDoSingleOp(), remoteHost.canDoBatchOp(), remoteHost.regMode(), queryString, canDoIdQuery,
				canDoZip, remoteToken);
	}

}
