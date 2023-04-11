package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

import io.vertx.mutiny.core.MultiMap;

public record QueryRemoteHost(String host, String tenant, MultiMap headers, String cSourceId, boolean canDoSingleOp,
		boolean canDoBatchOp, int regMode, String queryString) {

	public Object toJson() {
		// TODO Benni generate a json map to store in the db
		return null;
	}

	public boolean isLocal() {
		return host == null;
	}

	public static List<QueryRemoteHost> getRemoteHostsFromJson(List list) {
		// TODO Auto-generated method stub
		return null;
	}

}
