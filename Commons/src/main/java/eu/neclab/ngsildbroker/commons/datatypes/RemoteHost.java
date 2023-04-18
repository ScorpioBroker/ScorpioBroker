package eu.neclab.ngsildbroker.commons.datatypes;

import io.vertx.mutiny.core.MultiMap;

public record RemoteHost(String host, String tenant, MultiMap headers, String cSourceId, boolean canDoSingleOp,
		boolean canDoBatchOp, int regMode, boolean canDoZip, boolean canDoEntityId) {

	public Object toJson() {
		// TODO Benni generate a json map to store in the db
		return null;
	}

	public boolean isLocal() {
		// TODO Auto-generated method stub
		return false;
	}

}
