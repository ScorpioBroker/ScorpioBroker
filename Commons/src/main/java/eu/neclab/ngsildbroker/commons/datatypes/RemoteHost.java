package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.vertx.mutiny.core.MultiMap;

public record RemoteHost(String host, String tenant, MultiMap headers, String cSourceId, boolean canDoSingleOp,
		boolean canDoBatchOp, int regMode, boolean canDoZip, boolean canDoEntityId) {

	public Object toJson() {
		Map<String, Object> result = new HashMap<>(9);
		result.put("host",host);
		result.put("tenant",tenant);
		result.put("headers",headers);
		result.put("cSourceId",cSourceId);
		result.put("canDoSingleOp",canDoSingleOp);
		result.put("canDoBatchOp",canDoBatchOp);
		result.put("regMode",regMode);
		result.put("canDoZip",canDoZip);
		result.put("canDoEntityId", canDoEntityId);
		return result;
	}

	public boolean isLocal() {
		return host == null;
	}

	@Override
	public int hashCode() {
		//headers, 
		return Objects.hash(cSourceId, canDoBatchOp, canDoEntityId, canDoSingleOp, canDoZip, host, regMode,
				tenant);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RemoteHost other = (RemoteHost) obj;
		//&& Objects.equals(headers, other.headers)
		return Objects.equals(cSourceId, other.cSourceId) && canDoBatchOp == other.canDoBatchOp
				&& canDoEntityId == other.canDoEntityId && canDoSingleOp == other.canDoSingleOp
				&& canDoZip == other.canDoZip 
				&& Objects.equals(host, other.host) && regMode == other.regMode && Objects.equals(tenant, other.tenant);
	}
	
	

}
