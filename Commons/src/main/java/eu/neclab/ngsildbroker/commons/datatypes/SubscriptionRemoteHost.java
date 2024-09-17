package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;
import java.util.Map;

import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.core.MultiMap;

public class SubscriptionRemoteHost extends QueryRemoteHost {

	String subscriptionId;
	Map<String, Object> subParam;

	public SubscriptionRemoteHost(String host, String tenant, MultiMap headers, String cSourceId, boolean canDoQuery,
			boolean canDoBatchQuery, boolean canDoRetrieve, int regMode,
			List<Tuple3<String, String, String>> idsAndTypesAndIdPattern, Map<String, String> queryParams,
			boolean canDoEntityMap, boolean canDoZip, String entityMapToken, ViaHeaders viaHeaders) {
		super(host, tenant, headers, cSourceId, canDoQuery, canDoBatchQuery, canDoRetrieve, regMode,
				idsAndTypesAndIdPattern, queryParams, canDoEntityMap, canDoZip, entityMapToken, viaHeaders);
	}

	public String getSubscriptionId() {
		return subscriptionId;
	}

	public void setSubscriptionId(String subscriptionId) {
		this.subscriptionId = subscriptionId;
	}

	public static SubscriptionRemoteHost fromQueryRemoteHost(QueryRemoteHost qHost) {
		return new SubscriptionRemoteHost(qHost.host, qHost.tenant, qHost.headers, qHost.cSourceId, qHost.canDoQuery,
				qHost.canDoBatchQuery, qHost.canDoRetrieve, qHost.regMode, qHost.idsAndTypesAndIdPattern,
				qHost.queryParams, qHost.canDoEntityMap, qHost.canDoZip, qHost.entityMapToken, qHost.viaHeaders);
	}

	public void setSubParam(Map<String, Object> subParam) {
		this.subParam = subParam;
		
	}

	public Map<String, Object> getSubParam() {
		return subParam;
	}
	

}
