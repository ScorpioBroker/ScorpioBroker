package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class QueryHistoryEntitiesRequest extends BaseRequest {

	private QueryParams qp;

	public QueryHistoryEntitiesRequest(ArrayListMultimap<String, String> headers, QueryParams qp) {
		super(headers);
		this.qp = qp;
		List<String> tenant = this.headers.get(AppConstants.TENANT_HEADER);
		if(tenant.isEmpty()) {
			qp.setTenant(null);
		}else {
			qp.setTenant(tenant.get(0));
		}
	}

	public QueryParams getQp() {
		return qp;
	}

	public void setQp(QueryParams qp) {
		this.qp = qp;
	}

}
