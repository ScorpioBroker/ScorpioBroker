package eu.neclab.ngsildbroker.historyquerymanager.service;

import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.historyquerymanager.repository.HistoryDAO;
import io.smallrye.mutiny.Uni;

@Singleton
public class HistoryQueryService {

	private final static Logger logger = LoggerFactory.getLogger(HistoryQueryService.class);

	@Inject
	HistoryDAO historyDAO;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	public Uni<QueryResult> query(String tenant, Set<String> entityIds, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, TemporalQueryTerm tempQuery, AggrTerm aggrQuery, String lang, int lastN,
			int limit, int offSet, boolean count, boolean localOnly, Context context) {
		return null;
	}

	public Uni<Map<String, Object>> retrieveEntity(String tenant, String entityId, AttrsQueryTerm attrsQuery,
			AggrTerm aggrQuery, String lang, int lastN, boolean localOnly, Context context) {
		
		if(localOnly) {
			//return historyDAO.retrieveEntity(tenant, entityId, attrsQuery, aggrQuery, lang, lastN)
		}
		return null;
	}

}
