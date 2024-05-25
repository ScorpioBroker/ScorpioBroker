package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;

public interface QueryServiceInterface {

	Uni<Tuple2<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>, List<Map<String, Object>>>> getEntitiesFromUncalledHosts(
			String tenant, Map<Set<String>, Set<String>> types2EntityIds,
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> fullEntityCache,
			Context linkHeaders, QQueryTerm linkedQ, boolean expectFullEntities);

}
