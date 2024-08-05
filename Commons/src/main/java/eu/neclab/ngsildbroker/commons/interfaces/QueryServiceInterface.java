package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.datatypes.EntityCache;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;

public interface QueryServiceInterface {

	

	Uni<Tuple2<EntityCache, List<Map<String, Object>>>> getEntitiesFromUncalledHosts(
			String tenant, Map<Set<String>, Set<String>> types2EntityIds, EntityCache fullEntityCache,
			Context linkHeaders, QQueryTerm linkedQ, boolean expectFullEntities);

}
