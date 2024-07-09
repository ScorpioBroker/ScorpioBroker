package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import io.smallrye.mutiny.tuples.Tuple2;

public class EntityCache {

	Map<String, Tuple2<Map<String, Object>, Set<String>>> allIds2EntityAndHosts = Maps.newHashMap();

	public void putEntityIntoEntityCacheAndEntityMap(String entityId, Map<String, Object> entity, QueryRemoteHost host,
			EntityMap entityMap) {

		String id = (String) entity.get(NGSIConstants.JSON_LD_ID);
		if (entityMap != null) {
			entityMap.addEntry(id, host.cSourceId, host);
		}
		Tuple2<Map<String, Object>, Set<String>> allEntityAndRemoteHost = allIds2EntityAndHosts.get(id);
		if (allEntityAndRemoteHost == null) {
			allEntityAndRemoteHost = Tuple2.of(entity, Sets.newHashSet(host.cSourceId));
		}
	}

	public Map<String, Tuple2<Map<String, Object>, Set<String>>> getAllIds2EntityAndHosts() {
		return allIds2EntityAndHosts;
	}

	public void setAllIds2EntityAndHosts(Map<String, Tuple2<Map<String, Object>, Set<String>>> allIds2EntityAndHosts) {
		this.allIds2EntityAndHosts = allIds2EntityAndHosts;
	}

	public void setEntityIntoEntityCache(String id, Map<String, Object> entity, String csourceId) {
		allIds2EntityAndHosts.put(id, Tuple2.of(entity, Sets.newHashSet(csourceId)));

	}

	public void putEntity(String entityId, Map<String, Object> entity, String csourceId) {

		Tuple2<Map<String, Object>, Set<String>> value = allIds2EntityAndHosts.get(entityId);
		if (value == null) {
			value = Tuple2.of(entity, Sets.newHashSet(csourceId));
			allIds2EntityAndHosts.put(entityId, value);
		} else {
			value.getItem2().add(csourceId);
		}

	}

	public Tuple2<Map<String, Object>, Set<String>> get(String entityId) {
		return allIds2EntityAndHosts.get(entityId);
	}

	public boolean containsEntity(String id) {
		return allIds2EntityAndHosts.containsKey(id);
	}

}
