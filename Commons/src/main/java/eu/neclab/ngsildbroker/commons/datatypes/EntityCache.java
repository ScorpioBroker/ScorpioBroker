package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import io.smallrye.mutiny.tuples.Tuple2;

public class EntityCache {
	Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> type2Id2EntityAndHosts = Maps
			.newHashMap();
	Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> allIds2EntityAndHosts = Maps.newHashMap();

	public void putEntitiesIntoEntityCacheAndEntityMap(List<?> entities, QueryRemoteHost host, EntityMap entityMap) {
		for (Object entityObj : entities) {
			Map<String, Object> entity = (Map<String, Object>) entityObj;
			putEntityIntoEntityCacheAndEntityMap(entity, host, entityMap);
		}
	}

	public void putEntityIntoEntityCacheAndEntityMap(Map<String, Object> entity, QueryRemoteHost host,
			EntityMap entityMap) {
		List<String> types = (List<String>) entity.get(NGSIConstants.JSON_LD_TYPE);
		String id = (String) entity.get(NGSIConstants.JSON_LD_ID);
		if (entityMap != null) {
			entityMap.getEntry(id).addRemoteHost(host);
		}
		for (String type : types) {

			Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2Entity2RemoteHost = type2Id2EntityAndHosts
					.get(type);
			if (ids2Entity2RemoteHost == null) {
				ids2Entity2RemoteHost = Maps.newHashMap();
				type2Id2EntityAndHosts.put(type, ids2Entity2RemoteHost);
			}
			Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndRemoteHost = ids2Entity2RemoteHost
					.get(id);
			Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> allEntityAndRemoteHost = allIds2EntityAndHosts
					.get(id);
			if (entityAndRemoteHost == null) {
				Map<String, QueryRemoteHost> hostName2RemoteHost = Maps.newHashMap();
				hostName2RemoteHost.put(host.host(), host);
				entityAndRemoteHost = Tuple2.of(entity, hostName2RemoteHost);
			} else {
				Map<String, QueryRemoteHost> hostName2RemoteHost = entityAndRemoteHost.getItem2();
				hostName2RemoteHost.put(host.host(), host);
				Map<String, Object> currentEntity = entityAndRemoteHost.getItem1();
				if (currentEntity == null) {
					entityAndRemoteHost = Tuple2.of(entity, hostName2RemoteHost);
				} else {
					entityAndRemoteHost = Tuple2.of(EntityTools.mergeEntity(currentEntity, entity),
							hostName2RemoteHost);
				}
			}
			ids2Entity2RemoteHost.put(id, entityAndRemoteHost);
			if (allEntityAndRemoteHost == null) {
				Map<String, QueryRemoteHost> hostName2RemoteHost = Maps.newHashMap();
				hostName2RemoteHost.put(host.host(), host);
				allEntityAndRemoteHost = Tuple2.of(entity, hostName2RemoteHost);
			} else {
				Map<String, QueryRemoteHost> hostName2RemoteHost = allEntityAndRemoteHost.getItem2();
				hostName2RemoteHost.put(host.host(), host);
				Map<String, Object> currentEntity = allEntityAndRemoteHost.getItem1();
				if (currentEntity == null) {
					entityAndRemoteHost = Tuple2.of(entity, hostName2RemoteHost);
				} else {
					entityAndRemoteHost = Tuple2.of(EntityTools.mergeEntity(currentEntity, entity),
							hostName2RemoteHost);
				}
			}
			allIds2EntityAndHosts.put(id, entityAndRemoteHost);

		}

	}

	public void mergeCache(EntityCache remoteCache) {
		for (Entry<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> remoteEntry : remoteCache
				.getType2Id2EntityAndHosts().entrySet()) {
			String rType = remoteEntry.getKey();
			Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> rId2EntityAndHosts = remoteEntry
					.getValue();
			Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> lId2EntityAndHost = type2Id2EntityAndHosts
					.get(rType);
			if (lId2EntityAndHost == null) {
				lId2EntityAndHost = Maps.newHashMap();
				type2Id2EntityAndHosts.put(rType, lId2EntityAndHost);
			}
			for (Entry<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> rId2EntityAndHostEntry : rId2EntityAndHosts
					.entrySet()) {
				String rId = rId2EntityAndHostEntry.getKey();
				Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> lEntityAndHost = lId2EntityAndHost.get(rId);
				Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> lAllEntityAndHost = allIds2EntityAndHosts
						.get(rId);
				Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> rEntityAndHost = rId2EntityAndHostEntry
						.getValue();
				Map<String, Object> rEntity = rEntityAndHost.getItem1();
				Map<String, QueryRemoteHost> rRemoteHosts = rEntityAndHost.getItem2();
				if (lEntityAndHost == null) {
					lEntityAndHost = Tuple2.of(rEntity, rRemoteHosts);
					lId2EntityAndHost.put(rId, lEntityAndHost);
				} else {
					Map<String, Object> lEntity = lEntityAndHost.getItem1();
					Map<String, QueryRemoteHost> lRemoteHosts = lEntityAndHost.getItem2();
					lRemoteHosts.putAll(rRemoteHosts);
					if (lEntity == null) {
						lEntityAndHost = Tuple2.of(rEntity, lRemoteHosts);
						lId2EntityAndHost.put(rId, lEntityAndHost);
					} else {
						EntityTools.mergeEntity(lEntity, rEntity);
					}
				}
				if (lAllEntityAndHost == null) {
					lAllEntityAndHost = Tuple2.of(rEntity, rRemoteHosts);
					allIds2EntityAndHosts.put(rId, lEntityAndHost);
				} else {
					Map<String, Object> lEntity = lEntityAndHost.getItem1();
					Map<String, QueryRemoteHost> lRemoteHosts = lEntityAndHost.getItem2();
					lRemoteHosts.putAll(rRemoteHosts);
					if (lEntity == null) {
						lEntityAndHost = Tuple2.of(rEntity, lRemoteHosts);
						lId2EntityAndHost.put(rId, lEntityAndHost);
					}
				}
			}
		}

	}

	public Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> getType2Id2EntityAndHosts() {
		return type2Id2EntityAndHosts;
	}

	public void setType2Id2EntityAndHosts(
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> type2Id2EntityAndHosts) {
		this.type2Id2EntityAndHosts = type2Id2EntityAndHosts;
	}

	public Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> getAllIds2EntityAndHosts() {
		return allIds2EntityAndHosts;
	}

	public void setAllIds2EntityAndHosts(
			Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> allIds2EntityAndHosts) {
		this.allIds2EntityAndHosts = allIds2EntityAndHosts;
	}

	public Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> getByType(String type) {
		return type2Id2EntityAndHosts.get(type);
	}

	public void setEntityIntoEntityCache(String type, String id, Map<String, Object> entity,
			Map<String, QueryRemoteHost> remoteHostMap) {
		Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> id2EntityAndRemoteHostInfo = type2Id2EntityAndHosts
				.get(type);
		if (id2EntityAndRemoteHostInfo == null) {
			id2EntityAndRemoteHostInfo = Maps.newHashMap();
			type2Id2EntityAndHosts.put(type, id2EntityAndRemoteHostInfo);
		}
		allIds2EntityAndHosts.put(id, Tuple2.of(entity, remoteHostMap));
		id2EntityAndRemoteHostInfo.put(id, Tuple2.of(entity, remoteHostMap));

	}

	public void putEntity(String entityId, Collection<String> types, Map<String, Object> entity,
			Map<String, QueryRemoteHost> name2Host) {

		for (String type : types) {

			Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> id2EntityAndHosts = type2Id2EntityAndHosts
					.get(type);
			if (id2EntityAndHosts == null) {
				id2EntityAndHosts = Maps.newHashMap();
				type2Id2EntityAndHosts.put(type, id2EntityAndHosts);
			}
			Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> value = id2EntityAndHosts.get(entityId);
			if (value == null) {
				value = Tuple2.of(entity, name2Host);
			} else {
				name2Host.putAll(value.getItem2());
				value = Tuple2.of(entity, name2Host);
			}
			id2EntityAndHosts.put(entityId, value);
			allIds2EntityAndHosts.put(entityId, value);
		}
		

	}

}
