package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.terms.Query;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class EntityMap {

	private LinkedHashMap<String, Set<String>> entityId2CSourceIds = Maps.newLinkedHashMap();

	private HashMap<String, QueryRemoteHost> cSourceId2RemoteHost = Maps.newHashMap();

	private Map<String, Set<String>> csourceId2EntityIds = Maps.newHashMap();

	private Map<String, Object> linkedMaps = Maps.newHashMap();

	private String queryCheckSum;

	private boolean distEntities;

	private boolean regEmptyOrNoRegEntryAndNoLinkedQuery;

	private boolean noRootLevelRegEntryAndLinkedQuery;

	private boolean changed = false;

	private Query query;

	private String id;

	public EntityMap(String id, boolean distEntities, boolean regEmptyOrNoRegEntryAndNoLinkedQuery,
			boolean noRootLevelRegEntryAndLinkedQuery) {
		this.id = id;
		this.distEntities = distEntities;
		this.regEmptyOrNoRegEntryAndNoLinkedQuery = regEmptyOrNoRegEntryAndNoLinkedQuery;
		this.noRootLevelRegEntryAndLinkedQuery = noRootLevelRegEntryAndLinkedQuery;
	}

	public static EntityMap fromJson(String id, JsonObject json, ObjectMapper objectMapper) {
		boolean splitEntities = json.getBoolean("splitEntities");
		boolean regEmptyOrNoRegEntryAndNoLinkedQuery = json.getBoolean("regEmptyOrNoRegEntryAndNoLinkedQuery");
		boolean noRootLevelRegEntryAndLinkedQuery = json.getBoolean("noRootLevelRegEntryAndLinkedQuery");
		EntityMap result = new EntityMap(id, splitEntities, regEmptyOrNoRegEntryAndNoLinkedQuery,
				noRootLevelRegEntryAndLinkedQuery);

		LinkedHashMap<String, Set<String>> id2Cid = result.getEntityId2CSourceIds();
		HashMap<String, QueryRemoteHost> cId2Host = result.getcSourceId2RemoteHost();
		JsonArray entityMap = json.getJsonArray("entityMap");
		if (entityMap != null) {
			entityMap.forEach(arrayEntry -> {
				JsonObject obj = (JsonObject) arrayEntry;
				obj.forEach(entry -> {
					id2Cid.put(entry.getKey(), Sets.newHashSet(((JsonArray) entry.getValue()).getList()));
				});

			});
		}
		if (json.containsKey("hosts")) {
			json.getJsonObject("hosts").forEach(entry -> {

				try {
					cId2Host.put(entry.getKey(),
							objectMapper.readValue((String) entry.getValue(), QueryRemoteHost.class));
				} catch (JsonMappingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		}
		return result;
	}

	public void addEntry(String entityId, String csourceId, QueryRemoteHost remoteHost) {
		Set<String> csourceIds = entityId2CSourceIds.get(entityId);
		if (csourceIds == null) {
			csourceIds = Sets.newHashSet();
			entityId2CSourceIds.put(entityId, csourceIds);
		}
		csourceIds.add(csourceId);
		if (remoteHost != null && NGSIConstants.JSON_LD_NONE.equals(csourceId)) {
			cSourceId2RemoteHost.put(csourceId, remoteHost);
			linkedMaps.put(csourceId, remoteHost.entityMapToken);
		}
	}

	public LinkedHashMap<String, Set<String>> getEntityId2CSourceIds() {
		return entityId2CSourceIds;
	}

	public void setEntityId2CSourceIds(LinkedHashMap<String, Set<String>> entityId2CSourceIds) {
		this.entityId2CSourceIds = entityId2CSourceIds;
	}

	public HashMap<String, QueryRemoteHost> getcSourceId2RemoteHost() {
		return cSourceId2RemoteHost;
	}

	public void setcSourceId2RemoteHost(HashMap<String, QueryRemoteHost> cSourceId2RemoteHost) {
		this.cSourceId2RemoteHost = cSourceId2RemoteHost;
	}

	public Map<String, Set<String>> getCsourceId2EntityIds() {
		return csourceId2EntityIds;
	}

	public void setCsourceId2EntityIds(Map<String, Set<String>> csourceId2EntityIds) {
		this.csourceId2EntityIds = csourceId2EntityIds;
	}

	public Map<String, Object> getLinkedMaps() {
		return linkedMaps;
	}

	public void setLinkedMaps(Map<String, Object> linkedMaps) {
		this.linkedMaps = linkedMaps;
	}

	public boolean isRegEmptyOrNoRegEntryAndNoLinkedQuery() {
		return regEmptyOrNoRegEntryAndNoLinkedQuery;
	}

	public void setRegEmptyOrNoRegEntryAndNoLinkedQuery(boolean regEmptyOrNoRegEntryAndNoLinkedQuery) {
		this.regEmptyOrNoRegEntryAndNoLinkedQuery = regEmptyOrNoRegEntryAndNoLinkedQuery;
	}

	public boolean isNoRootLevelRegEntryAndLinkedQuery() {
		return noRootLevelRegEntryAndLinkedQuery;
	}

	public void setNoRootLevelRegEntryAndLinkedQuery(boolean noRootLevelRegEntryAndLinkedQuery) {
		this.noRootLevelRegEntryAndLinkedQuery = noRootLevelRegEntryAndLinkedQuery;
	}

	public boolean isChanged() {
		return changed;
	}

	public void setChanged(boolean changed) {
		this.changed = changed;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public JsonObject toSQLJson(ObjectMapper objectMapper) {
		JsonObject result = new JsonObject();
		result.put("id", id);
		result.put("entityMap", entityId2CSourceIds);
		result.put("linkedMaps", linkedMaps);
		result.put("distEntities", distEntities);
		result.put("regEmptyOrNoRegEntryAndNoLinkedQuery", regEmptyOrNoRegEntryAndNoLinkedQuery);
		result.put("noRootLevelRegEntryAndLinkedQuery", noRootLevelRegEntryAndLinkedQuery);
		JsonObject hosts = new JsonObject();
		for (Entry<String, QueryRemoteHost> entry : cSourceId2RemoteHost.entrySet()) {
			try {
				hosts.put(entry.getKey(), objectMapper.writeValueAsString(entry.getValue()));
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		result.put("hosts", hosts);
		return result;
	}

	public String getQueryCheckSum() {
		return queryCheckSum;
	}

	public void setQueryCheckSum(String queryCheckSum) {
		this.queryCheckSum = queryCheckSum;
	}

	public boolean isEmpty() {
		return entityId2CSourceIds.isEmpty();
	}

	public int size() {
		return entityId2CSourceIds.size();
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public QueryRemoteHost getRemoteHost(String cId) {
		return cSourceId2RemoteHost.get(cId);
	}

	public boolean removeEntries(Set<String> ids) {
		if (ids.isEmpty()) {
			return false;
		}
		ids.forEach(id -> {
			entityId2CSourceIds.remove(id);
		});

		return true;
	}

	public void addLinkedMap(String cSourceId, String mapId) {
		linkedMaps.put(cSourceId, mapId);

	}

	public boolean isDistEntities() {
		return distEntities;
	}

	public void setDistEntities(boolean distEntities) {
		this.distEntities = distEntities;
	}

}
