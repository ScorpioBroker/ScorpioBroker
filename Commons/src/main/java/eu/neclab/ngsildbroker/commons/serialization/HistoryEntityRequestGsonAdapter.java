package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import eu.neclab.ngsildbroker.commons.datatypes.HistoryAttribInstance;
import eu.neclab.ngsildbroker.commons.datatypes.HistoryEntityRequest;

public class HistoryEntityRequestGsonAdapter
		implements JsonSerializer<HistoryEntityRequest>, JsonDeserializer<HistoryEntityRequest> {

	private static final String ENTITY_ID = "id";
	private static final String ATTRIBUTE_ID = "aid";
	private static final String ENTITY_TYPE = "t";
	private static final String ENTITY_CREATED_AT = "cat";
	private static final String ENTITY_MODIFIED_AT = "mat";
	private static final String ENTITY_INSTANCE_ID = "iid";
	private static final String ATTRIBS = "attr";
	private static final String VALUE = "v";
	private static final String OVERWRITE_OP = "op";
	private static final String NULL_VALUE = "NUUULLLL";

	@Override
	public HistoryEntityRequest deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		JsonObject top = json.getAsJsonObject();
		HistoryEntityRequest result = new HistoryEntityRequest();
		for (Entry<String, JsonElement> entry : top.entrySet()) {
			String key = entry.getKey();
			String value = "";
			if (entry.getValue().isJsonPrimitive()) {
				value = entry.getValue().getAsString();
				if (value.equals(NULL_VALUE)) {
					value = null;
				}
			}
			switch (key) {
			case ENTITY_ID:
				result.setId(value);
				break;
			case ENTITY_TYPE:
				result.setType(value);
				break;
			case ENTITY_CREATED_AT:
				result.setCreatedAt(value);
				break;
			case ENTITY_MODIFIED_AT:
				result.setModifiedAt(value);
				break;
			case ENTITY_INSTANCE_ID:
				result.setInstanceId(value);
				break;
			case ATTRIBS:
				if (value == null) {
					result.setAttribs(null);
				} else {
					ArrayList<HistoryAttribInstance> list = new ArrayList<HistoryAttribInstance>();
					Iterator<JsonElement> it = entry.getValue().getAsJsonArray().iterator();
					while (it.hasNext()) {
						JsonObject next = it.next().getAsJsonObject();
						String entityId = null;
						String entityType = null;
						String entityCreatedAt = null;
						String entityModifiedAt = null;
						String attributeId = null;
						String elementValue = null;
						Boolean overwriteOp = null;
						for (Entry<String, JsonElement> attrEntry : next.entrySet()) {
							String attrEntryValue = attrEntry.getValue().getAsString();
							if (attrEntryValue.equals(NULL_VALUE)) {
								attrEntryValue = null;
							}
							switch (attrEntry.getKey()) {
							case VALUE:
								elementValue = attrEntryValue;
								break;
							case ENTITY_ID:
								entityId = attrEntryValue;
								break;
							case ENTITY_TYPE:
								entityType = attrEntryValue;
								break;
							case ATTRIBUTE_ID:
								attributeId = attrEntryValue;
								break;
							case ENTITY_CREATED_AT:
								entityCreatedAt = attrEntryValue;
								break;
							case ENTITY_MODIFIED_AT:
								entityModifiedAt = attrEntryValue;
								break;
							case OVERWRITE_OP:
								if (attrEntryValue == null) {
									overwriteOp = null;
								} else {
									overwriteOp = attrEntry.getValue().getAsBoolean();
								}
								break;

							default:
								break;
							}

						}
						HistoryAttribInstance inst = new HistoryAttribInstance(entityId, entityType, entityCreatedAt,
								entityModifiedAt, attributeId, elementValue, overwriteOp);
						list.add(inst);

					}
					result.setAttribs(list);
				}
				break;
			default:
				break;
			}
		}
		return null;
	}

	@Override
	public JsonElement serialize(HistoryEntityRequest src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		String entityId = src.getId();
		String entityType = src.getType();
		String entityCreatedAt = src.getCreatedAt();
		String entityModifiedAt = src.getModifiedAt();
		String instanceId = src.getInstanceId();
		if (entityId != null) {
			top.addProperty(ENTITY_ID, entityId);
		} else {
			top.addProperty(ENTITY_ID, NULL_VALUE);
		}
		if (entityType != null) {
			top.addProperty(ENTITY_TYPE, entityType);
		} else {
			top.addProperty(ENTITY_TYPE, NULL_VALUE);
		}
		if (entityCreatedAt != null) {
			top.addProperty(ENTITY_CREATED_AT, entityCreatedAt);
		} else {
			top.addProperty(ENTITY_CREATED_AT, NULL_VALUE);
		}
		if (entityModifiedAt != null) {
			top.addProperty(ENTITY_MODIFIED_AT, entityModifiedAt);
		} else {
			top.addProperty(ENTITY_MODIFIED_AT, NULL_VALUE);
		}
		if (instanceId != null) {
			top.addProperty(ENTITY_INSTANCE_ID, instanceId);
		} else {
			top.addProperty(ENTITY_INSTANCE_ID, NULL_VALUE);
		}

		if (src.getAttribs() == null || src.getAttribs().size() == 0) {
			top.addProperty(ATTRIBS, NULL_VALUE);
		} else {
			JsonArray attrs = new JsonArray();
			for (HistoryAttribInstance entry : src.getAttribs()) {
				String elementValue = entry.getElementValue();
				String entryId = entry.getEntityId();
				String entryType = entry.getEntityType();
				String entryAttrId = entry.getAttributeId();
				String entryCAt = entry.getEntityCreatedAt();
				String entyMAt = entry.getEntityModifiedAt();

				Boolean overwriteOp = entry.getOverwriteOp();
				JsonObject obj = new JsonObject();
				if (elementValue != null) {
					obj.addProperty(VALUE, elementValue);
				} else {
					obj.addProperty(VALUE, NULL_VALUE);
				}
				if (entryId != null) {
					obj.addProperty(ENTITY_ID, entryId);
				} else {
					obj.addProperty(ENTITY_ID, NULL_VALUE);
				}
				if (entryType != null) {
					obj.addProperty(ENTITY_TYPE, entryType);
				} else {
					obj.addProperty(ENTITY_TYPE, NULL_VALUE);
				}
				if (entryAttrId != null) {
					obj.addProperty(ATTRIBUTE_ID, entryAttrId);
				} else {
					obj.addProperty(ATTRIBUTE_ID, NULL_VALUE);
				}
				if (entryCAt != null) {
					obj.addProperty(ENTITY_CREATED_AT, entryCAt);
				} else {
					obj.addProperty(ENTITY_CREATED_AT, NULL_VALUE);
				}
				if (entyMAt != null) {
					obj.addProperty(ENTITY_MODIFIED_AT, entyMAt);
				} else {
					obj.addProperty(ENTITY_MODIFIED_AT, NULL_VALUE);
				}
				if (overwriteOp != null) {
					obj.addProperty(OVERWRITE_OP, overwriteOp);
				} else {
					obj.addProperty(OVERWRITE_OP, NULL_VALUE);
				}
				attrs.add(obj);
			}
			top.add(ATTRIBS, attrs);
		}

		return top;
	}

}
