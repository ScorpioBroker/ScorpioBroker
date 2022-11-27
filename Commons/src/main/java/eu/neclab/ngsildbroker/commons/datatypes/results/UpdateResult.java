package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;

public class UpdateResult extends CRUDBaseResult {
//	[
//	  {
//	    "https://uri.etsi.org/ngsi-ld/default-context/notUpdated": [
//	      {
//	        "https://uri.etsi.org/ngsi-ld/attributeName": [
//	          {
//	            "@id": "https://uri.etsi.org/ngsi-ld/default-context/attrib3"
//	          }
//	        ],
//	        "https://uri.etsi.org/ngsi-ld/reason": [
//	          {
//	            "@value": "already exists"
//	          }
//	        ]
//	      },
//	      {
//	        "https://uri.etsi.org/ngsi-ld/attributeName": [
//	          {
//	            "@id": "https://uri.etsi.org/ngsi-ld/default-context/attrib4"
//	          }
//	        ],
//	        "https://uri.etsi.org/ngsi-ld/reason": [
//	          {
//	            "@value": "already exists"
//	          }
//	        ]
//	      }
//	    ],
//	    "https://uri.etsi.org/ngsi-ld/updated": [
//	      {
//	        "@value": "attrib1"
//	      },
//	      {
//	        "@value": "attrib2"
//	      }
//	    ]
//	  }
//	]

	public UpdateResult() {
		this(null, null, null);
	}

	public UpdateResult(String endpoint, MultiMap headers, String cSourceId) {
		super(endpoint, headers, cSourceId);
	}

	private List<String> updated = Lists.newArrayList();
	private List<Map<String, String>> notUpdated = Lists.newArrayList();

	public void addToUpdated(String updatedAttrId) {
		updated.add(updatedAttrId);
	}

	public void addToNotUpdated(String notUpdatedAttrId, String reason) {
		Map<String, String> notUpdatedEntry = Maps.newHashMap();
		notUpdatedEntry.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAME_SHORT, notUpdatedAttrId);
		notUpdatedEntry.put(NGSIConstants.NGSI_LD_REASON_SHORT, reason);
		notUpdated.add(notUpdatedEntry);
	}

	@Override
	public Object getJson() {
		if (!updated.isEmpty()) {
			json.put(NGSIConstants.NGSI_LD_UPDATED_SHORT, updated);
		}
		if (!notUpdated.isEmpty()) {
			json.put(NGSIConstants.NGSI_LD_NOT_UPDATED_SHORT, notUpdated);
		}
		return json;
	}

	public static UpdateResult fromPayload(JsonObject body, String endpoint, MultiMap headers, String cSourceId) {
		if (body == null) {
			return null;
		}
		UpdateResult result = new UpdateResult(endpoint, headers, cSourceId);
		Map<String, Object> map = body.getMap();
		Object remoteUpdated = map.get(NGSIConstants.NGSI_LD_UPDATED_SHORT);
		Object remoteNotUpdated = map.get(NGSIConstants.NGSI_LD_UPDATED_SHORT);
		if (remoteUpdated != null) {
			result.updated = (List<String>) remoteUpdated;
		}
		if (remoteNotUpdated != null) {
			result.notUpdated = (List<Map<String, String>>) remoteNotUpdated;
		}
		return result;

	}

	public List<String> getUpdated() {
		return updated;
	}

	public List<Map<String, String>> getNotUpdated() {
		return notUpdated;
	}

}
