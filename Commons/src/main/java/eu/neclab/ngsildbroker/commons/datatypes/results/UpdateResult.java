package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class UpdateResult {
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
	private List<Map<String, Object>> updated = new ArrayList<Map<String, Object>>();
	private List<Map<String, Object>> notUpdated = new ArrayList<Map<String, Object>>();

	public void addToUpdated(String updatedAttrId) {
		HashMap<String, Object> tmp = new HashMap<String, Object>();
		tmp.put(NGSIConstants.JSON_LD_VALUE, updatedAttrId);
		updated.add(tmp);
	}

	public void addToNotUpdated(String notUpdatedAttrId, String reason) {
		HashMap<String, Object> notUpdatedEntry = new HashMap<String, Object>();
		HashMap<String, Object> tmp = new HashMap<String, Object>();
		List<Map<String, Object>> tmp2 = new ArrayList<Map<String, Object>>();
		tmp.put(NGSIConstants.JSON_LD_ID, notUpdatedAttrId);
		tmp2.add(tmp);
		notUpdatedEntry.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAME, tmp2);
		tmp = new HashMap<String, Object>();
		tmp2 = new ArrayList<Map<String, Object>>();
		tmp.put(NGSIConstants.JSON_LD_VALUE, reason);
		tmp2.add(tmp);
		notUpdatedEntry.put(NGSIConstants.NGSI_LD_REASON, tmp2);
		notUpdated.add(notUpdatedEntry);
	}

	public List<Map<String, Object>> getUpdated() {
		return updated;
	}

	public List<Map<String, Object>> getNotUpdated() {
		return notUpdated;
	}

	public Map<String, Object> toJsonMap() {
		HashMap<String, Object> result = new HashMap<String, Object>();
		if (!updated.isEmpty()) {
			result.put(NGSIConstants.NGSI_LD_UPDATED, updated);
		}
		if (!notUpdated.isEmpty()) {
			result.put(NGSIConstants.NGSI_LD_NOT_UPDATED, notUpdated);
		}
		return result;
	}

}
