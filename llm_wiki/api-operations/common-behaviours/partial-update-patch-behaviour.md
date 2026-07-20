---
type: NGSI-LD Clause
title: "Partial Update Patch Behaviour"
description: "The Partial Update Patch procedure modifies an existing NGSI-LD element by overwriting the data at the Attribute level, replacing it with the data provided in the NGSI-LD Fragment."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.8
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.8"
---

The Partial Update Patch procedure modifies an existing NGSI-LD element by overwriting the data at the Attribute
level, replacing it with the data provided in the NGSI-LD Fragment.
When updating NGSI-LD elements (Entities, Context Source Registrations or Context Subscriptions) using
NGSI-LD Fragments, implementations shall determine the exact set of changes being requested by comparing the
content of the provided Fragment (patch) against the current content (a JSON-LD object) of the target element.
With respect to update operations, implementations shall perform an algorithm equivalent to the one described below
(adapted from IETF RFC 7396 [16]), in order to observe the name to URI expansion rules and the JSON-LD null
processing):
- For each member of the Fragment perform the term to URI expansion.
- If the provided Fragment (a JSON Merge Patch document) contains members that do not appear within the target (their URIs do not match), those members are added to the target.
- For each member of the Fragment contained by the target, the target member value is replaced by the value given in the Fragment. In the case of a member representing a reified Property or Relationship including a datasetId, such member is only replaced if the datasetId is the same, otherwise the member of the Fragment is added as a new instance to the target. If no datasetId is present, the default Attribute instance is targeted and replaced if present and otherwise added. In case of a member type (of an entity) in Entity Fragments, all included Entity Types are added, if they are not already contained in the type member of the target.


- For each member of the Fragment, whose value is an NGSI-LD Null, contained by the target, the target member is deleted. In the case of deleting a specific Attribute instance with a datasetId, the handling shall be in accordance with the description found in clause 5.6.5. A datasetId cannot be deleted by setting it to the

value "urn:ngsi-ld:null".

EXAMPLE 1: Given an Entity containing the following Property:

{

"temperature": { "type": "Property", "value": 25, "unitCode": "CEL" "observedAt": "2022-03-14T01:59:26.535Z"

} }

Applying partial attribute update operation (as defined in clause 5.6.4) at the Attribute level onto the temperature Attribute, with the following Attribute Fragment payload:

{

"type": "Property",
"value": 100,
"observedAt": "2022-03-14T13:00:00.000Z"

}

Results in an overwrite of the value and observedAt sub-Attributes, leaving the unitCode sub-Attribute untouched as shown:

{

"temperature": { "type": "Property", "value": 100, "unitCode": "CEL" "observedAt": "2022-03-14T13:00:00.000Z"

} }

EXAMPLE 2: Given an Entity containing the following Property:

{

"temperature": { "type": "Property", "value": 25, "unitCode": "CEL" "observedAt": "2022-03-14T01:59:26.535Z"

} }

Applying an update attributes operation (as defined in clause 5.6.2) onto the Entity as a whole

with the following Entity Fragment payload:

{

"temperature": { "type": "Property", "value": 100, "observedAt": "2022-03-14T13:00:00.000Z"

} }

Results in an overwrite of the whole temperature Attribute - other Attributes would remain untouched. The result is that the value and observedAt sub-Attributes are updated and the unitCode sub-Attribute is removed as shown:

{

"temperature": { "type": "Property", "value": 100, "observedAt": "2022-03-14T13:00:00.000Z"

} }


EXAMPLE 3: Given an Entity containing the following Property:

{

"temperature": { "type": "Property", "value": 25, "unitCode": "CEL" "observedAt": "2022-03-14T01:59:26.535Z"

}

}
Applying an update attributes operation (as defined in clause 5.6.2) onto the Entity as a whole,
with the following Entity Fragment payload:

{

"temperature": { "type": "Property", "value": "urn:ngsi-ld:null"

} }

Results in the deletion of the whole temperature Attribute - all other Attributes remain untouched.

# Related

* [Clause 5.6.2](/api-operations/provision/update-attributes.md)
* [Clause 5.6.4](/api-operations/provision/partial-attribute-update.md)
* [Clause 5.6.5](/api-operations/provision/delete-attribute.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.8](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Partial Update Patch Behaviour
