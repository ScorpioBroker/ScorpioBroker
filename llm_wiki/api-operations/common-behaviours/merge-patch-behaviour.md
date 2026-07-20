---
type: NGSI-LD Clause
title: "Merge Patch Behaviour"
description: "The merge patch procedure modifies an existing NGSI-LD element by applying the set of changes found in an NGSI-LD Fragment data to the target resource."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.12
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.12"
---

The merge patch procedure modifies an existing NGSI-LD element by applying the set of changes found in an
NGSI-LD Fragment data to the target resource. Unlike the partial update patch behaviour (described in clause 5.5.8),
which replaces the complete element on the first level, e.g. a whole Attribute, the procedure described in this clause
merges the provided information with the existing information up to an arbitrary depth, e.g. including going into JSON
objects representing a Property value.
When merging NGSI-LD Entities using NGSI-LD Fragments, implementations shall determine the exact set of changes
being requested by comparing the content of the provided Fragment (patch) against the current content (a JSON-LD
object) of the target element.
With respect to merge operations, implementations shall perform an algorithm equivalent to the one described below
(adapted from IETF RFC 7396 [16], in order to observe the name to URI expansion rules and JSON-LD null
processing):
- For each member of the Fragment perform the term to URI expansion.
- If the provided Fragment (a JSON Merge Patch document) contains members that do not appear within the target (their URIs do not match), those members are added to the target.
- For each member of the Fragment contained by the target, the target member value is merged with the value given in the Fragment. NGSI-LD Nulls within the Fragment are given special meaning to indicate the removal of existing values within the target member value. In the case of a member representing a reified Property or Relationship including a datasetId, such member is only updated if the datasetId is the same, otherwise the member of the Fragment is added as a new instance to the target. If no datasetId is present, the default Attribute instance is targeted and merged if present and otherwise added. In case of a member type (of an Entity) in Entity Fragments, all included Entity Types are added, if they are not already contained in the type member of the target.
- For each member of the Fragment, whose value is an NGSI-LD Null, contained by the target, the target member is removed. In the case of deleting a specific Attribute instance with a datasetId, the handling shall be according to the description in clause 5.6.5. A datasetId cannot be deleted by setting it to the value "urn:ngsi-ld:null".

EXAMPLE 1: Given an Entity containing the following Property: {

"temperature": { "type" : "Property", "value" : 25, "unitCode": "CEL" "observedAt": "2022-03-14T01:59:26.535Z"

} }

Applying a merge entity operation (as defined in clause 5.6.17) onto the Entity as a whole, with the following Entity Fragment payload:

{

"temperature": { "type" : "Property", "value" : 100, "observedAt": "2022-03-14T13:00:00.000Z"

}

}

Results in the update of the value and observedAt sub-Attributes and leaves the unitCode sub-Attribute untouched, as shown:

{

| | | | "type" : "Property", | |
| --- | --- | --- | --- | --- |
| | | | "value" : 100, | |
| | | | "unitCode": "CEL", | |
| | | | "observedAt": "2022-03-14T13:00:00.000Z" | |
| | } | | | |

"temperature": {

} EXAMPLE 2: Given an Entity containing the following Property: {

"address": { "type" : "Property", "value" : { "street": "Straße des 17. Juni", "city": "Berlin", "country": "Germany"

} }

}
Applying a merge entity operation (as defined in clause 5.6.17) onto the Entity as a whole with
the following Entity Fragment payload:
{

"address": { "type" : "Property", "value" : { "street": "Pariser Platz", "country": "urn:ngsi-ld:null"

} }

}
Results in the updating of the address Attribute value applying the JSON Merge Patch processing
rules as defined in IETF RFC 7396 [16], updating street and removing country resulting as shown:
{

"address": { "type" : "Property", "value": { "street": "Pariser Platz", "city": "Berlin"

}

}

# Related

* [Merge Entity](/api-operations/provision/merge-entity.md)
* [Partial Update Patch](/api-operations/common-behaviours/partial-update-patch-behaviour.md)
* [Clause 5.5.8](/api-operations/common-behaviours/partial-update-patch-behaviour.md)
* [Clause 5.6.17](/api-operations/provision/merge-entity.md)
* [Clause 5.6.5](/api-operations/provision/delete-attribute.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.12](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Merge Patch Behaviour
