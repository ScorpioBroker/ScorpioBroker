---
type: NGSI-LD Operation
title: "Delete Attribute"
description: "5.6.5.1 Description This operation allows deleting an NGSI-LD Attribute (Property or Relationship)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.5
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.5"
---

5.6.5.1 Description

This operation allows deleting an NGSI-LD Attribute (Property or Relationship). The Attribute itself and all its children shall be deleted.


5.6.5.2 Use case diagram

A Context Producer can delete a specific Attribute within an NGSI-LD system as shown in Figure 5.6.5.2-1.

Figure 5.6.5.2-1: Delete Attribute use case

5.6.5.3 Input data

- Entity ID (URI) of the concerned Entity, the target Entity.
- A selector of Entity types as specified by clause 4.17 (optional).
- Target Attribute (Property or Relationship) to be deleted, identified by a name.
- An optional parameter identifying the datasetId of the target Attribute instance to be deleted. Otherwise the default Attribute instance is targeted.
- An optional flag deleteAll indicating whether also all target Attribute instances with a datasetId are to be deleted.
- An optional JSON-LD @context.

5.6.5.4 Behaviour

- If the target Entity ID is not a valid URI or it is not present, then an error of type BadRequestData shall be raised.
- If the target Attribute name is not a valid name or it is not present, then an error of type BadRequestData shall be raised.
- If an exclusive or redirect Context Source Registration matches against the input data, the input data (see clause 5.12), the input data is forwarded. For each matching registration: - If the Delete Attribute operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Delete Attribute update operation is not supported by the matched registration, this shall result in an error of type Conflict in case the complete delete Attribute failed, or in a partial success if some parts of the delete Attribute succeeded. No further processing is required.


- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent held locally, and no matching registrations apply, then an error of type ResourceNotFound shall be raised.
- For any inclusive Context Source Registrations that match against the input data, that input data is also forwarded for remote processing to matching endpoints in case the Delete Attribute operation is supported by the matched registration.
- Apply term expansion as mandated by clause 5.5.7 so that the fully qualified name (URI) associated to the target Attribute is properly obtained.
- If the target Entity does not contain the target Attribute then an error of type ResourceNotFound shall be raised.
- If the target Attribute is scope, remove the scope Attribute from the target Entity.
- If the deleteAll flag is set, remove all target Attribute instances from the target Entity.
- Otherwise: - if a datasetId parameter is provided, remove only the target Attribute instance from the given dataset whose datasetId matches the parameter; - if no datasetId parameter is provided, remove the default target Attribute instance from the target Entity.

5.6.5.5 Output data

None.

# Related

* [HTTP: Resource: entities/{entityId}/attrs/{attrId}](/http-binding/resource-entities-entityid-attrs-attrid.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Delete Attribute
