---
type: NGSI-LD Operation
title: "Delete Entity"
description: "5.6.6.1 Description This operation allows deleting an NGSI-LD Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.6
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.6"
---

5.6.6.1 Description

This operation allows deleting an NGSI-LD Entity.

5.6.6.2 Use case diagram

A Context Producer can completely delete an Entity within an NGSI-LD system as shown in Figure 5.6.6.2-1.

Figure 5.6.6.2-1: Delete Entity use case


5.6.6.3 Input data

- Entity ID (URI) of the Entity to be deleted, the target Entity.
- A selector of Entity types as specified by clause 4.17 (optional).

5.6.6.4 Behaviour

- If the target Entity ID is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent held locally, and no matching registrations apply (see clause 5.12), then an error of type ResourceNotFound shall be raised.
- If an exclusive or redirect Context Source Registration matches against the id, the request is forwarded for remote processing. For each matching registration: - If the Delete Entity operation is supported by the registration (see clause 4.3.6), the request is forwarded to the Registration endpoint. - If the Delete Entity update operation is not supported by the matched registration, this shall result in an error of type Conflict in case the complete delete Entity failed, or in a partial success if some parts of the delete Entity succeeded.
- For any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded in the case the Delete Entity operation is supported for remote processing by matching endpoints.
- The input data shall be used to remove the entity locally if it exists.

5.6.6.5 Output data

None.

# Related

* [HTTP: Resource: entities/{entityId}](/http-binding/resource-entities-entityid.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Delete Entity
