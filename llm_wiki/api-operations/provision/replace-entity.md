---
type: NGSI-LD Operation
title: "Replace Entity"
description: "5.6.18.1 Description This operation allows the modification of an existing NGSI-LD Entity by replacing all of the Attributes (Properties or Relationships)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.18
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.18"
---

5.6.18.1 Description

This operation allows the modification of an existing NGSI-LD Entity by replacing all of the Attributes (Properties or Relationships).

5.6.18.2 Use case diagram

A Context Producer can replace an entire Entity within an NGSI-LD system as shown in Figure 5.6.18.2-1.


Figure 5.6.18.2-1: Replace Entity use case

5.6.18.3 Input data

- A URI representing the id of the Entity to be replaced (target Entity).
- A selector of Entity types as specified by clause 4.17 (optional).
- A JSON-LD document representing an NGSI-LD Entity.

5.6.18.4 Behaviour

- If the target Entity ID is not a valid URI or it is not present, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent held locally, and no matching registrations apply (see clause 5.12), then an error of type ResourceNotFound shall be raised.
- The behaviour defined in clause 5.5.4 on JSON-LD validation. NGSI-LD Nulls are not supported by this operation.
- If an exclusive or redirect Context Source Registration matches against the input data, Attributes from matching input data are forwarded. For each matching registration: - If the Replace Entity operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Replace Entity operation is not supported by the registration, this shall result in an error of type Conflict in case the complete Replace Entity operation failed, or in a partial success if some parts of it succeeded.
- The matching Attributes are then removed from the Fragment and not processed further.
- For any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded in the case the Replace Entity operation is supported for remote processing to matching endpoints.
- If the target Entity exists locally, completely replace the existing Entity with the same Entity ID with the new Entity content provided. The system generated createdAt Temporal Property of the Entity as defined in clause 4.8 remain unchanged.


5.6.18.5 Output data

None.

# Related

* [HTTP: Resource: entities/{entityId}](/http-binding/resource-entities-entityid.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.18](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Replace Entity
