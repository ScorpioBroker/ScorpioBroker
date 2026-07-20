---
type: NGSI-LD Operation
title: "Replace Attribute"
description: "5.6.19.1 Description This operation allows the replacement of a single Attribute (Property or Relationship) within an NGSI-LD Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.19
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.19"
---

5.6.19.1 Description

This operation allows the replacement of a single Attribute (Property or Relationship) within an NGSI-LD Entity.

5.6.19.2 Use case diagram

A Context Producer can carry out a replacement of an Attribute within an Entity within an NGSI-LD System as shown in Figure 5.6.19.2-1.

Figure 5.6.19.2-1: Replace Attribute use case

5.6.19.3 Input data

- Entity ID (URI) of the concerned Entity, the target Entity.
- A selector of Entity types as specified by clause 4.17 (optional).
- Target Attribute (Property or Relationship) to be replaced, identified by a name.
- A JSON-LD document representing an NGSI-LD Attribute Fragment.

5.6.19.4 Behaviour

- If the target Entity ID is not a valid URI, then an error of type BadRequestData shall be raised.
- If the target Attribute name is not valid, then an error of type BadRequestData shall be raised.
- If the target Attribute is scope, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent held locally, and no matching registrations apply (see clause 5.12), then an error of type ResourceNotFound shall be raised.
- The behaviour defined in clause 5.5.4 on JSON-LD validation.


- If an exclusive or redirect Context Source Registration matches against the input data, the input data is forwarded. For each matching registration: - If the Replace Attribute operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Replace Attribute operation is not supported by the registration, this shall result in an error of type Conflict in case the complete Replace Attribute operation failed, or in a partial success if some parts of it succeeded. No further processing is required.
- For any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded in the case the Replace Attribute operation is supported for remote processing to matching endpoints.
- Apply term expansion as mandated by clause 5.5.7, so that the fully qualified name (URI) associated to the target Attribute is properly obtained.
- If the target Entity does not contain the target Attribute: - as a default instance in case no datasetId is present; - as an instance with the specified datasetId if present; - then this shall result in an error of type ResourceNotFound in case the complete Replace Attribute operation failed, or in a partial success if some parts of it succeeded.
- Completely replace the existing Attribute instance with the new Attribute content provided. The system generated createdAt Temporal Property as defined in clause 4.8 remains unchanged.

5.6.19.5 Output data

None.

# Related

* [HTTP: Resource: entities/{entityId}/attrs/{attrId}](/http-binding/resource-entities-entityid-attrs-attrid.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.19](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Replace Attribute
