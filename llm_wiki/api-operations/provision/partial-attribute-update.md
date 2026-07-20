---
type: NGSI-LD Operation
title: "Partial Attribute update"
description: "5.6.4.1 Description This operation allows performing a partial update on an NGSI-LD Entity's Attribute (Property or Relationship)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.4"
---

5.6.4.1 Description

This operation allows performing a partial update on an NGSI-LD Entity's Attribute (Property or Relationship). A partial update only changes the elements provided in an Entity Fragment, leaving the rest as they are. This operation supports the deletion of sub-Attributes but not the deletion of the whole Attribute itself.

5.6.4.2 Use case diagram

A Context Producer can carry out a partial Attribute update of an Entity within an NGSI-LD System as shown in Figure 5.6.4.2-1.

Figure 5.6.4.2-1: Partial Attribute update use case

5.6.4.3 Input data

- Entity ID (URI) of the concerned Entity, the target Entity.
- A selector of Entity types as specified by clause 4.17 (optional).


- Target Attribute (Property or Relationship) to be modified, identified by a name.
- A JSON-LD document representing an NGSI-LD Attribute Fragment.

5.6.4.4 Behaviour

- If the target Entity ID is not a valid URI or it is not present, then an error of type BadRequestData shall be raised.
- If the target Attribute name is not valid or it is not present, then an error of type BadRequestData shall be raised.
- If the target Attribute is scope, then an error of type BadRequestData shall be raised.
- The behaviour defined in clause 5.5.4 on JSON-LD validation. NGSI-LD Nulls should be supported by this operation. If NGSI-LD Nulls are found in the payload, but are not supported, an error of type OperationNotSupported shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent held locally, and no matching registrations apply (see clause 5.12), then an error of type ResourceNotFound shall be raised.
- If an exclusive or redirect Context Source Registration matches against the input data, the Attributes from matching input data are forwarded for remote processing. For each matching registration: - If the Partial Attribute update operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Partial Attribute update operation is not supported by the registration, this shall result in an error of type Conflict in case the complete partial Attribute update failed, or in a partial success if some parts of the partial Attribute update succeeded. No further processing is required.
- For any inclusive Context Source Registrations that match against the input data, that input data is also forwarded for remote processing to matching endpoints in case the Partial Attribute update operation is supported.
- Apply term expansion as mandated by clause 5.5.7, so that the fully qualified name (URI) associated to the target Attribute is properly obtained.
- If the target Entity does not contain the target Attribute: - as a default instance in case no datasetId is present; - as an instance with the specified datasetId if present; then an error of type ResourceNotFound shall be raised.
- Perform a partial update patch operation on the target Attribute following the algorithm mandated by clause 5.5.8. If present in the provided NGSI-LD Entity Fragment, the type of the Attribute has to be the same as the type of the targeted Attribute fragment, i.e. it is not allowed to change the type of an Attribute.

5.6.4.5 Output data

None.

# Related

* [HTTP: Resource: entities/{entityId}/attrs/{attrId}](/http-binding/resource-entities-entityid-attrs-attrid.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
* [Clause 5.5.8](/api-operations/common-behaviours/partial-update-patch-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Partial Attribute update
