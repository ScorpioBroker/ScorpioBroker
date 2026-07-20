---
type: NGSI-LD Operation
title: "Update Attributes"
description: "5.6.2.1 Description This operation allows modifying an existing NGSI-LD Entity by updating already existing Attributes (Properties or Relationships) and by appending non-existing ones."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.2
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.2"
---

5.6.2.1 Description

This operation allows modifying an existing NGSI-LD Entity by updating already existing Attributes (Properties or Relationships) and by appending non-existing ones.

5.6.2.2 Use case diagram

A Context Producer can update Attributes within an NGSI-LD system as shown in Figure 5.6.2.2-1.

Figure 5.6.2.2-1: Update Attributes use case


5.6.2.3 Input data

- A URI representing the id of the Entity to be updated (target Entity).
- A selector of Entity types as specified by clause 4.17 (optional).
- A JSON-LD document representing an NGSI-LD Entity Fragment.

5.6.2.4 Behaviour

- If the Entity ID is not present or it is not a valid URI then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent to the target entity held locally, and no matching registrations apply (see clause 5.12), an error of type ResourceNotFound shall be raised.
- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation. NGSI-LD Nulls should be supported by this operation. If NGSI-LD Nulls are found in the payload, but are not supported, an error of type OperationNotSupported shall be raised.
- If an exclusive or redirect Context Source Registration matches against the input data, Attributes from matching input data are forwarded for remote processing. For each matching registration: - If the Update Attributes operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Update Attributes operation is not supported by the registration, this shall result in an error of type Conflict if the complete update failed or in a partial success if some parts of the update succeeded.
- The matching Attributes are then removed from the Fragment and not processed further.
- If there are remaining Attributes, for any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded for remote processing to matching endpoints in case the Update Attributes operation is supported by the matched registration.
- Then, implementations shall perform a partial update patch operation over the remains of the target Entity as mandated by clause 5.5.8, using the following procedure.
- For each Attribute (Property or Relationship) included by the Entity Fragment at root level: - If the target Entity does not include a matching Attribute (considering term expansion rules as mandated by clause 5.5.7) then such Attribute shall be appended to the target Entity. - If the target Entity already includes a matching Attribute (considering term expansion rules as mandated by clause 5.5.7): - If a datasetId is present in the Attribute included by the Entity Fragment: - If an Attribute instance in the target Entity has the same datasetId and the Attribute value is not NGSI-LD Null, then the existing Attribute instance with the specified datasetId in the target Entity shall be replaced by the new one supplied. The system generated createdAt Temporal Property as defined in clause 4.8 shall remain unchanged. - If an Attribute instance in the target Entity has the same datasetId and the Attribute value is NGSI-LD Null then the existing Attribute instance with the specified datasetId in the target Entity shall be deleted. - Otherwise the Attribute instance with the specified datasetId shall be appended to the target Entity.


- If no datasetId is present in the Attribute included by the Entity Fragment, the default Attribute instance is targeted: - If the default Attribute instance is present and the Attribute value is not NGSI-LD Null, then the existing Attribute in the target Entity shall be replaced by the new one supplied. The system generated createdAt Temporal Property as defined in clause 4.8 shall remain unchanged. - If the default Attribute instance is present and the Attribute value is NGSI-LD Null, then the existing Attribute in the target Entity shall be deleted. - Otherwise the default Attribute instance shall be appended to the target Entity.

- If type is included in the Fragment and it includes Entity Type names that are not yet in the target Entity, add them to the list of Entity Type names of the target Entity.
- If scope is included in the Fragment and the target entity includes scope, replace the scope by the one included in the Fragment, otherwise ignore it.

5.6.2.5 Output data

- A status code indicating whether all the new Attributes were updated or only some of them.
- List of Attributes (Properties or Relationships) actually updated.

# Related

* [HTTP: Resource: entities/{entityId}/attrs/](/http-binding/resource-entities-entityid-attrs.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
* [Clause 5.5.8](/api-operations/common-behaviours/partial-update-patch-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Update Attributes
