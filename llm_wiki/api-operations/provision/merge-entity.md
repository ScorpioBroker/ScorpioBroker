---
type: NGSI-LD Operation
title: "Merge Entity"
description: "5.6.17.1 Description This operation allows modification of an existing NGSI-LD Entity aligning to the JSON Merge Patch processing rules defined in IETF RFC 7396 [16] by adding new Attributes (Properti"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.17
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.17"
---

5.6.17.1 Description

This operation allows modification of an existing NGSI-LD Entity aligning to the JSON Merge Patch processing rules defined in IETF RFC 7396 [16] by adding new Attributes (Properties or Relationships) or modifying or deleting existing Attributes associated with an existing Entity.

5.6.17.2 Use case diagram

A Context Producer can perform a merge on an Entity within an NGSI-LD system as shown in Figure 5.6.17.2-1.

Figure 5.6.17.2-1: Merge Entity use case


5.6.17.3 Input data

- A URI representing the id of the Entity to be merged (target Entity).
- A selector of Entity types as specified by clause 4.17 (optional).
- A JSON-LD document representing an NGSI-LD Entity Fragment.
- An optional flag indicating whether the JSON-LD document contains a simplified representation of the entity.
- An optional parameter indicating a common observedAt timestamp to use across merged Attributes.
- An optional parameter representing a common IETF RFC 5646 [28] language tag to use across merged LanguageMap Attributes.

5.6.17.4 Behaviour

The following behaviour shall be exhibited by compliant implementations:

- If the Entity ID is not present or it is not a valid URI then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent held locally, and no matching registrations apply (see clause 5.12), then an error of type ResourceNotFound shall be raised.
- If an exclusive or redirect Context Source Registration matches against the input data, Attributes from matching input data are forwarded. For each matching registration: - If the Merge Entity operation is supported by the matched registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Merge Entity operation is not supported by the matched registration, this shall result in an error of type Conflict in case the complete Merge Entity operation failed, or in a partial success if some parts of it succeeded. The matching Attributes are then removed from the Fragment and not processed further.
- For any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded in the case the Merge Entity operation is supported for remote processing to matching endpoints.
- The behaviour defined in clause 5.5.4 on JSON-LD validation. NGSI-LD Nulls should be supported by this operation. If NGSI-LD Nulls are found in the payload, but are not supported, an error of type OperationNotSupported shall be raised. Then, implementations shall perform a merge operation over the target Entity as mandated by clause 5.5.12, using the following procedure: For each Attribute (Property or Relationship) included by the Entity Fragment: - If the target Entity does not include a matching Attribute (considering term expansion rules as mandated by clause 5.5.7), then such Attribute shall be appended to the target Entity. - If the target Entity already includes a matching Attribute (considering term expansion rules as mandated by clause 5.5.7): - If the Attribute (Property or Relationship) to be merged is represented in a simplified representation, the type of any pre-existing Attribute in the target entity shall be preserved. - If a common language tag is defined and a LanguageProperty Attribute to be merged is represented as a string, the pre-existing languageMap JSON object shall be preserved. The string value shall only replace the value associated to the language tag key found within the languageMap.


- If a common observedAt timestamp is defined and an existing Attribute to be merged previously
contained an observedAt sub-Attribute, the observedAt sub-Attribute is also updated using the
common timestamp, unless the Entity Fragment itself contains an explicit updated value for the
observedAt sub-Attribute.
- If a datasetId is present in the Attribute included by the Entity Fragment:
- If an Attribute instance in the target Entity has the same datasetId:
- If overwrite is allowed and the Attribute value is not NGSI-LD Null, then the existing
Attribute with the specified datasetId in the target Entity shall be merged with the new
one supplied.
- If overwrite is allowed and the Attribute value is NGSI-LD Null, then the existing
Attribute with the specified datasetId in the target Entity shall be deleted.
- If overwrite is not allowed, the existing Attribute with the specified datasetId in the
target Entity shall be left untouched.
- Otherwise the Attribute instance with the specified datasetId shall be appended to the target
Entity.
- If no datasetId is present in the Attribute included by the Entity Fragment, the default Attribute
instance is targeted:
- If the default Attribute instance is present:
- If overwrite is allowed and the Attribute value is not NGSI-LD Null, then the existing
Attribute in the target Entity shall be merged with the new one supplied.
- If overwrite is allowed and the Attribute value is NGSI-LD Null, then the existing
Attribute with the specified datasetId in the target Entity shall be deleted.
- If overwrite is not allowed, the existing Attribute in the target Entity shall be left
untouched.
- Otherwise if value is not NGSI-LD Null, the default Attribute instance shall be appended to
the target Entity.
- If type is included in the Fragment and it includes Entity Type names that are not yet in the target Entity, add them to the list of Entity Type names of the target Entity.
- If scope is included in the Fragment and overwrite is allowed, the scope of the target Entity will become the one included in the Fragment. Otherwise, the Scopes in the Fragment that are not part of the value of scope of the target Entity will be appended to the value of the scope of the target Entity. If there is more than one Scope, the value of scope is represented as a JSON array containing all Scopes.

5.6.17.5 Output data

- A status code indicating whether all the Attributes were merged successfully.
- List of Attributes (Properties and/or Relationships) actually merged.

# Related

* [HTTP: Resource: entities/{entityId}](/http-binding/resource-entities-entityid.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
* [Clause 5.5.12](/api-operations/common-behaviours/merge-patch-behaviour.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.17](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Merge Entity
