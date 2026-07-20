---
type: NGSI-LD Operation
title: "Append Attributes"
description: "5.6.3.1 Description This operation allows modifying an NGSI-LD Entity by adding new attributes (Properties or Relationships)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.3"
---

5.6.3.1 Description

This operation allows modifying an NGSI-LD Entity by adding new attributes (Properties or Relationships).

5.6.3.2 Use case diagram

A Context Producer can append new Attributes to an existing Entity within an NGSI-LD system as shown in Figure 5.6.3.2-1.

Figure 5.6.3.2-1: Append Attributes use case


5.6.3.3 Input data

- A URI representing the id of the E to be modified (target Entity).
- A selector of Entity types as specified by clause 4.17 (optional).
- A JSON-LD document representing an NGSI-LD Entity Fragment.
- An optional flag indicating whether overwriting existing Attributes within the append operation should be permitted or denied. By default, Attribute overwrites are permitted.

5.6.3.4 Behaviour

The following behaviour shall be exhibited by compliant implementations:

- If the Entity ID is not present or it is not a valid URI then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about this Entity, because there is no existing Entity which id (URI), and where specified type, is equivalent held locally to the one passed as a parameter, and no matching registrations apply (see clause 5.12), an error of type ResourceNotFound shall be raised.
- The behaviour defined in clause 5.5.4 on JSON-LD validation.
- If an exclusive or redirect Context Source Registration matches against the input data, the Attributes from matching input data are forwarded for remote processing. For each matching registration: - If the Append Attributes operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Append Attributes operation is not supported by the registration, this shall result in an error of type Conflict if the complete append failed or in a partial success if some parts of the append succeeded.
- The matching Attributes are then removed from the Fragment and not processed further.
- For any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded for remote processing to matching endpoints in case the Append Attributes operation is supported.
- Then, implementations shall perform an Append Attributes operation over the remains of the target Entity as using the following procedure: - For each Attribute (Property or Relationship) included by the Entity Fragment at root level: - If a datasetId is present in the Attribute included by the Entity Fragment: - If no Attribute instance of the same target Entity exists that has the same datasetId, then such an Attribute shall be appended to the target Entity. - If an Attribute instance of the same target Entity exists that has the same datasetId: - If overwrite is allowed, then the existing Attribute with the specified datasetId in the target Entity shall be replaced by the new one supplied. The system generated createdAt Temporal Property as defined in clause 4.8 shall remain unchanged. - If overwrite is not allowed, the existing Attribute with the specified datasetId in the target Entity shall be left untouched. - If no datasetId is present in the Attribute included by the Entity Fragment: - If no default Attribute instance of the same target Entity exists, then such Attribute shall be appended to the target Entity. - If a default Attribute instance of the same target Entity exists:


- If overwrite is allowed, then the existing default Attribute in the target Entity shall be
replaced by the new one supplied. The system generated createdAt Temporal Property
as defined in clause 4.8 shall remain unchanged.
- If overwrite is not allowed the existing default Attribute in the target Entity shall be left
untouched.
- If type is included in the Fragment and it includes Entity Type names that are not yet in the target Entity,
add them to the list of Entity Type names of the target Entity.
- If scope is included in the Fragment and overwrite is allowed, the scope of the target Entity will become
the one included in the Fragment. Otherwise, the Scopes in the Fragment that are not part of the value of
scope of the target Entity will be appended to the value of the scope of the target Entity. If there is more
than one Scope, the value of scope is represented as a JSON array containing all Scopes.

5.6.3.5 Output data

- A status code indicating whether all the new Attributes were appended or only some of them.
- List of Attributes (Properties and/or Relationships) actually appended.

# Related

* [HTTP: Resource: entities/{entityId}/attrs/](/http-binding/resource-entities-entityid-attrs.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Append Attributes
