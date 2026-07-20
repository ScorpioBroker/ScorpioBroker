---
type: NGSI-LD Operation
title: "Modify Attribute instance in Temporal Evolution of an Entity"
description: "5.6.14.1 Description This operation allows modifying a specific Attribute (Property or Relationship) instance, identified by its instanceId, of the Temporal Evolution of an Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.14
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.14"
---

5.6.14.1 Description

This operation allows modifying a specific Attribute (Property or Relationship) instance, identified by its instanceId, of
the Temporal Evolution of an Entity.
This operation enables the correction of wrong information that could have been previously added to the Temporal
Evolution of an Entity.


5.6.14.2 Use case diagram

A Context Producer can modify a specific Attribute instance, identified by a given instanceId, of the Temporal Evolution of an Entity within an NGSI-LD system as shown in Figure 5.6.14.2-1.

Figure 5.6.14.2-1: Modify Attribute Instance in Temporal Evolution of an Entity use case

5.6.14.3 Input data

- Entity ID (URI) of the target Temporal Evolution of an Entity to be modified by changing an instance of the target Attribute.
- Target Attribute (Property or Relationship) to be modified, identified by a name.
- Attribute instance to be modified, identified by its instanceId.
- A JSON-LD document representing an NGSI-LD Fragment of EntityTemporal, including only the new Attribute instance, contained by an Array of exactly one item.
- An optional JSON-LD @context.

5.6.14.4 Behaviour

- If the target Entity ID is not a valid URI or it is not present, then an error of type BadRequestData shall be raised.
- If the target Attribute name is not a valid name or it is not present, then an error of type BadRequestData shall be raised.
- If the target instanceId is not a valid URI or it is not present, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI) is equivalent held locally and no matching registrations apply, then an error of type ResourceNotFound shall be raised.
- If an exclusive or redirect Context Source Registration matches against the input data, the input data is forwarded. For each matching registration: - If the "Modify Attribute instance in Temporal Evolution of an Entity" operation is supported by the matched registration, matching input data is forwarded to the Registration endpoint.

Context Producer

Modify Attribute instance in Temporal Evolution of an Entity

NGSI-LD Client

NGSI-LD System modified

1..*

modify attribute instance in temporal evolution of an entity (entityId, attributeId, instanceId)

EntityTemporal Fragment


- If the "Modify Attribute instance in Temporal Evolution of an Entity" operation is not supported by the
matched registration, this shall result in an error of type Conflict in case the complete Modify Attribute
instance in Temporal Evolution of an Entity operation failed, or in a partial success if some
parts of it succeeded.
No further processing is required.
- For any inclusive Context Source Registrations that match against the input data, that input data is also forwarded for remote processing to matching endpoints.
- If the target Temporal Evolution of an Entity exists locally, implementations shall do the following: - Apply term expansion as mandated by clause 5.5.7 so that the fully qualified name (URI) associated to the target Attribute is properly obtained. - If the target Temporal Evolution of an Entity does not contain the target Attribute then an error of type ResourceNotFound shall be raised. - If for the target Attribute no instance with the specified instanceId exists, an error of type ResourceNotFound shall be raised.
- Replace the target Attribute instance identified by the instanceId with the Attribute instance in the EntityTemporal Fragment. The createdAt property of the concerned instance shall remain unchanged, but the modifiedAt property shall be set to the timestamp corresponding to this modification.

5.6.14.5 Output data

None.

# Related

* [HTTP: Resource: temporal/entities/{entityId}/attrs/{attrId}/ {instanceId}](/http-binding/resource-temporal-entities-entityid-attrs-attrid-instanceid.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.14](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Modify Attribute instance in Temporal Evolution of an Entity
