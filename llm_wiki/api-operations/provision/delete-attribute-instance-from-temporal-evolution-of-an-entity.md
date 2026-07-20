---
type: NGSI-LD Operation
title: "Delete Attribute instance from Temporal Evolution of an Entity"
description: "5.6.15.1 Description This operation allows deleting one Attribute instance (Property or Relationship), identified by its instanceId, of the Temporal Evolution of an Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.15
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.15"
---

5.6.15.1 Description

This operation allows deleting one Attribute instance (Property or Relationship), identified by its instanceId, of the Temporal Evolution of an Entity. The Attribute itself and all its child elements shall be deleted. This operation enables the removal of individual Attribute instances that could have been previously added to the Temporal Evolution of an Entity.

5.6.15.2 Use case diagram

A Context Producer can delete an Attribute instance, identified by a given instanceId, of the Temporal Evolution of an Entity within an NGSI-LD system as shown in Figure 5.6.15.2-1.


Figure 5.6.15.2-1: Delete Attribute Instance from Temporal Evolution of an Entity use case

5.6.15.3 Input data

- Entity ID (URI) of the target Temporal Evolution of an Entity to be modified by deleting an instance of the target Attribute.
- Target Attribute (Property or Relationship) to be deleted, identified by a name.
- Attribute instance to be deleted, identified by its instanceId.
- An optional JSON-LD @context.

5.6.15.4 Behaviour

- If the target Entity ID is not a valid URI or it is not present, then an error of type BadRequestData shall be raised.
- If the target Attribute name is not a valid name or it is not present, then an error of type BadRequestData shall be raised.
- If the target instanceId is not a valid URI or it is not present, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI) is equivalent held locally and no matching registrations apply, then an error of type ResourceNotFound shall be raised.
- If an exclusive or redirect Context Source Registration matches against the input data, the input data is forwarded. For each matching registration: - If the "Delete Attribute instance from Temporal Evolution of an Entity" operation is supported by the matched registration, matching input data is forwarded to the Registration endpoint. - If the "Delete Attribute instance from Temporal Evolution of an Entity" operation is not supported by the matched registration, this shall result in an error of type Conflict in case the complete "Delete Attribute instance from Temporal Evolution of an Entity" failed, or in a partial success if some parts of it succeeded. No further processing is required.
- For any inclusive Context Source Registrations that match against the input data, that input data is also forwarded for remote processing to matching endpoints.

Context Producer

Delete Attribute instance from Temporal Evolution of an Entity

NGSI-LD Client

NGSI-LD System deleted

1..*

delete attribute instance from temporal evolution of an entity (entityId, attributeId, instanceId)


- If the target Temporal Evolution of an Entity exists locally, implementations shall do the following: - Apply term expansion as mandated by clause 5.5.7 so that the fully qualified name (URI) associated to the target Attribute is properly obtained.
- If the target Temporal Evolution of the Entity does not contain the target Attribute then an error of type ResourceNotFound shall be raised.
- If for the target Attribute no instance with the specified instanceId exists, an error of type ResourceNotFound shall be raised.
- Remove the instance, with the specified instanceId, of the target Attribute from the target Entity.

5.6.15.5 Output data

None.

# Related

* [HTTP: Resource: temporal/entities/{entityId}/attrs/{attrId}/ {instanceId}](/http-binding/resource-temporal-entities-entityid-attrs-attrid-instanceid.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.15](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Delete Attribute instance from Temporal Evolution of an Entity
