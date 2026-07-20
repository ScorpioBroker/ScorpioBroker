---
type: NGSI-LD Operation
title: "Add Attributes to Temporal Evolution of an Entity"
description: "5.6.12.1 Description This operation allows modifying the Temporal Evolution of an Entity by adding new Attribute instances."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.12
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.12"
---

5.6.12.1 Description

This operation allows modifying the Temporal Evolution of an Entity by adding new Attribute instances.

5.6.12.2 Use case diagram

A Context Producer can add new Attributes or Attribute instances to an existing Temporal Evolution of an Entity within an NGSI-LD system as shown in Figure 5.6.12.2-1.


Figure 5.6.12.2-1: Add Attributes to Temporal Evolution of an Entity use case

5.6.12.3 Input data

- Entity ID (URI) of the target Temporal Evolution of an Entity to be modified with additional Attributes.
- A JSON-LD document representing an NGSI-LD Fragment of EntityTemporal, including only the new Attribute instance(s), and contained by an Array.

5.6.12.4 Behaviour

The following behaviour shall be exhibited by compliant implementations:

- If the Entity ID is not present or it is not a valid URI then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Temporal Evolution of an Entity, because there is no existing Temporal Evolution of an Entity whose id (URI) is equivalent to the one passed as a parameter held locally and no matching registrations apply, an error of type ResourceNotFound shall be raised.
- The behaviour defined in clause 5.5.4 on JSON-LD validation.
- If an exclusive or redirect Context Source Registration matches against the input data, the Attributes from matching input data are forwarded for remote processing. For each matching registration: - If the "Add Attributes to Temporal Evolution of an Entity" operation is supported by the matched registration, matching input data is forwarded to the Registration endpoint. - If the "Add Attributes to Temporal Evolution of an Entity" operation is not supported by the matched registration, this shall result in an error of type Conflict if the complete "Add Attributes to Temporal Evolution of an Entity" operation failed or in a partial success if some parts of it succeeded. The matching Attributes are then removed from the Fragment and not processed further.
- For any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded for remote processing to matching endpoints.

Context Producer

Add Attributes to Temporal Evolution of an Entity

NGSI-LD Client

NGSI-LD System added

1..*

EntityTemporal Fragment

add attributes to temporal evolution of an entity (entityId)


- If the target Temporal Evolution of an Entity exists locally and matches against the remaining input data, implementations shall do the following: - For each Attribute (Property or Relationship) instance included by the EntityTemporal Fragment at root level: - The Attribute (considering term expansion rules as mandated by clause 5.5.7) instance(s) shall be added to the target Temporal Evolution of an Entity.
- If type is included in the EntityTemporal Fragment and it includes Entity Type names that are not yet in the target Temporal Evolution of an Entity, add them to the list of Entity Type names of the target Temporal Evolution of the Entity.

5.6.12.5 Output data

None.

# Related

* [HTTP: Resource: temporal/entities/{entityId}/attrs/](/http-binding/resource-temporal-entities-entityid-attrs.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.12](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Add Attributes to Temporal Evolution of an Entity
