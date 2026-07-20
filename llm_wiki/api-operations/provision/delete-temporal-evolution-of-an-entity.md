---
type: NGSI-LD Operation
title: "Delete Temporal Evolution of an Entity"
description: "5.6.16.1 Description This operation allows deleting the Temporal Evolution of an Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.16
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.16"
---

5.6.16.1 Description

This operation allows deleting the Temporal Evolution of an Entity.

5.6.16.2 Use case diagram

A Context Producer can completely delete the Temporal Evolution of an Entity within an NGSI-LD system as shown in Figure 5.6.16.2-1.

Figure 5.6.16.2-1: Delete Temporal Evolution of an Entity use case

5.6.16.3 Input data

- Entity ID (URI) of the target Temporal Evolution of an Entity to be deleted.

5.6.16.4 Behaviour

- If the target Entity ID is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.

Context Producer

Delete Temporal Evolution of an Entity

NGSI-LD Client

NGSI-LD System deleted

1..*

delete temporal evolution of an entity (entityId)


- If the NGSI-LD endpoint does not know about the target Entity because there is no existing Entity whose id (URI) is equivalent held locally and no matching registrations apply, then an error of type ResourceNotFound shall be raised.
- If an exclusive or redirect Context Source Registration matches against the input data, the input data is forwarded. For each matching registration: - If the "Delete Temporal Evolution of Entity" operation is supported by the matched registration, matching input data is forwarded to the Registration endpoint. - If the "Delete Temporal Evolution of Entity" operation is not supported by the matched registration, this shall result in an error of type Conflict in case the complete "Delete Temporal Evolution of Entity" failed, or in a partial success if some parts of it succeeded. No further processing is required.
- For any inclusive Context Source Registrations that match against the input data, that input data is also forwarded for remote processing to matching endpoints.
- If the target Temporal Evolution of an Entity exists locally, the entire Temporal Evolution of the Entity shall be removed.

5.6.16.5 Output data

None.

# Related

* [HTTP: Resource: temporal/entities/{entityId}](/http-binding/resource-temporal-entities-entityid.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.16](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Delete Temporal Evolution of an Entity
