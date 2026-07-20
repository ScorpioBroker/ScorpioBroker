---
type: NGSI-LD Operation
title: "Delete Context Source Registration"
description: "5.9.4.1 Description This operation allows deleting a Context Source Registration from an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.9.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.9.4"
---

5.9.4.1 Description

This operation allows deleting a Context Source Registration from an NGSI-LD system.

5.9.4.2 Use case diagram

A context provider can delete a context source registration from an NGSI-LD system as shown in Figure 5.9.4.2-1.

Figure 5.9.4.2-1: Delete context source registration use case

5.9.4.3 Input data

Registration identifier (URI) of the context source registration to be deleted (target registration).

5.9.4.4 Behaviour

- If the target context source registration id is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target context source registration, because there is no existing context source registration whose id (URI) is equivalent, then an error of type ResourceNotFound shall be raised.
- Otherwise the context source registration shall be removed.


5.9.4.5 Output data

None.

# Related

* [HTTP: Resource: csourceRegistrations/{registrationId}](/http-binding/resource-csourceregistrations-registrationid.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.9.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Delete Context Source Registration
