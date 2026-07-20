---
type: NGSI-LD Operation
title: "Update Context Source Registration"
description: "5.9.3.1 Description This operation allows updating a Context Source Registration in an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.9.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.9.3"
---

5.9.3.1 Description

This operation allows updating a Context Source Registration in an NGSI-LD system.

5.9.3.2 Use case diagram

A Context Provider can update a Context Source Registration in an NGSI-LD system as shown in Figure 5.9.3.2-1.


Figure 5.9.3.2-1: Update context source registration use case

5.9.3.3 Input data

- Context Source Registration identifier (URI), the target Context Source Registration.
- A JSON-LD document representing a Context Source Registration Fragment (clause 5.4).

5.9.3.4 Behaviour

- If the target Context Source Registration id (id) is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If a "contextSourceInfo" array is defined and the restrictions expressed by clause 4.3.6.6 are not met by the Context Source Registration, then an error of type BadRequestData shall be raised.
- If the NGSI-LD System does not know about the target Context Source Registration, because there is no existing Context Source Registration whose id (URI) is equivalent, an error of type ResourceNotFound shall be raised.
- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation.
- If the data types and restrictions expressed by clause 5.2.9 are not met by the Context Source Registration Fragment, then an error of type BadRequestData shall be raised.
- Term to URI expansion of Attribute names shall be observed as mandated by clause 5.5.7.
- If the Context Source Registration to be updated has its mode property defined as exclusive, the following additional restrictions apply: - If an exclusive or redirect Context Source Registration already matches against the Entity ID (URI) and any of the Attributes defined in the registration, an error of type Conflict shall be raised. - If an Entity already exists for the supplied Entity ID (URI) and the existing Entity contains any of the Attributes defined in the registration, an error of type Conflict shall be raised.
- If the Context Source Registration to be updated has its mode property defined as redirect, the following additional restriction applies: - If an existing Entity already matches the Context Source Registration, an error of type Conflict shall be raised.


- If the Context Source to be updated has its mode property defined as auxiliary, the following additional restriction applies: - If the operations property is not defined as one of: "retrieveOps", "retrieveEntity" or "queryEntity", an error of type BadRequestData shall be raised.
- Then, implementations shall modify the target Context Source Registration as mandated by clause 5.5.8.

5.9.3.5 Output data

None.

# Related

* [HTTP: Resource: csourceRegistrations/{registrationId}](/http-binding/resource-csourceregistrations-registrationid.md)
* [Clause 4.3.6.6](/framework/architecture/additional-pre-and-post-processing-of-extra-information-when-contacting-context-.md)
* [Clause 5.2.9](/api-operations/data-types/csourceregistration.md)
* [Clause 5.4](/api-operations/ngsi-ld-fragments.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
* [Clause 5.5.8](/api-operations/common-behaviours/partial-update-patch-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.9.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Update Context Source Registration
