---
type: NGSI-LD Operation
title: "Create or Update (Upsert) Temporal Evolution of an Entity"
description: "5.6.11.1 Description This operation allows creating or updating (by adding new Attribute instances) the Temporal Evolution of an Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.11
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.11"
---

5.6.11.1 Description

This operation allows creating or updating (by adding new Attribute instances) the Temporal Evolution of an Entity.

5.6.11.2 Use case diagram

A Context Producer can create the Temporal Evolution of an Entity within an NGSI-LD system as
shown in Figure 5.6.11.2-1.
Similarly, if the Entity already exists then an Update scenario will be in place.

Figure 5.6.11.2-1: Create or Update (Upsert) Temporal Evolution of an Entity use case

5.6.11.3 Input data

A JSON-LD document representing the Temporal Evolution of an Entity as mandated by clause 5.2.20.

5.6.11.4 Behaviour

Implementations shall exhibit the following behaviour:

- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation.
- If an exclusive Context Source Registration already exists for this Entity id (URI), Attributes from matching input data are forwarded for remote processing: - For matching Registrations where the "Create or Update (Upsert) Temporal Evolution of an Entity" operation is supported, the operation is forwarded to the registration endpoint. If the endpoint then raises an error, this shall result in an error in case the complete "Create or Update (Upsert) Temporal Evolution of an Entity" operation failed or in a partial success if some parts of it succeeded.


- For matching Registrations where the "Create Entity" operation is not supported, this shall result in an
error of type Conflict in case the complete "Create or Update (Upsert) Temporal Evolution of an Entity"
operation failed or in a partial success if some parts of it succeeded.
The matching Attributes are then removed from the Fragment and not processed further.
- If any redirect Context Source Registrations exist that match against the input data, that input data is forwarded for remote processing by one or more matching endpoints: - For matching Registrations where the "Create or Update (Upsert) Temporal Evolution of an Entity" operation is supported, matching input data is forwarded. If any such endpoint then raises an error, this shall result in an error in case the complete "Create or Update (Upsert) Temporal Evolution of an Entity" operation failed or in a partial success if some parts of it succeeded. - For matching redirect Registrations where the "Create or Update (Upsert) Temporal Evolution of an Entity" operation is not supported, this shall result in an error of type Conflict in case the complete "Create or Update (Upsert) Temporal Evolution of an Entity" operation failed or in a partial success if some parts of it succeeded. The matching Attributes are then removed from the Fragment and not processed further.
- For any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded for remote processing by matching endpoints.
- If the NGSI-LD endpoint already knows about this Temporal Evolution of an Entity, because there is an existing Temporal Evolution of an Entity whose id (URI) is the same, then all the Attribute instances included by the Temporal Evolution shall be added to the existing Entity as mandated by clause 5.6.12. If type is included in the EntityTemporal Fragment and it includes Entity Type names that are not yet in the target Temporal Evolution of an Entity, add them to the list of Entity Type names of the target Temporal Evolution of an Entity.
- Otherwise, implementations shall create the provided Temporal Evolution of an Entity.

5.6.11.5 Output data

On creation, a URI identifying the newly created Temporal Evolution of an Entity. None otherwise.

# Related

* [HTTP: Resource: temporal/entities/](/http-binding/resource-temporal-entities.md)
* [Clause 5.2.20](/api-operations/data-types/entitytemporal.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.6.12](/api-operations/provision/add-attributes-to-temporal-evolution-of-an-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.11](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Create or Update (Upsert) Temporal Evolution of an Entity
