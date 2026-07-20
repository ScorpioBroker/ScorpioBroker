---
type: NGSI-LD Operation
title: "Create Entity"
description: "5.6.1.1 Description This operation allows creating a new NGSI-LD Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.1
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.1"
---

5.6.1.1 Description

This operation allows creating a new NGSI-LD Entity.

5.6.1.2 Use case diagram

A Context Producer can create an Entity within an NGSI-LD system as shown in Figure 5.6.1.2-1.

Figure 5.6.1.2-1: Create entity use case

5.6.1.3 Input data

A JSON-LD document representing an NGSI-LD Entity as mandated by clause 5.2.4.

5.6.1.4 Behaviour

Implementations shall exhibit the following behaviour:

- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation.
- If an exclusive Context Source Registration already exists for this Entity id (URI), Attributes from matching input data are forwarded for remote processing: - For matching Registrations where the Create Entity operation is supported, the operation is forwarded to the registration endpoint. If the endpoint then raises an error, this shall result in an error in case the complete create failed or in a partial success if some parts of the create succeeded. - For matching Registrations where the Create Entity operation is not supported, this shall result in an error of type Conflict if the complete Create Entity operation failed or in a partial success if some parts of it succeeded. The matching Attributes are then removed from the Fragment and not processed further.


- If any redirect Context Source Registrations exist that match against the input data, that input data is forwarded for remote processing by one or more matching endpoints: - For matching Registrations where the Create Entity operation is supported, matching input data is forwarded. If any such endpoint then raises an error, this shall result in an error in case the complete create has failed or in a partial success if some parts of the create has succeeded. - For matching redirect Registrations where the Create Entity operation is not supported, this shall result in an error of type Conflict if the complete Create Entity operation failed or in a partial success if some parts of it succeeded. The matching Attributes are then removed from the Fragment and not processed further.
- For any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded for remote processing by matching endpoints in case the Create Entity operation is supported.
- If the Entity already exists locally this shall result in an error of type AlreadyExists, if the complete Create Entity operation has failed or in a partial success if some parts of it has succeeded.
- Any remaining input data shall be used to create the Entity locally.

5.6.1.5 Output data

A URI identifying the newly created Entity.

# Related

* [HTTP: Resource: entities/](/http-binding/resource-entities.md)
* [Clause 5.2.4](/api-operations/data-types/entity.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Create Entity
