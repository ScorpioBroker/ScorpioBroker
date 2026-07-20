---
type: NGSI-LD Operation
title: "Batch Entity Creation"
description: "5.6.7.1 Description This operation allows creating a batch of NGSI-LD Entities."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.7
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.7"
---

5.6.7.1 Description

This operation allows creating a batch of NGSI-LD Entities.

5.6.7.2 Use case diagram

A Context Producer can create a batch of NGSI-LD Entities within an NGSI-LD system as shown in Figure 5.6.7.2-1.


Figure 5.6.7.2-1: Create a batch of Entities use case

5.6.7.3 Input data

- A JSON-LD Array containing one or more JSON-LD documents each one representing an NGSI-LD Entity as mandated by clause 5.2.4. See clause 5.5.11.1 for information on behaviour when there is more than one instance of the same entity in the input Array.

5.6.7.4 Behaviour

Implementations shall exhibit the following behaviour:

- If the input Array is empty or contains a null value in any of its items an error of type BadRequestData shall be raised.
- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation.
- Let S be an array which shall contain a list of Entity IDs, one for each NGSI-LD Entity successfully created. S shall be initialized to the empty array.
- Let E be an array which shall contain a list of BatchEntityError as defined by clause 5.2.17, one for each NGSI-LD Entity which resulted in error. E shall be initialized to the empty array.
- For each Context Source Registration CSR in the Context Registry: - Let IN be a copy of the original input array. - Remove from IN all Entities not matched by CSR and remove non-matching Attributes from the remaining Entities. - Remove all Attributes from the remaining Entities in IN for which there is a matching exclusive Context Source Registration, which is not CSR itself. - Remove all Attributes from the remaining Entities in IN for which there is a matching redirect Context Source Registration, unless CSR is a redirect Context Source Registration itself. - If IN is empty, continue with the next Context Source Registration if there is any. - If the Batch Entity Creation operation is supported by CSR: - Forward the Batch Entity Creation request with IN as input Array.

Context Producer

Batch Entity Creation

NGSI-LD Client

Batch Entity Creation

NGSI-LD System response

1..*

Entity Array

S: Array E: Array


- Merge the returned list of Entities successfully created with S.
- Merge the returned list of Entities in Error with E.
- Otherwise, if the Create Entity operation (clause 5.6.1) is supported by CSR:
- For each Entity EN in the input array:
- Forward a Create Entity request for Entity EN.
- Merge any successful result(s) for Entity EN created with S.
- Merge any error result(s) for Entity EN created with E.
- Otherwise:
- In case CSR is an exclusive or redirect Context Source Registration, add an Error of
type Conflict for each Entity in IN to E.
- For each of the NGSI-LD Entities included in the input Array execute the behaviour defined by clause 5.6.1, but limited to a local operation, as follows: - If the Entity was successfully created, then add the corresponding Entity ID to the S array. - If the Entity creation failed, then a new BatchEntityError shall be added to E containing the failed Entity ID and the related ProblemDetails.

5.6.7.5 Output data

- The list of Entities successfully created (S Array), if all Entities were created correctly; or
- the list of Entities successfully created (S Array) and the list of Entities in error (E Array), if only some or none of the Entities were created.

# Related

* [HTTP: Resource: entityOperations/create](/http-binding/resource-entityoperations-create.md)
* [Clause 5.2.17](/api-operations/data-types/batchentityerror.md)
* [Clause 5.2.4](/api-operations/data-types/entity.md)
* [Clause 5.5.11.1](/api-operations/common-behaviours/batch-entity-creation-case.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.6.1](/api-operations/provision/create-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Batch Entity Creation
