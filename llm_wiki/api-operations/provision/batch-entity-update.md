---
type: NGSI-LD Operation
title: "Batch Entity Update"
description: "5.6.9.1 Description This operation allows updating a batch of NGSI-LD Entities."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.9
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.9"
---

5.6.9.1 Description

This operation allows updating a batch of NGSI-LD Entities.

5.6.9.2 Use case diagram

A Context Producer can update a batch of Entities within an NGSI-LD system as shown in Figure 5.6.9.2-1.

Figure 5.6.9.2-1: Update a batch of Entities use case

5.6.9.3 Input data

- A JSON-LD Array containing one or more JSON-LD documents each one representing an Entity as mandated by clause 5.2.4. See clause 5.5.11.3 for information on behaviour when there is more than one instance of the same entity in the input Array.
- An optional flag indicating whether Attributes shall be overwritten or not. By default, Attributes will be overwritten.

Context Producer

Batch Entity Update

NGSI-LD Client

Batch Entity Update

NGSI-LD System response

1..*

Entity Array

S: Array E: Array


5.6.9.4 Behaviour

Implementations shall exhibit the following behaviour:

- If the input Array is empty or contains a null value in any of its items, an error of type BadRequestData shall be raised.
- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation.
- Let S be an array which shall contain a list of Entity IDs, one for each NGSI-LD Entity which was successfully processed. S shall be initialized as the empty array.
- Let E be an array which shall contain a list of BatchEntityError as defined by clause 5.2.17, one for each NGSI-LD Entity which resulted in error. E shall be initialized as the empty array.
- For each Context Source Registration CSR in the Context Registry: - Let IN be a copy of the original input array. - Remove from IN all Entities not matched by CSR and remove non-matching Attributes from the remaining Entities. - Remove all Attributes from the remaining Entities in IN for which there is a matching exclusive Context Source Registration, which is not CSR itself. - Remove all Attributes from the remaining Entities in IN for which there is a matching redirect Context Source Registration, unless CSR is a redirect Context Source Registration itself. - Remove all Entities without Attributes from IN. - If IN is empty, continue with the next Context Source Registration if there is any. - If the Batch Entity Update operation is supported by CSR: - Forward the Batch Entity Update request with IN as input Array. - Merge the returned list of Entities successfully created with S. - Merge the returned list of Entities in Error with E. - Otherwise, if the Update Attributes operation (clause 5.6.2) is supported by CSR and Attribute overwrite is permitted: - For each Entity EN in the input array: - Forward an Update Attributes request for Entity EN. - Merge any successful result(s) for Entity EN updated with S. - Merge any error result(s) for Entity EN updated with E. - Otherwise, if the Append Attributes operation (clause 5.6.3) is supported by CSR and Attribute overwrite is not permitted: - For each Entity EN in the input array: - Forward an Append Attributes request for Entity EN with Attribute overwrite disabled: - Merge any successful result(s) for Entity EN updated with S. - Merge any error result(s) for Entity EN updated with E.

- Otherwise: - In case CSR is an exclusive or redirect Context Source Registration, add an Error of type Conflict for each Entity in IN to E.


- For each of the NGSI-LD Entities included in the input Array execute the behaviour defined by clause 5.6.3, but limited to a local operation, as follows: - If the Entity was successfully updated (Attributes appended), then add the corresponding Entity ID to the S array. - If the Entity update failed, then a new BatchEntityError shall be added to E containing the failed Entity ID and the ProblemDetails associated.

5.6.9.5 Output data

- none (if all Entities are successfully updated); or
- the list of Entities successfully updated (S Array), and the list of Entities in error (E Array), if only some or none of the Entities have been successfully updated.

# Related

* [HTTP: Resource: entityOperations/update](/http-binding/resource-entityoperations-update.md)
* [Clause 5.2.17](/api-operations/data-types/batchentityerror.md)
* [Clause 5.2.4](/api-operations/data-types/entity.md)
* [Clause 5.5.11.3](/api-operations/common-behaviours/batch-entity-update-case.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.6.2](/api-operations/provision/update-attributes.md)
* [Clause 5.6.3](/api-operations/provision/append-attributes.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.9](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Batch Entity Update
