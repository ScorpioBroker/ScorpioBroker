---
type: NGSI-LD Operation
title: "Batch Entity Delete"
description: "5.6.10.1 Description This operation allows deleting a batch of NGSI-LD Entities."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.10
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.10"
---

5.6.10.1 Description

This operation allows deleting a batch of NGSI-LD Entities.

5.6.10.2 Use case diagram

A Context Producer can delete a batch of Entities within an NGSI-LD system as shown in Figure 5.6.10.2-1.

Figure 5.6.10.2-1: Delete a batch of Entities use case

5.6.10.3 Input data

- A JSON-LD Array containing a list of Entity IDs (URIs) that are requested to be deleted. See clause 5.5.11.4 for information on behaviour when there is more than one instance of the same Entity ID in the input Array.


5.6.10.4 Behaviour

Implementations shall exhibit the following behaviour:

- If the input Array is empty or contains a null value in any of its items, an error of type BadRequestData shall be raised.
- Let S be an array which shall contain a list of Entity IDs, one for each NGSI-LD Entity which was successfully processed. S shall be initialized to the empty array.
- Let E be an array which shall contain a list of BatchEntityError as defined by clause 5.2.17, one for each NGSI-LD Entity which resulted in error. E shall be initialized to the empty array.
- For each Context Source Registration CSR in the Context Registry: - Let IN be a copy of the original input array. - Remove from IN all Entities not matched by CSR and remove non-matching Attributes from the remaining Entities. - Remove all Attributes from the remaining Entities in IN for which there is a matching exclusive Context Source Registration, which is not CSR itself. - Remove all Attributes from the remaining Entities in IN for which there is a matching redirect Context Source Registration, unless CSR is a redirect Context Source Registration itself. - Remove all Entities without Attributes from IN. - If IN is empty, continue with the next Context Source Registration if there is any. - If the Batch Entity Delete operation is supported by CSR: - Forward the Batch Entity Delete request with IN as input Array. - Merge the returned list of Entities successfully created with S. - Merge the returned list of Entities in Error with E. - Otherwise, if the Delete Entity operation (clause 5.6.6) is supported by CSR: - For each Entity EN in the input array: - Forward a Delete Entity request for Entity EN. - Merge any successful result(s) for Entity EN deleted with S. - Merge any error result(s) for Entity EN deleted with E. - Otherwise: - In case CSR is an exclusive or redirect Context Source Registration, add an Error of type Conflict for each Entity in IN to E. - For each of the NGSI-LD Entity IDs included in the input Array execute the behaviour defined by clause 5.6.6, but limited to a local operation, as follows: - If the Entity corresponding to an Entity ID was successfully deleted, then add such Entity ID to the S array. - If the Entity deletion failed, then a new BatchEntityError shall be added to E containing the failed Entity ID and the related ProblemDetails.


5.6.10.5 Output data

- none (if all Entities that already existed are successfully deleted); or
- the list of Entities successfully deleted (S Array), and the list of Entities in error (E Array), if some or all of the Entities have not been successfully deleted.

# Related

* [HTTP: Resource: entityOperations/delete](/http-binding/resource-entityoperations-delete.md)
* [Clause 5.2.17](/api-operations/data-types/batchentityerror.md)
* [Clause 5.5.11.4](/api-operations/common-behaviours/batch-entity-delete-case.md)
* [Clause 5.6.6](/api-operations/provision/delete-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.10](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Batch Entity Delete
