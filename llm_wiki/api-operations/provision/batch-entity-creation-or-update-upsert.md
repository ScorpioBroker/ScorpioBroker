---
type: NGSI-LD Operation
title: "Batch Entity Creation or Update (Upsert)"
description: "5.6.8.1 Description This operation allows creating a batch of NGSI-LD Entities, updating each of them if they already existed."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.8
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.8"
---

5.6.8.1 Description

This operation allows creating a batch of NGSI-LD Entities, updating each of them if they already existed. In some database jargon this kind of operation is known as "upsert".

5.6.8.2 Use case diagram

A Context Producer can create or update a batch of Entities within an NGSI-LD system as shown in Figure 5.6.8.2-1.


Figure 5.6.8.2-1: Upsert a batch of Entities use case

5.6.8.3 Input data

- A JSON-LD Array containing one or more JSON-LD documents each one representing an Entity as mandated by clause 5.2.4. See clause 5.5.11.2 for information on behaviour when there is more than one instance of the same entity in the input Array.
- An optional flag indicating the update mode (only applies in case the Entity already exists): - Replace. All the existing Entity content shall be replaced (default mode). - Update. Existing Entity content shall be updated.

5.6.8.4 Behaviour

Implementations shall exhibit the following behaviour:

- If the input Array is empty or contains a null value in any of its items, an error of type BadRequestData shall be raised.
- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation.
- Let S be an array which shall contain a list of Entity IDs, one for each NGSI-LD Entity which was successfully processed. S shall be initialized to the empty array.
- Let E be an array which shall contain a list of BatchEntityError as defined by clause 5.2.17, one for each NGSI-LD Entity which resulted in error. E shall be initialized to the empty array.
- For each Context Source Registration CSR in the Context Registry: - Let IN be a copy of the original input array. - Remove from IN all Entities not matched by CSR and remove non-matching Attributes from the remaining Entities. - Remove all Attributes from the remaining Entities in IN for which there is a matching exclusive Context Source Registration, which is not CSR itself. - Remove all Attributes from the remaining Entities in IN for which there is a matching redirect Context Source Registration, unless CSR is a redirect Context Source Registration itself.

Context Producer

Batch Entity Upsert

NGSI-LD Client

Batch Entity Upsert

NGSI-LD System response

1..*

Entity Array

S: Array E: Array


- If IN is empty, continue with the next Context Source Registration if there is any.
- If the Batch Entity Creation or Update (Upsert) operation is supported by CSR:
- Forward the Batch Entity Creation or Update (Upsert) request with IN as input Array.
- Merge the returned list of Entities successfully created with S.
- Merge the returned list of Entities in Error with E.
- Otherwise, if the Create Entity operation (clause 5.6.1) is supported by CSR:
- For each Entity EN in the input array:
- Forward a Create Entity request for Entity EN.
- If an error of type AlreadyExists is returned:
- If the Replace Entity operation (clause 5.6.18) is supported by CSR and the value of the
update mode flag is Replace or the flag is not set, forward a Replace Entity request for
Entity EN.
- Otherwise, if the Update Attributes operation (clause 5.6.2) is supported by CSR and
the value of the update mode flag is Update, forward an Update Attributes request for
Entity EN.
- Otherwise add an OperationNotSupported Error for Entity EN related to CSR to E.
- Merge any successful result(s) for Entity EN created or updated with S.
- Merge any error result(s) for Entity EN created or updated with E.
- Otherwise, if the Replace Entity operation (clause 5.6.18) is supported by CSR and the value of the
update mode flag is Replace or the flag is not set:
- Forward a Replace Entity request for Entity EN.
- Merge any successful result(s) for Entity EN updated with S.
- Merge any error result(s) for Entity EN updated with E.
- Otherwise, if the Update Attributes operation (clause 5.6.2) is supported by CSR and the value of the
update mode flag is Update:
- Forward an Update Attributes request for Entity EN.
- Merge any successful result(s) for Entity EN updated with S.
- Merge any error result(s) for Entity EN updated with E.
- Otherwise:
- In case CSR is an exclusive or redirect Context Source Registration, add an Error of
type Conflict for each Entity in IN to E.
- For each of the NGSI-LD Entities included in the input Array implementations shall: - Create the Entity locally if it does not exist (i.e. no Entity with the same Entity ID is present) executing the behaviour defined by clause 5.6.1, but limited to a local operation. - If there were an existing Entity with the same Entity ID, it shall be completely replaced by the new Entity content provided, if the requested update mode is 'replace'. - If there were an existing Entity with the same Entity ID, the behaviour defined by clause 5.6.2 shall be executed, but limited to a local operation, if the requested update mode is "update".


- If successful, the local creation or update shall be added to S. If while processing an Entity there is any kind of error or abnormal situation, a BatchEntityError shall be added to E containing the failed Entity ID and the related ProblemDetails.

5.6.8.5 Output data

- none (if all Entities already existed and are successfully updated); or
- the list of Entities successfully created (S Array), if all Entities not existing prior to this request have been successfully created and the others have been successfully updated; or
- the list of Entities successfully created or updated (S Array), and the list of Entities in error (E Array), if only some or none of the Entities have been successfully created or updated.

# Related

* [HTTP: Resource: entityOperations/upsert](/http-binding/resource-entityoperations-upsert.md)
* [Clause 5.2.17](/api-operations/data-types/batchentityerror.md)
* [Clause 5.2.4](/api-operations/data-types/entity.md)
* [Clause 5.5.11.2](/api-operations/common-behaviours/batch-entity-creation-or-update-upsert-case.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.6.1](/api-operations/provision/create-entity.md)
* [Clause 5.6.18](/api-operations/provision/replace-entity.md)
* [Clause 5.6.2](/api-operations/provision/update-attributes.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.8](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Batch Entity Creation or Update (Upsert)
