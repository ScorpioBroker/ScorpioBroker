---
type: NGSI-LD Clause
title: "Context Information Provision"
description: "5.6.1 Create Entity 5.6.1.1 Description This operation allows creating a new NGSI-LD Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6"
---

5.6.1 Create Entity

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

5.6.2 Update Attributes

5.6.2.1 Description

This operation allows modifying an existing NGSI-LD Entity by updating already existing Attributes (Properties or Relationships) and by appending non-existing ones.

5.6.2.2 Use case diagram

A Context Producer can update Attributes within an NGSI-LD system as shown in Figure 5.6.2.2-1.

Figure 5.6.2.2-1: Update Attributes use case


5.6.2.3 Input data

- A URI representing the id of the Entity to be updated (target Entity).
- A selector of Entity types as specified by clause 4.17 (optional).
- A JSON-LD document representing an NGSI-LD Entity Fragment.

5.6.2.4 Behaviour

- If the Entity ID is not present or it is not a valid URI then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent to the target entity held locally, and no matching registrations apply (see clause 5.12), an error of type ResourceNotFound shall be raised.
- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation. NGSI-LD Nulls should be supported by this operation. If NGSI-LD Nulls are found in the payload, but are not supported, an error of type OperationNotSupported shall be raised.
- If an exclusive or redirect Context Source Registration matches against the input data, Attributes from matching input data are forwarded for remote processing. For each matching registration: - If the Update Attributes operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Update Attributes operation is not supported by the registration, this shall result in an error of type Conflict if the complete update failed or in a partial success if some parts of the update succeeded.
- The matching Attributes are then removed from the Fragment and not processed further.
- If there are remaining Attributes, for any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded for remote processing to matching endpoints in case the Update Attributes operation is supported by the matched registration.
- Then, implementations shall perform a partial update patch operation over the remains of the target Entity as mandated by clause 5.5.8, using the following procedure.
- For each Attribute (Property or Relationship) included by the Entity Fragment at root level: - If the target Entity does not include a matching Attribute (considering term expansion rules as mandated by clause 5.5.7) then such Attribute shall be appended to the target Entity. - If the target Entity already includes a matching Attribute (considering term expansion rules as mandated by clause 5.5.7): - If a datasetId is present in the Attribute included by the Entity Fragment: - If an Attribute instance in the target Entity has the same datasetId and the Attribute value is not NGSI-LD Null, then the existing Attribute instance with the specified datasetId in the target Entity shall be replaced by the new one supplied. The system generated createdAt Temporal Property as defined in clause 4.8 shall remain unchanged. - If an Attribute instance in the target Entity has the same datasetId and the Attribute value is NGSI-LD Null then the existing Attribute instance with the specified datasetId in the target Entity shall be deleted. - Otherwise the Attribute instance with the specified datasetId shall be appended to the target Entity.


- If no datasetId is present in the Attribute included by the Entity Fragment, the default Attribute instance is targeted: - If the default Attribute instance is present and the Attribute value is not NGSI-LD Null, then the existing Attribute in the target Entity shall be replaced by the new one supplied. The system generated createdAt Temporal Property as defined in clause 4.8 shall remain unchanged. - If the default Attribute instance is present and the Attribute value is NGSI-LD Null, then the existing Attribute in the target Entity shall be deleted. - Otherwise the default Attribute instance shall be appended to the target Entity.

- If type is included in the Fragment and it includes Entity Type names that are not yet in the target Entity, add them to the list of Entity Type names of the target Entity.
- If scope is included in the Fragment and the target entity includes scope, replace the scope by the one included in the Fragment, otherwise ignore it.

5.6.2.5 Output data

- A status code indicating whether all the new Attributes were updated or only some of them.
- List of Attributes (Properties or Relationships) actually updated.

5.6.3 Append Attributes

5.6.3.1 Description

This operation allows modifying an NGSI-LD Entity by adding new attributes (Properties or Relationships).

5.6.3.2 Use case diagram

A Context Producer can append new Attributes to an existing Entity within an NGSI-LD system as shown in Figure 5.6.3.2-1.

Figure 5.6.3.2-1: Append Attributes use case


5.6.3.3 Input data

- A URI representing the id of the E to be modified (target Entity).
- A selector of Entity types as specified by clause 4.17 (optional).
- A JSON-LD document representing an NGSI-LD Entity Fragment.
- An optional flag indicating whether overwriting existing Attributes within the append operation should be permitted or denied. By default, Attribute overwrites are permitted.

5.6.3.4 Behaviour

The following behaviour shall be exhibited by compliant implementations:

- If the Entity ID is not present or it is not a valid URI then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about this Entity, because there is no existing Entity which id (URI), and where specified type, is equivalent held locally to the one passed as a parameter, and no matching registrations apply (see clause 5.12), an error of type ResourceNotFound shall be raised.
- The behaviour defined in clause 5.5.4 on JSON-LD validation.
- If an exclusive or redirect Context Source Registration matches against the input data, the Attributes from matching input data are forwarded for remote processing. For each matching registration: - If the Append Attributes operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Append Attributes operation is not supported by the registration, this shall result in an error of type Conflict if the complete append failed or in a partial success if some parts of the append succeeded.
- The matching Attributes are then removed from the Fragment and not processed further.
- For any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded for remote processing to matching endpoints in case the Append Attributes operation is supported.
- Then, implementations shall perform an Append Attributes operation over the remains of the target Entity as using the following procedure: - For each Attribute (Property or Relationship) included by the Entity Fragment at root level: - If a datasetId is present in the Attribute included by the Entity Fragment: - If no Attribute instance of the same target Entity exists that has the same datasetId, then such an Attribute shall be appended to the target Entity. - If an Attribute instance of the same target Entity exists that has the same datasetId: - If overwrite is allowed, then the existing Attribute with the specified datasetId in the target Entity shall be replaced by the new one supplied. The system generated createdAt Temporal Property as defined in clause 4.8 shall remain unchanged. - If overwrite is not allowed, the existing Attribute with the specified datasetId in the target Entity shall be left untouched. - If no datasetId is present in the Attribute included by the Entity Fragment: - If no default Attribute instance of the same target Entity exists, then such Attribute shall be appended to the target Entity. - If a default Attribute instance of the same target Entity exists:


- If overwrite is allowed, then the existing default Attribute in the target Entity shall be
replaced by the new one supplied. The system generated createdAt Temporal Property
as defined in clause 4.8 shall remain unchanged.
- If overwrite is not allowed the existing default Attribute in the target Entity shall be left
untouched.
- If type is included in the Fragment and it includes Entity Type names that are not yet in the target Entity,
add them to the list of Entity Type names of the target Entity.
- If scope is included in the Fragment and overwrite is allowed, the scope of the target Entity will become
the one included in the Fragment. Otherwise, the Scopes in the Fragment that are not part of the value of
scope of the target Entity will be appended to the value of the scope of the target Entity. If there is more
than one Scope, the value of scope is represented as a JSON array containing all Scopes.

5.6.3.5 Output data

- A status code indicating whether all the new Attributes were appended or only some of them.
- List of Attributes (Properties and/or Relationships) actually appended.

5.6.4 Partial Attribute update

5.6.4.1 Description

This operation allows performing a partial update on an NGSI-LD Entity's Attribute (Property or Relationship). A partial update only changes the elements provided in an Entity Fragment, leaving the rest as they are. This operation supports the deletion of sub-Attributes but not the deletion of the whole Attribute itself.

5.6.4.2 Use case diagram

A Context Producer can carry out a partial Attribute update of an Entity within an NGSI-LD System as shown in Figure 5.6.4.2-1.

Figure 5.6.4.2-1: Partial Attribute update use case

5.6.4.3 Input data

- Entity ID (URI) of the concerned Entity, the target Entity.
- A selector of Entity types as specified by clause 4.17 (optional).


- Target Attribute (Property or Relationship) to be modified, identified by a name.
- A JSON-LD document representing an NGSI-LD Attribute Fragment.

5.6.4.4 Behaviour

- If the target Entity ID is not a valid URI or it is not present, then an error of type BadRequestData shall be raised.
- If the target Attribute name is not valid or it is not present, then an error of type BadRequestData shall be raised.
- If the target Attribute is scope, then an error of type BadRequestData shall be raised.
- The behaviour defined in clause 5.5.4 on JSON-LD validation. NGSI-LD Nulls should be supported by this operation. If NGSI-LD Nulls are found in the payload, but are not supported, an error of type OperationNotSupported shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent held locally, and no matching registrations apply (see clause 5.12), then an error of type ResourceNotFound shall be raised.
- If an exclusive or redirect Context Source Registration matches against the input data, the Attributes from matching input data are forwarded for remote processing. For each matching registration: - If the Partial Attribute update operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Partial Attribute update operation is not supported by the registration, this shall result in an error of type Conflict in case the complete partial Attribute update failed, or in a partial success if some parts of the partial Attribute update succeeded. No further processing is required.
- For any inclusive Context Source Registrations that match against the input data, that input data is also forwarded for remote processing to matching endpoints in case the Partial Attribute update operation is supported.
- Apply term expansion as mandated by clause 5.5.7, so that the fully qualified name (URI) associated to the target Attribute is properly obtained.
- If the target Entity does not contain the target Attribute: - as a default instance in case no datasetId is present; - as an instance with the specified datasetId if present; then an error of type ResourceNotFound shall be raised.
- Perform a partial update patch operation on the target Attribute following the algorithm mandated by clause 5.5.8. If present in the provided NGSI-LD Entity Fragment, the type of the Attribute has to be the same as the type of the targeted Attribute fragment, i.e. it is not allowed to change the type of an Attribute.

5.6.4.5 Output data

None.

5.6.5 Delete Attribute

5.6.5.1 Description

This operation allows deleting an NGSI-LD Attribute (Property or Relationship). The Attribute itself and all its children shall be deleted.


5.6.5.2 Use case diagram

A Context Producer can delete a specific Attribute within an NGSI-LD system as shown in Figure 5.6.5.2-1.

Figure 5.6.5.2-1: Delete Attribute use case

5.6.5.3 Input data

- Entity ID (URI) of the concerned Entity, the target Entity.
- A selector of Entity types as specified by clause 4.17 (optional).
- Target Attribute (Property or Relationship) to be deleted, identified by a name.
- An optional parameter identifying the datasetId of the target Attribute instance to be deleted. Otherwise the default Attribute instance is targeted.
- An optional flag deleteAll indicating whether also all target Attribute instances with a datasetId are to be deleted.
- An optional JSON-LD @context.

5.6.5.4 Behaviour

- If the target Entity ID is not a valid URI or it is not present, then an error of type BadRequestData shall be raised.
- If the target Attribute name is not a valid name or it is not present, then an error of type BadRequestData shall be raised.
- If an exclusive or redirect Context Source Registration matches against the input data, the input data (see clause 5.12), the input data is forwarded. For each matching registration: - If the Delete Attribute operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Delete Attribute update operation is not supported by the matched registration, this shall result in an error of type Conflict in case the complete delete Attribute failed, or in a partial success if some parts of the delete Attribute succeeded. No further processing is required.


- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent held locally, and no matching registrations apply, then an error of type ResourceNotFound shall be raised.
- For any inclusive Context Source Registrations that match against the input data, that input data is also forwarded for remote processing to matching endpoints in case the Delete Attribute operation is supported by the matched registration.
- Apply term expansion as mandated by clause 5.5.7 so that the fully qualified name (URI) associated to the target Attribute is properly obtained.
- If the target Entity does not contain the target Attribute then an error of type ResourceNotFound shall be raised.
- If the target Attribute is scope, remove the scope Attribute from the target Entity.
- If the deleteAll flag is set, remove all target Attribute instances from the target Entity.
- Otherwise: - if a datasetId parameter is provided, remove only the target Attribute instance from the given dataset whose datasetId matches the parameter; - if no datasetId parameter is provided, remove the default target Attribute instance from the target Entity.

5.6.5.5 Output data

None.

5.6.6 Delete Entity

5.6.6.1 Description

This operation allows deleting an NGSI-LD Entity.

5.6.6.2 Use case diagram

A Context Producer can completely delete an Entity within an NGSI-LD system as shown in Figure 5.6.6.2-1.

Figure 5.6.6.2-1: Delete Entity use case


5.6.6.3 Input data

- Entity ID (URI) of the Entity to be deleted, the target Entity.
- A selector of Entity types as specified by clause 4.17 (optional).

5.6.6.4 Behaviour

- If the target Entity ID is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI), and where specified type, is equivalent held locally, and no matching registrations apply (see clause 5.12), then an error of type ResourceNotFound shall be raised.
- If an exclusive or redirect Context Source Registration matches against the id, the request is forwarded for remote processing. For each matching registration: - If the Delete Entity operation is supported by the registration (see clause 4.3.6), the request is forwarded to the Registration endpoint. - If the Delete Entity update operation is not supported by the matched registration, this shall result in an error of type Conflict in case the complete delete Entity failed, or in a partial success if some parts of the delete Entity succeeded.
- For any inclusive Context Source Registrations that match against the remaining input data, that input data is also forwarded in the case the Delete Entity operation is supported for remote processing by matching endpoints.
- The input data shall be used to remove the entity locally if it exists.

5.6.6.5 Output data

None.

5.6.7 Batch Entity Creation

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

5.6.8 Batch Entity Creation or Update (Upsert)

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
- For each Context Source Registration CSR in the Context Registry: - Let IN be a copy of t

… (truncated; see full clause in the PDF).

# Related

* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
* [Clause 5.12](/api-operations/matching/matching-context-source-registrations.md)
* [Clause 5.2.17](/api-operations/data-types/batchentityerror.md)
* [Clause 5.2.4](/api-operations/data-types/entity.md)
* [Clause 5.5.11.1](/api-operations/common-behaviours/batch-entity-creation-case.md)
* [Clause 5.5.11.2](/api-operations/common-behaviours/batch-entity-creation-or-update-upsert-case.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Context Information Provision
