# Common Behaviours

# Common Behaviours

* [Common Behaviours](common-behaviours.md) - 5.5.1 Introduction This clause defines common behaviours for the API operations.
* [Introduction](introduction.md) - This clause defines common behaviours for the API operations.
* [Error types](error-types.md) - Table 5.5.2-1 details a list of error types defined by NGSI-LD.
* [Error response payload body](error-response-payload-body.md) - When reporting errors back to clients, NGSI-LD implementations shall generate a JSON object in accordance with IETF RFC
* [General NGSI-LD validation](general-ngsi-ld-validation.md) - All the operations that take a JSON-LD document as input shall process such JSON-LD document as follows: - If the reques
* [Default @context assignment](default-at-context-assignment.md) - If the input provided by an API client does not include any @context, then the implementation shall at minimum assign th
* [Operation execution and generic error handling](operation-execution-and-generic-error-handling.md) - When executing an operation if an unexpected error happens and the operation cannot be completed, implementations shall
* [Term to URI expansion or compaction](term-to-uri-expansion-or-compaction.md) - NGSI-LD API operations allow clients to use short-hand strings as non-qualified names, particularly for Property, Relati
* [Partial Update Patch Behaviour](partial-update-patch-behaviour.md) - The Partial Update Patch procedure modifies an existing NGSI-LD element by overwriting the data at the Attribute level,
* [Pagination Behaviour](pagination-behaviour.md) - 5.5.9.1 General Pagination Behaviour When resolving NGSI-LD Query operations, NGSI-LD Systems shall exhibit the behaviou
* [General Pagination Behaviour](general-pagination-behaviour.md) - When resolving NGSI-LD Query operations, NGSI-LD Systems shall exhibit the behaviour described by the present clause: -
* [Pagination option using limit and offset](pagination-option-using-limit-and-offset.md) - The general pagination behaviour described in clause 5.5.9.1 only requires pointers to the following and previous pages,
* [Pagination with Entity maps](pagination-with-entity-maps.md) - In the case of queries based on Entity maps, the set of Entities considered for the result is fixed with the initial que
* [Multi-Tenant Behaviour](multi-tenant-behaviour.md) - If a Tenant is specified for an NGSI-LD operation, the operation shall only be applied to information related to the spe
* [More than one instance of the same Entity in an Entity array](more-than-one-instance-of-the-same-entity-in-an-entity-array.md) - 5.5.11.0 Foreword The following operations operate on an array of entities (as input payload): - Batch Entity Creation (
* [Foreword](foreword.md) - The following operations operate on an array of entities (as input payload): - Batch Entity Creation (clause 5.6.7).
* [Batch Entity Creation case](batch-entity-creation-case.md) - The first occurrence of an entity in the input array (the oldest one) is used for the creation of the entity.
* [Batch Entity Creation or Update (Upsert) case](batch-entity-creation-or-update-upsert-case.md) - This operation has two modes of operation, with an optional flag to select between the two.
* [Batch Entity Update case](batch-entity-update-case.md) - This operation has two modes of operation, with an optional flag to select between the two.
* [Batch Entity Delete case](batch-entity-delete-case.md) - The Batch Entity Delete operation has as input an array of Entity IDs, for the entities to be deleted.
* [Batch Entity Merge case](batch-entity-merge-case.md) - The Batch Entity Merge operation has as input an array of Entity IDs, for the entities to be merged.
* [Merge Patch Behaviour](merge-patch-behaviour.md) - The merge patch procedure modifies an existing NGSI-LD element by applying the set of changes found in an NGSI-LD Fragme
* [Limiting operations to local scope](limiting-operations-to-local-scope.md) - The API provides a binding-specific mechanism to limit the execution of operations to a local scope, i.e.
* [Distributed Transactional Behaviour](distributed-transactional-behaviour.md) - The following operations may occur as part of a distributed transactional request: - Retrieve Entity (clause 5.7.1).
* [Snapshot Behaviour](snapshot-behaviour.md) - If a Snapshot (see clause 4.3.7) is specified for an NGSI-LD operation, the operation shall only be applied to informati
