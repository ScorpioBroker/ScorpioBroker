---
type: NGSI-LD Data Type
title: "CSourceRegistration"
description: "This type represents the data needed to register a new Context Source."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.9
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.9"
---

This type represents the data needed to register a new Context Source.

The supported JSON members shall follow the indications provided in Table 5.2.9-1.


Table 5.2.9-1: CSourceRegistration data type definition Name Data Type Restriction Cardinality Description id String Valid URI. Unique registration identifier. (JSON-LD @id).

0..1 Generated at creation time, if it is not provided, it will be assigned during registration process and returned to client. It cannot be later modified in update operations.

| | | in update operations. | | | |
| --- | --- | --- | --- | --- | --- |
| type String | It shall be equal to | JSON-LD @type 1 | | | |
| | "ContextSourceRegistra | Use reserved type for | | | |
| | tion" | identifying Context Source | | | |

1 JSON-LD @type
Use reserved type for
identifying Context Source
Registration.
endpoint String It shall be a dereferenceable URI 1 Endpoint expressed as
dereferenceable URI
through which the
Context Source
exposes its NGSI-LD
interface.
contextSourceInfo KeyValuePair[] 0..1 Generic {key, value} array
to convey optional
information to provide
when contacting the
registered Context
Source.

information RegistrationInfo[] See data type definition in clause 5.2.10. Empty array (0 length) is not allowed

1 Describes the Entities, Properties and Relationships for which the Context Source may be able to provide information.

contextSourceAlias String Non-empty string. Pseudonym field as defined in IETF RFC 7230 [27]

0..1 A previously retrieved unique id for a registered Context Source which is used to identify loops. In the multi-tenancy use case (see clause 4.14), this id shall be used to identify a specific Tenant within a registered Context Source. description String Non-empty string 0..1 A description of this Context Source Registration.

datasetId String[] Valid URIs, "@none" for including the default Attribute instances.

| expiresAt String | DateTime (clause 4.6.3) | 0..1 Provides an expiration | | | |
| --- | --- | --- | --- | --- | --- |
| | | date. When passed the | | | |
| | | Context Source | | | |
| | | Registration will | | | |
| | | become invalid and the | | | |
| | | Context Source | | | |

0..1 Specifies the datasetIds of Attributes that the Context Source can provide, defined as per clause 4.5.5. might no longer be available.

location GeoJSON Geometry as mandated by clause 4.7

0..1 Location for which the Context Source may be able to provide information.


Name Data Type Restriction Cardinality Description management Registration Management Info

See data type definition in clause 5.2.34

0..1 Holds additional optional registration management information that can be used to limit unnecessary distributed operation requests.

managementInterval TimeInterval See data type definition in clause 5.2.11

0..1 If present, the Context Source can be queried for Temporal Entity Representations. (If latest Entity information is also provided, a separate Context Registration is needed for this purpose). The managementInterval specifies the time interval for which the Context Source can provide Entity information as specified by the createdAt, modifiedAt and deletedAt Temporal Properties. A temporal query based on the createdAt, modifiedAt or deletedAt Temporal Property is matched against the managementInterval for overlap.

mode String It shall be one of: "inclusive", "exclusive", "redirect" or "auxiliary"

The mode is assumed to be "inclusive" if not explicitly defined

0..1 The definition of the mode of distributed operation (see clause 4.3.6) supported by the registered Context Source.

observationInterval TimeInterval See data type definition in clause 5.2.11

0..1 If present, the Context Source can be queried for Temporal Entity Representations. (If latest Entity information is also provided, a separate Context Registration is needed for this purpose). The observationInterval specifies the time interval for which the Context Source can provide Entity information as specified by the observedAt Temporal Property. A temporal query based on the observedAt Temporal Property, which is the default, is matched against the observationInterval for overlap.

Name Data Type Restriction Cardinality Description observationSpace GeoJSON Geometry as mandated by clause 4.7

0..1 Geographic location that includes the observation spaces of all entities as specified by their respective observationSpace GeoProperty for which the Context Source may be able to provide information.

| operations | String[] | | Entries are limited to the named | 0..1 The definition limited | | | |
| --- | --- | --- | --- | --- | --- | --- | --- |
| | | | API operations and named | subset of API operations | | | |
| | | | operation groups (see clause | supported by the | | | |
| | | | 4.20) | registered Context | | | |

Source.

If undefined, the default set of operations is "federationOps" (see clause 4.20).

operationSpace GeoJSON Geometry as mandated by clause 4.7

0..1 Geographic location that includes the operation spaces of all entities as specified by their respective operationSpace GeoProperty for which the Context Source may be able to provide information.

| refreshRate | String | | String representing a duration in | 0..1 An indication of the likely | | | |
| --- | --- | --- | --- | --- | --- | --- | --- |
| | | | ISO 8601 [17] format | period of time to elapse | | | |
| | | | | between updates at this | | | |

| | | | | Brokers may optionally | | | |
| --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | use this information to | | | |
| | | | | help implement caching. | | | |
| registrationName | String | | Non-empty string | 0..1 A name given to this | | | |

0..1 An indication of the likely period of time to elapse between updates at this registered endpoint. Context Source Registration.

scope String or String[]

Scope(s) 0..1 Scopes (see clause 4.18) for which the Context Source has Entities. tenant String 0..1 Identifies the Tenant that has to be specified in all requests to the Context Source that are related to the information registered in this Context Source Registration. If not present, the default Tenant is assumed. Should only be present in systems supporting multi-tenancy.

The members (defined by Table 5.2.9-2) of the CSourceRegistration data structure are also defined. They are read-only and shall be automatically generated by NGSI-LD implementations. In the event that they are provided (in update or create operations) NGSI-LD implementations shall ignore them.


Table 5.2.9-2: Additional members of the CSourceRegistration data type Name Data Type Restrictions Cardinality Description lastFailure String DateTime (clause 4.6.3) 0..1 Timestamp corresponding to the instant when the last distributed operation resulting in a failure (for instance, in the HTTP binding, an HTTP response code other than 2xx) was returned.

status String Allowed values: "ok" "failed"

0..1 Read-only., Status of the
Registration. It shall be "ok" if
the last attempt to perform a
distributed operation succeeded.
It shall be "failed" if the last
attempt to perform a distributed
operation failed.
timesFailed Number 0 or greater value 0..1 Number of times that the
registration triggered a
distributed operation request that
failed.
timesSent Number 0 or greater value 0..1 Number of times that the
registration triggered a
distributed operation, including
failed attempts.
lastSuccess String DateTime (clause 4.6.3) 0..1 Timestamp corresponding to the
instant when the last successfully
distributed operation was sent.
Created on first successful
operation.

# Related

* [Clause 4.14](/framework/languages/supporting-multiple-tenants.md)
* [Clause 4.18](/framework/languages/ngsi-ld-scopes.md)
* [Clause 4.20](/framework/languages/ngsi-ld-distributed-operation-names.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
* [Clause 4.7](/framework/geo-temporal/geospatial-properties.md)
* [Clause 5.2.10](/api-operations/data-types/registrationinfo.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.9](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — CSourceRegistration
