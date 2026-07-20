---
type: NGSI-LD Clause
title: "Distributed Operations"
description: "4.3.6.1 Introduction One fundamental concept underpinning all of the prototypical architectures described above (clauses 4.3.2, 4.3.3 and 4.3.4) is the idea that Entity data does not need to be centra"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.6
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.6"
---

4.3.6.1 Introduction

One fundamental concept underpinning all of the prototypical architectures described above (clauses 4.3.2, 4.3.3 and
4.3.4) is the idea that Entity data does not need to be centralized within a single Context Broker. When reading
context information, a Context Broker can be used as a single point of access to retrieve Entity data found
distributed across multiple associated Context Brokers each receiving a context consumption request. Similarly,
when modifying an Entity, a single request to a Context Broker may result in the operation being distributed and
different parts of that Entity being updated across multiple Context Brokers each receiving a context provision
request.
As long as there is only a centralized Context Broker, i.e. there are no Context Sources registered, all NGSI
LD requests, with few exceptions such as Update Attributes (see clause 5.6.2) and the batch operations (see clauses
5.6.7, 5.6.8, 5.6.9, 5.6.10 and 5.6.20), can either be successfully executed completely, or result in an error. In the
distributed case, all requests can be partially successful. For the centralized case described above, only specific
operations, such as Update Attributes and the batch operations, can be partially successful.
It is the responsibility of the Context Broker to respect the registration parameters when issuing distributed
requests. For instance, if a registration states that only Entities of a given type are offered, the distributed request does
not contain additional types. Such a strict requirement is justified because Context Sources (the receivers of the
distributed request) are not in a position to determine whether a request has been triggered by a registration, rendering
them unable to ensure that the registration parameters are respected in the first place. This applies for any kind of
context data a Context Broker can exchange such as Entity IDs, entity types, attribute names, geofenced areas, etc.
Ultimately, all constraints specified in the registration shall be respected.
When a Context Source is registered, an operation mode is selected. This defines the basis for distributed
operations and also defines whether or not the Context Broker is permitted to hold context data about the Entities
and Attributes locally itself.
If two registered Context Sources are providing context data for the same Attribute, the Attribute instances can be
distinguished by datasetId. The mechanism for determining which data shall be returned is defined in clause 4.5.5.
It is possible to restrict a registered Context Source to operate on a specific Entity type or list of Entity types. In
order for Context Broker hierarchies to support and restrict the distribution of such limited operations, the Entity
type selector (see clause 4.17) can be added as a filter on forwarded requests even where its presence initially seems
redundant.


Furthermore, registered Context Sources may indicate that they are only willing to respond to a limited subset of API operations. Context Brokers shall respect this, to avoid unnecessarily sending distributed operation requests which are always guaranteed to fail. For example, a Context Source may consistently refuse certain API operations since it does not support them. Alternatively, some Context Source endpoints (such as updates) may be protected for use by authorized users only, and not accessible to a Context Broker without those rights. Limited access is likely to be the case in extended data sharing scenarios, where a registered Context Source, and the data held within it, may belong to an external third party. For the endpoints served, all registered Context Sources shall support the normalized representation of Entities as default. Support of additional representation formats is optional and will depend on the implementation. System generated attributes such as modifiedAt and createdAt (see clause 4.8) should be supported by registered Context Sources, at a minimum no error shall be returned if they are not available when requested.

4.3.6.2 Additive Registrations

For additive registrations, the Context Broker is permitted to hold context data about the Entities and Attributes
locally itself, and also obtain data from external sources. Context provisioning operations are serviced both locally by
the Context Broker itself, and also distributed on to the registered sources.
An inclusive Context Source Registration specifies that the Context Broker considers all registered
Context Sources as equals and will distribute operations to those Context Sources even if relevant context
data is available directly within the Context Broker itself (in which case, all results will be integrated in the final
response). Data from every Context Source registered by an inclusive Context Source Registration is
requested with an equal priority. This is the default mode of operation.
An auxiliary Context Source Registration never overrides data held directly within a Context Broker.
Auxiliary distributed operations are limited to context information consumption operations (see clause 5.7). Context
data from auxiliary context sources is only included if it is supplementary to the context data otherwise available to the
Context Broker. Auxiliary Context Source Registrations are always accepted as there can never be a
conflict.

4.3.6.3 Proxied Registrations

For proxied registrations, the Context Broker itself is not permitted to hold context data about the registered
Entities and Attributes locally (thus all registered context data is obtained from the external registered sources).
Unregistered Attributes of an Entity are permitted to be held locally; when context provisioning operations are received,
registered Attributes are distributed on to the registered sources and never serviced directly by the Context Broker
itself.
An exclusive Context Source Registration specifies that all of the registered context data is held in a single
location external to the Context Broker. The Context Broker itself holds no data locally about the registered
Attributes and no overlapping proxied Context Source Registrations shall be supported for the same
combination of registered Attributes on the Entity. An exclusive registration shall always relate to specific Attributes
found on a single Entity. Thus, the registration shall define both:
- An entity id (i.e. an id pattern or Entity type defining a group of entities is not supported for exclusive registrations).
- Attributes. Once an exclusive Context Source Registration has been created, no further exclusive or redirect Context Source Registrations can be created for that same combination of Entity ID and Attributes. A redirect Context Source Registration also specifies that the registered context data is held in a location external to the Context Broker. It is possible to register (any combination of):
- A whole Entity by id or id pattern (i.e. without specifying individual Attributes in the registration; in this case, all Attributes are held externally).
- Entities by Entity type only (with or without specifying individual Attributes).


- Attributes only. Potentially multiple distinct redirect registrations can apply at the same time. The Context Broker itself holds no data locally in conflict to the registration. In the case that multiple overlapping redirect registrations are defined, operations are distributed to all registered Context Sources.

4.3.6.4 Limiting Cascading Distributed Operations

When creating a registration, it is unknown whether the requested data is held at the distributed endpoint, or it is in turn
distributed via further registrations. It is necessary to include a binding-specific mechanism to request operations only
on the registered endpoint itself to avoid cascades of an excessive lengths, duplicates or loops.
Furthermore, it is not known if any distributed endpoints of a registered Context Source are in turn reliant on
previously encountered Context Sources thus causing an infinite loop. Therefore, when processing a distributed
operation, a specific field listing all previously encountered Context Sources (e.g. a Via header in the response in
case of HTTP binding (IETF RFC 7230 [27])) shall be passed as part of the request and this field can be used to exclude
duplicated sources from matching as context source registrations.
In the case of multi-tenancy (see clause 4.14) each Tenant found within each registered Context Source shall be
considered separately.

4.3.6.5 Extra information to provide when contacting Context Source

If the optional array (of KeyValuePair type, as defined by clause 5.2.22) contextSourceInfo of the CSourceRegistration
is present, it contains, whatever extra information the Context Broker shall convey when contacting the Context
Source. This can be information the Context Broker needs to successfully communicate with the Context
Source (e.g. Authorization material), or for the Context Source to correctly interpret the received content
(e.g. the Link URL to fetch an @context). The method for conveying this information is binding-specific, e.g. using
headers in the case of HTTP.
Instead of providing the actual value, the special value "urn:ngsi-ld:request" can be used to indicate that the
respective value is to be taken from the request that triggered the given request, if present.
EXAMPLE: If the key value pair "user": "urn:ngsi-ld:request" is part of contextSourceInfo of
the CSourceRegistration, the Context Broker checks if "user" was conveyed in the
triggering request. If this is the case, e.g. "user": "abcd", "user": "abcd" is also
conveyed when contacting the Context Source.
As Tenant information, if applicable, is directly specified in the CSourceRegistration, it shall not be part of
contextSourceInfo. Binding-specific information that is used for setting up the connection or is specific for an
interaction, e.g. Content-length in HTTP, cannot be overridden by contextSourceInfo. If present, such information shall
be ignored.

4.3.6.6 Additional pre- and post-processing of extra information when contacting Context Source

The following key-values have a specific well-defined meaning when defined as elements within the optional array contextSourceInfo of the CSourceRegistration.

If the key "accept" is defined:

- the value shall be a MIME type acceptable to the Context Broker (one of: "application/json", "application/ld+json").
- the response from the distributed endpoint shall be returned in this defined format and if necessary, the Context Broker shall be responsible for converting this to the desired content type when aggregating responses to the initial request.


If the key "contentType" is defined:

- the value shall be a MIME type acceptable to the Context Broker (one of: "application/json", "application/ld+json").
- the Context Broker shall provide the request and the associated @context as required by the MIME type when distributing the request to the context source endpoint, regardless of how it was provided in the initial request. If the key "jsonldContext" is defined:
- the value shall correspond to a URL reference as defined by the JSON-LD specification [2], section 3.1.
- the Context Broker shall apply a compaction operation as defined by the JSON-LD specification [2], section 4.1.5 over both payload and query parameters using the JSON-LD Context supplied in the value of the "jsonldContext" key-value pair, prior to distributing the request to the context source endpoint and forwarding with this JSON-LD context using an appropriate binding. Additionally, if a payload is defined in the initial request to the Context Broker, the "Content-Type" of the forwarded request shall be "application/json" and the Context Broker shall remove any @context members from the payload prior to distributing the request to the context source endpoint. If the key "ngsildConformance" is defined:
- The value shall define in the form major.minor, for example 1.5.
- The Context Broker shall apply a backwards compatibility operation over the payload (as defined by clause 4.3.6.8) prior to distributing the request to the context source endpoint such that the forwarded payload conforms to the specified version of the NGSI-LD specification.

4.3.6.7 Querying and Retrieving Distributed Entities as Unitary Operations

Context Broker architectures assume that Entity data does not need to be centralized within a single Context
Broker, however, when querying context information, Entity data retrieval can be considered as a unitary operation,
masking the fact that each registered Context Broker is receiving a separate distributed Context Consumption
request.
To process each Context Consumption request efficiently, and to support consistent pagination, it is necessary for the
Context Broker to initially make a broad request to each registered Context Source whose registration is
matching the request.
In the case of a query Entities operation (clause 5.7.2) or query Temporal Evolution of Entities operation
(clause 5.7.4), a list of Entity identifiers is returned and stored together with the registration information in an Entity
map. Only the Entities whose identifiers are contained in the Entity map are considered when rendering the result pages.
Filtering based on queries, geoqueries, scope queries or attribute filters, as provided in the original query request, is
applied to the Entities before adding the Entity to a result page, i.e. identifiers of Entities not matching the filters at the
time of checking are removed from the Entity map.
In the case of a retrieve Entity operation (clause 5.7.1) or retrieve Temporal Evolution of an Entity operation
(clause 5.7.3), an Entity map can be used to make subsequent retrievals of the same Entity more efficient, as the Entity
map provides the information about which Context Source(s) store relevant Entity information, and other Context
Sources do not have to be considered.
This Entity mapping is an internal operation, not usually exposed to the end user, however it is necessary to explicitly
define a consistent mechanism for Entity map creation, caching and retrieval.
A specific field pointing to the location of a cached EntityMap (e.g. a custom header in the response in case of HTTP
binding, see Table 6.4.3.2-2) shall be returned within the response of a query, whenever this is requested by the client.
Similarly, the reuse of a previously created EntityMap can be requested by passing the same specific field into a
request.
Since an exclusive Context Source Registration already specifies that all context data is held in a single
location, its relevance to a distributed query can be inferred within the Registered Context Source without the use of an
EntityMap.

When executing Context Consumption or Subscription operations, a significant optimization in performance can be
achieved if it is known a priori whether individual Entities are themselves distributed among Context Brokers and
Context Sources, or if each Context Broker and Context Source always stores complete Entities. In the
latter case all parameters used for filtering such as queries, geoqueries, scope queries or attribute filters can be
forwarded and applied locally, whereas in the former case, the Entity first has to be assembled by the Context
Broker and only then the filtering can be applied. Since being able to apply filters locally is significantly more
efficient, a parameter can indicate that, for the given request, only Entities are to be expected that are stored in their
entirety on each Context Broker and each Context Sources, and there are no Entities that are themselves
stored in a distributed fashion. Such Entities are also referred to as split Entities. Context Broker implementations
should enable configuring a default for this parameter, so deployments where no split Entities are to be expected can
filter locally and thus be more efficient.
In the case of split Entities, EntityMaps initially only store "candidate Entities" as no filters could be applied, because
only a part of the Entity was available. In the process of pagination, the filters will be (re-)checked. Any Entity not (or
no longer) fulfilling the filter shall be removed from the EntityMap.

4.3.6.8 Backwards compatibility of Context Source payloads

When retrieving Entity data found distributed across multiple associated Context Brokers each Context
Source is sent a context consumption request. A Context Broker shall assume that every Context Source
will return valid NGSI-LD Entity data in a format that it understands and it shall reject data that is invalid. However,
since the definition of a valid NGSI-LD Entity has broadened with each version of the NGSI-LD specification, it is
possible that a registered Context Source could respond with valid NGSI-LD Entity data which does not fit the
narrower confines of a previous NGSI-LD specification.
Therefore, when making a context consumption request, a Context Broker may wish to indicate that it is only
capable of interpreting responses which conform to a specific NGSI-LD specification, in which case the Context
Source shall endeavour to amend its payload accordingly. Table 4.3.6.8-1 describes a minimal level of support for a
NGSI-LD Entity data as specified by each version of the NGSI-LD specification, and the expected fallback behaviour
required if a context consumption request is made to receive data conformant to an earlier version of the NGSI-LD
specification. Table 4.3.6.8-2 describes conformance fallbacks for an NGSI-LD Property (and its subclasses) and Table
4.3.6.8-2 describes conformance fallbacks for an NGSI-LD Relationship (and its subclasses).
The version of the NGSI-LD specification requested and the conformant version returned is defined in the form
major.minor, for example 1.5.

Table 4.3.6.8-1: NGSI-LD Entity data type attribute support Name Data Type Definition Version Introduced

Conformant Data Fallback

id String Valid URI 1.0
type String or String[]
(see note 1)

1.0

expiresAt String DateTime as mandated by clause 4.22

1.9 Remove attribute from payload

location GeoProperty Default geospatial Property of an entity. See clause 4.7

| observationSpace | GeoProperty | | See clause 4.7 | 1.0 | | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| operationSpace | GeoProperty | | See clause 4.7 | 1.0 | | | | | |
| scope | String or String[] | | See clause 4.18 | 1.4 | | Remove attribute from | | | |

1.0 payload

| | Property or | | Property as mandated by | 1.0 | | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | Property[] | | clause 4.5.2. | | | | | | |
| | (see note 2) | | | | | | | | |
| | GeoProperty or | | GeoProperty as mandated | 1.0 | | | | | |
| | GeoProperty[] | | by clause 4.5.2. | | | | | | |
| | (see note 2) | | | | | | | | |
| | LanguageProperty or | | LanguageProperty as | 1.4 | | Reformat attribute as | | | |
| | LanguageProperty[] | | mandated by clause 4.5.18. | | | Property | | | |
| | (see note 2) | | | | | | | | |
| | JsonProperty or | | JsonProperty as | 1.8 | | Reformat attribute as | | | |
| | JsonProperty[] | | mandated by clause 4.5.24. | | | Property | | | |
| | (see note 2) | | | | | | | | |


Name Data Type Definition Version Introduced

Conformant Data Fallback

VocabProperty or VocabProperty[] (see note 2)

VocabProperty as mandated by clause 4.5.20.

1.8 Reformat attribute as Property

ListProperty or ListProperty[] (see note 1)

ListProperty as mandated by clause 4.5.21.

1.8 Reformat attribute as Property

 Relationship or Relationship[] (see note 3)

Relationship as mandated by clause 4.5.3.

1.0

ListRelationship or ListRelationship[] (see note 3)

ListRelationship as mandated by clause 4.5.22.

1.8 Reformat attribute as Relationship

NOTE 1: From 1.3 onwards, an Entity type can be assigned multiple values. For 1.0 backwards compatibility only
return a single element with preference to the first instance.
NOTE 2: From 1.3 onwards, multiple instances of a Property (or subclass of Property) identified by the same Property
name can be separated by datasetId. For 1.0 backwards compatibility only return a single element of
Property Array with preference to the default instance.
NOTE 3: From 1.3 onwards, multiple instances of a Relationship (or subclass of Relationship) identified by the same
Relationship name can be separated by datasetId. From 1.3 onwards. For 1.0 backwards compatibility only
return a single element of Relationship Array with preference to the default instance.

Table 4.3.6.8-2: NGSI-LD Property data type attribute support Name Data Type Definition Version Introduced

Conformant Data Fallback

type String 1.0 value Any JSON value as defined by IETF RFC 8259 [6]

See NGSI-LD Value definition in clause 3.1

1.0

datasetId String Valid URI as mandated by clause 4.5.5

1.3 Remove attribute from payload

expiresAt String DateTime as mandated by clause 4.22

1.9 Remove attribute from payload

observedAt String DateTime as mandated by clause 4.8

1.3 Remove attribute
from payload
unitCode String As mandated by [15] 1.3 Remove attribute
from payload
valueType String See clause 4.5.2 1.9 Remove attribute
from payload

Table 4.3.6.8-3: NGSI-LD Relationship data type attribute support Name Data Type Definition Version Introduced

Conformant Data Fallback

type String Valid URI 1.0 object String or String[] 1.0 datasetId String Valid URI as mandated by clause 4.5.5

1.3 Remove attribute from payload

expiresAt String DateTime as mandated by clause 4.22

1.9 Remove attribute from payload objectType String or String[] See clause 4.5.23 1.8 Remove attribute from payload

observedAt String DateTime as mandated by clause 4.8

1.3 Remove attribute from payload

When responding to a context consumption request to supply data conforming to a specific NGSI-LD specification, Context Sources should indicate the version of the specification the returned payload actually conforms to. In general, Context Sources will not be expected to be flexible enough to supply payloads conformant to all past and future versions of the specification, but the requesting Context Broker may use supplied version information when collating data from multiple Context Sources. and to validate and amend received payloads.

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.14](/framework/languages/supporting-multiple-tenants.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.18](/framework/languages/ngsi-ld-scopes.md)
* [Clause 4.22](/framework/languages/transient-storage-of-entities-and-attributes.md)
* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 4.5.18](/framework/data-representation/ngsi-ld-languageproperty-representations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Distributed Operations
