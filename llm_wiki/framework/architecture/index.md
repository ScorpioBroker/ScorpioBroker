# Architectural Considerations

# Architectural Considerations

* [NGSI-LD Architectural Considerations](ngsi-ld-architectural-considerations.md) - 4.3.1 Introduction The NGSI-LD API is intended to be primarily an API and does not define a specific architecture.
* [Introduction](introduction.md) - The NGSI-LD API is intended to be primarily an API and does not define a specific architecture.
* [Centralized architecture](centralized-architecture.md) - Figure 4.3.2-1 shows a centralized architecture.
* [Distributed architecture](distributed-architecture.md) - Figure 4.3.3-1 shows a distributed architecture.
* [Federated architecture](federated-architecture.md) - The federated architecture shown in Figure 4.3.4-1 is used in cases where existing domains are to be federated.
* [NGSI-LD API Structure and Implementation Options](ngsi-ld-api-structure-and-implementation-options.md) - As stated in clause 4.3.1, the NGSI-LD API is structured into a Core API and an optional Temporal API.
* [Distributed Operations](distributed-operations.md) - 4.3.6.1 Introduction One fundamental concept underpinning all of the prototypical architectures described above (clauses
* [Introduction](introduction-4-3-6-1.md) - One fundamental concept underpinning all of the prototypical architectures described above (clauses 4.3.2, 4.3.3 and 4.3
* [Additive Registrations](additive-registrations.md) - For additive registrations, the Context Broker is permitted to hold context data about the Entities and Attributes local
* [Proxied Registrations](proxied-registrations.md) - For proxied registrations, the Context Broker itself is not permitted to hold context data about the registered Entities
* [Limiting Cascading Distributed Operations](limiting-cascading-distributed-operations.md) - When creating a registration, it is unknown whether the requested data is held at the distributed endpoint, or it is in
* [Extra information to provide when contacting Context Source](extra-information-to-provide-when-contacting-context-source.md) - If the optional array (of KeyValuePair type, as defined by clause 5.2.22) contextSourceInfo of the CSourceRegistration i
* [Additional pre- and post-processing of extra information when contacting Context Source](additional-pre-and-post-processing-of-extra-information-when-contacting-context-.md) - The following key-values have a specific well-defined meaning when defined as elements within the optional array context
* [Querying and Retrieving Distributed Entities as Unitary Operations](querying-and-retrieving-distributed-entities-as-unitary-operations.md) - Context Broker architectures assume that Entity data does not need to be centralized within a single Context Broker, how
* [Backwards compatibility of Context Source payloads](backwards-compatibility-of-context-source-payloads.md) - When retrieving Entity data found distributed across multiple associated Context Brokers each Context Source is sent a c
* [Snapshots](snapshots.md) - Context information can be dynamic, e.g.
