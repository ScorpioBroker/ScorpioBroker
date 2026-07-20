---
type: NGSI-LD Clause
title: "Matching Context Source Registrations"
description: "When querying Context Source Registrations as described in clause 5.10.2 and subscribing to Context Source Registrations as described in clause 5.11.2, the Entities and/or Attributes specified in the"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.12
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.12"
---

When querying Context Source Registrations as described in clause 5.10.2 and subscribing to Context
Source Registrations as described in clause 5.11.2, the Entities and/or Attributes specified in the request have
to be matched against the set of Context Source Registrations, extracting the matching ones. This clause
describes this matching.
The relevant specification information in the query for Context Source Registrations are the selector of
Entity Types (if present), the list of Entity identifiers (if present), the id pattern (if present) and the list of Attribute
names (if present). In the case of subscriptions to Context Source Registrations, it is the Entities as
specified in the array of type EntitySelector in the Subscription, the watchedAttributes element of the Subscription and
the attributes specified as part of the NotificationParams element of the Subscription. If the attributes in the
NotificationParams element are empty or not present, the matching is done as if no attribute identifiers have been
specified, otherwise the combination of the watchedAttributes and the attributes in the NotificationParams element are
used as the specified attribute identifiers for the matching.
Even though the way relevant Entities are specified differs in queries and subscriptions, they consist of the same
information, so for the purpose of this clause, the specification of Entity Types or Attributes refers to the relevant
elements for matching, i.e. Entity Types, Entity identifiers, id pattern and Attribute names. A specification of Entity
Types or Attributes shall contain at least one of:
a) selector of Entity Types; or
b) list of Attribute names.
A specification of Entity Types or Attributes matches a Context Source Registration if at least one of the
RegistrationInfo elements in the information element matches. An Entity specification matches a RegistrationInfo if the
following conditions hold:
- If present, the selector of Entity Types, Entity identifiers and id pattern match at least one of the EntityInfo elements of the RegistrationInfo (see below).
- If present, the Attribute identifiers match the combination of Properties and Relationships specified in the RegistrationInfo (see below).


An Entity specification consisting of selector of Entity Types, Entity identifiers and id pattern matches an EntityInfo element of the RegistrationInfo if the type selector matches the entity types in the EntityInfo element and one of the following conditions holds:

- The EntityInfo contains neither an id nor an idPattern.
- One of the specified Entity identifiers matches the id in the EntityInfo.
- At least one of the specified Entity identifiers matches the idPattern in the EntityInfo.
- The specified id pattern matches the id in the EntityInfo.
- Both a specified id pattern and an idPattern in the Entity Info are present (since in the general case it is not easily feasible to determine if there can be identifiers matching both patterns). Attribute names match the combination of Properties and Relationships if one of the following conditions hold:
- No Attribute names have been specified (as this means all Attributes are requested).
- The combination of Properties and Relationships is empty (as this means only Entities have been registered and the Context Sources may have matching Property or Relationship instances).
- If at least one of the specified attribute names matches a Property or Relationship specified in the RegistrationInfo. If the request that triggered the matching includes a datasetId parameter and the CSourceRegistration to be matched contains a datasetId element, the CSourceRegistration should only be considered matching, if both have at least one value in common. If only one of them specifies a datasetId, it is considered a match. In the case of distributed operations (see clause 4.3.6.4), where a listing of all previously encountered Context Sources is supplied with the request, no registration shall match if the CSourceRegistration contextSourceAlias can be found within the listing of previously encountered Context Sources. Note that distributed queries (see clause 4.3.6.7), can be supplied with an EntityMap (see clause 4.5.25) which lists all Entity ids successfully matched during a previous request. If the location of an EntityMap is passed into a subsequent request, the retrieved EntityMap shall be used in preference to the matching algorithm described above, provided that the EntityMap is valid and has not expired.

# Related

* [Clause 4.3.6.4](/framework/architecture/limiting-cascading-distributed-operations.md)
* [Clause 4.3.6.7](/framework/architecture/querying-and-retrieving-distributed-entities-as-unitary-operations.md)
* [Clause 4.5.25](/framework/data-representation/ngsi-ld-entitymap-representation.md)
* [Clause 5.10.2](/api-operations/discovery/query-context-source-registrations.md)
* [Clause 5.11.2](/api-operations/csource-subscription/create-context-source-registration-subscription.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.12](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Matching Context Source Registrations
