---
type: NGSI-LD Clause
title: "Multi-Attribute Support"
description: "4.5.5.1 Introduction For each Entity, there can be Attributes that simultaneously have more than one instance."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.5
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.5"
---

4.5.5.1 Introduction

For each Entity, there can be Attributes that simultaneously have more than one instance. In the case of Properties, there
may be more than one source at a time that provides a Property instance, e.g. based on independent sensor
measurements with different quality characteristics. For instance, take a speedometer and a GPS both providing the
current speed of a car. In the case of Relationships, there may be non-functional Relationships requiring separate
metadata attached to each object, e.g. for a room, there may be multiple "contains" Relationships to all sorts of
objects currently in the room that have been put there by different people (a separate "placedBy"
Relationship-of-the-Relationship) and which are dynamically changing over time.
To be able to explicitly manage such multi-attributes, the optional datasetId property is used, which is of datatype URI,
or equal to the JSON-LD keyword "@none". It is introduced for Properties and Relationships in clauses 4.5.2 and
4.5.3 respectively. If a datasetId is provided when creating, updating, appending or deleting Attributes, only instances
with the same datasetId are affected, leaving instances with another datasetId or an instance without a datasetId
untouched. If no datasetId is provided, or "datasetId": "@none" is supplied, it is considered as the default
Attribute instance. Thus, the creation, updating, appending or deleting of Attributes without providing a datasetId only
affects the default Attribute instance. There can only be one default Attribute instance for an Attribute with a given
Attribute name in any request or response. An example can be found in annex C, clause C.2.2.
When requesting Entity information, if there are multiple instances of matching Attributes these are returned as arrays
of Attributes, instead of a single Attribute element. The datasetId of the default Attribute instance is never explicitly
included in responses, i.e. a default Attribute instance does not have a datasetId.

The datasetId can be used to create different "views" of Entities. All Attribute instances sharing the same datasetId can be seen as one "view". If one or more datasetIds are specified in the request, only the Attribute instances that match one of the datasetIds will be returned. The datasetId of the default Attribute instance can be specified as "@none" In case that no Attribute instances match the provided datasetIds, then the Attribute shall not be returned with the Entity. If no datasetId is provided, then all available Attribute instances will be returned. There is no multi-attribute support for non-reified Attributes, in particular this applies to the Temporal Properties createdAt, modifiedAt, deletedAt, expiresAt and observedAt, and also the unitCode Property.

4.5.5.2 Processing of Conflicting Transient Entities

In case of conflicting information when an Entity is received from a registered Context Source and marked with an expiresAt DateTime, but this expiresAt is not duplicated across all versions of the Entity received across all registered Context Sources, the following mechanism shall be used to differentiate. For each version of the Entity received where the expiresAt non-reified attribute is present at the Entity level:

- If no expiresAt attribute is present as a property of an Attribute, a non-reified expiresAt attribute is added as a property of that Attribute corresponding to the expiresAt found on the Entity.
- If the expiresAt attribute is present as a property of an Attribute, and the value on the Attribute is further in the future than the expiresAt value found on the Entity, the value of the expiresAt on the Attribute is overwritten to corresponding to the earlier DateTime.

4.5.5.3 Processing of Conflicting Attributes

In case of conflicting information for an Attribute, where a datasetId is duplicated, but there are differences in the other attribute data, if an expiresAt DateTime is present on the Attribute and the date lies in the past, it shall be discarded, thereafter the one with the most recent observedAt DateTime, if present, and otherwise the one with the most recent modifiedAt DateTime shall be provided. If no other mechanism for determining the most current Attribute instance is found, the NGSI-LD system shall choose the Attribute instance at random and the result is indeterminate. When conflicting information is received for the non-reified expiresAt attribute at an Entity level:

- If an expiresAt attribute is missing from at least one version of the Entity received from registered Context Sources, the expiresAt attribute is removed from the Entity.


- Otherwise, the expiresAt attribute of the Entity is set to the DateTime corresponding to the expiresAt DateTime which is furthest in the future.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Multi-Attribute Support
