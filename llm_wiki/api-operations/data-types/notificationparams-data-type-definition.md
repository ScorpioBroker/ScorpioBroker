---
type: NGSI-LD Data Type
title: "NotificationParams data type definition"
description: "This datatype represents the parameters that allow to convey the details of a notification."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.14.1
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.14.1"
---

This datatype represents the parameters that allow to convey the details of a notification.
The supported JSON members shall follow the requirements provided in Table 5.2.14.1-1.

Table 5.2.14.1-1: NotificationParams data type definition

Restrictions Cardinality Description

Name Data Type

| endpoint | Endpoint | See data type definition in | | | 1 | Notification endpoint details. | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | clause 5.2.15 | | | | | | | | |
| attributes | String[] | Attribute name as short hand strings or | | | 0..1 | A synonym for pick, except that id, | | | | |
| | | URIs. Empty array (0 length) is not | | | | type, scope are not allowed. | | | | |
| | | allowed | | | | Deprecated | | | | |

Attribute names to be included in the notification payload body. If undefined it will mean all Attributes.

format String It shall be one of: "normalized", "concise", "simplified" (or its synonym "keyValues")

0..1 Conveys the representation format of the entities delivered at notification time. By default, it will be in the normalized format.

join String It shall be one of: "flat", "inline", "@none"

0..1 String representing the type of Linked Entity retrieval to apply. By default, it will be "@none". joinLevel Number Positive Integer 0..1 Depth of Linked Entity retrieval to apply. Default is 1. Only applicable if join parameter is "flat", or "inline".

0..1 When defined, the specified Entity members are removed from each Entity within the payload.

omit String[] Entity member ("id", "type", "scope" or a projected Attribute name) as a valid attribute projection language string as per clause 4.21. Empty array (0 length) is not allowed

0..1 When defined, every Entity within the payload body is reduced down to only contain the specified Entity members.

pick String[] Entity member ("id", "type", "scope" or a projected Attribute name as a valid attribute projection language string as per clause 4.21). Empty array (0 length) is not allowed


Name Data Type

Restrictions Cardinality Description

showChanges Boolean false by default 0..1 If true the previous value
(previousValue) of Properties or
languageMap
(previousLanguageMap) of
Language Properties or object
(previousObject) of Relationships is
provided in addition to the current
one. This requires that it exists, i.e.
in case of modifications and
deletions, but not in the case of
creations.
showChanges cannot be true in
case format is "keyValues".
sysAttrs Boolean false by default 0..1 If true, the system generated
attributes createdAt and modifiedAt
and the system attribute expiresAt
are included in the response
payload body, in the case of a
deletion also deletedAt.

# Related

* [Clause 4.21](/framework/languages/ngsi-ld-attribute-projection-language.md)
* [Clause 5.2.15](/api-operations/data-types/endpoint.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.14.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NotificationParams data type definition
