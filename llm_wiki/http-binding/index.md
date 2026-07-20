# API HTTP Binding (clause 6)

# Sections

* [Common behaviours](common-behaviours/) - Clause 6.3 HTTP binding behaviours
* [API HTTP Binding](api-http-binding.md) - 6.1 Introduction This clause defines the resources and operations of the NGSI-LD API.
* [Introduction](introduction.md) - This clause defines the resources and operations of the NGSI-LD API.
* [Global Definitions and Resource Structure](global-definitions-and-resource-structure.md) - All resource URIs of this API shall have the following root: - {apiRoot}/{apiName}/{apiVersion}/ NOTE 1: The apiRoot dis
* [Resource: entities/](resource-entities.md) - 6.4.1 Description This resource represents the entities known to an NGSI-LD system.
* [Resource: entities/{entityId}](resource-entities-entityid.md) - 6.5.1 Description This resource represents an entity known to an NGSI-LD system.
* [Resource: entities/{entityId}/attrs/](resource-entities-entityid-attrs.md) - 6.6.1 Description This resource represents all the Attributes (Properties or Relationships) of an NGSI-LD Entity.
* [Resource: entities/{entityId}/attrs/{attrId}](resource-entities-entityid-attrs-attrid.md) - 6.7.1 Description This resource represents an attribute (Property or Relationship) of an NGSI-LD Entity.
* [Resource: csourceRegistrations/](resource-csourceregistrations.md) - 6.8.1 Description This resource represents the context source registrations known to an NGSI-LD system.
* [Resource: csourceRegistrations/{registrationId}](resource-csourceregistrations-registrationid.md) - 6.9.1 Description This resource represents the context source registration, identified by registrationId, known to an NG
* [Resource: subscriptions/](resource-subscriptions.md) - 6.10.1 Description This resource represents the subscriptions known to an NGSI-LD system.
* [Resource: subscriptions/{subscriptionId}](resource-subscriptions-subscriptionid.md) - 6.11.1 Description This resource represents a subscription known to an NGSI-LD system.
* [Resource: csourceSubscriptions/](resource-csourcesubscriptions.md) - 6.12.1 Description This resource represents the context source registration subscriptions known to an NGSI-LD system.
* [Resource: csourceSubscriptions/{subscriptionId}](resource-csourcesubscriptions-subscriptionid.md) - 6.13.1 Description This resource represents the context source registration subscription, identified by subscriptionId, 
* [Resource: entityOperations/create](resource-entityoperations-create.md) - 6.14.1 Description A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity creatio
* [Resource: entityOperations/upsert](resource-entityoperations-upsert.md) - 6.15.1 Description A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity creatio
* [Resource: entityOperations/update](resource-entityoperations-update.md) - 6.16.1 Description A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity update 
* [Resource: entityOperations/delete](resource-entityoperations-delete.md) - 6.17.1 Description A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity deletio
* [Resource: temporal/entities/](resource-temporal-entities.md) - 6.18.1 Description This resource represents the Temporal Evolution of Entities known to an NGSI-LD system.
* [Resource: temporal/entities/{entityId}](resource-temporal-entities-entityid.md) - 6.19.1 Description This resource is associated to the Temporal Evolution of an Entity known to an NGSI-LD system.
* [Resource: temporal/entities/{entityId}/attrs/](resource-temporal-entities-entityid-attrs.md) - 6.20.1 Description This resource represents all the Attributes (Properties or Relationships) of a Temporal Evolution of 
* [Resource: temporal/entities/{entityId}/attrs/{attrId}](resource-temporal-entities-entityid-attrs-attrid.md) - 6.21.1 Description This resource represents an Attribute (Property or Relationship) of a Temporal Evolution of an Entity
* [Resource: temporal/entities/{entityId}/attrs/{attrId}/ {instanceId}](resource-temporal-entities-entityid-attrs-attrid-instanceid.md) - 6.22.1 Description This resource represents an Attribute (Property or Relationship) instance of a Temporal Evolution of 
* [Resource: entityOperations/query](resource-entityoperations-query.md) - 6.23.1 Description A sub-resource, pertaining to the entityOperations/ resource, intended to enable querying for entitie
* [Resource: temporal/entityOperations/query](resource-temporal-entityoperations-query.md) - 6.24.1 Description A sub-resource, pertaining to the temporal/entityOperations/ resource, intended to enable temporal qu
* [Resource: types/](resource-types.md) - 6.25.1 Description This resource represents the entity types available in an NGSI-LD system.
* [Resource: types/{type}](resource-types-type.md) - 6.26.1 Description This resource represents the specified entity type for which entity instances are available in an NGS
* [Resource: attributes/](resource-attributes.md) - 6.27.1 Description This resource represents the attributes available in an NGSI-LD system.
* [Resource: attributes/{attrId}](resource-attributes-attrid.md) - 6.28.1 Description This resource represents the specified attribute that belongs to entity instances existing within the
* [Resource: jsonldContexts/](resource-jsonldcontexts.md) - 6.29.1 Description This resource represents the @contexts known to an NGSI-LD system.
* [Resource: jsonldContexts/{contextId}](resource-jsonldcontexts-contextid.md) - 6.30.1 Description This resource represents a JSON-LD @context stored in the broker's internal @context storage.
* [Resource: entityOperations/merge](resource-entityoperations-merge.md) - 6.31.1 Description A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity merge f
* [Resource: entityMaps/{entityMapId}](resource-entitymaps-entitymapid.md) - 6.32.1 Description This resource represents an EntityMap available in the broker's internal storage or memory.
* [Resource: info/sourceIdentity](resource-info-sourceidentity.md) - 6.33.1 Description This resource represents identity information about the Context Source itself.
* [Resource: entityMaps](resource-entitymaps.md) - 6.34.1 Description This resource represents the Entity maps in an NGSI-LD system.
* [Resource: temporal/entityMaps](resource-temporal-entitymaps.md) - 6.35.1 Description This resource is used for creating entityMaps based on temporal queries in an NGSI-LD system.
* [Resource: snapshots](resource-snapshots.md) - 6.36.1 Description This resource represents Snapshots available in an NGSI-LD system.
* [Resource: snapshots/{snapshotId}](resource-snapshots-snapshotid.md) - 6.37.1 Description This resource represents a snapshot in an NGSI-LD system.
* [Resource: snapshots/{snapshotId}/clone](resource-snapshots-snapshotid-clone.md) - 6.38.1 Description This resource enables the cloning of a snapshot in an NGSI-LD system.
