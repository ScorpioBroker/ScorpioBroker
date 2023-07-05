INSERT INTO public.contexts (id, body, kind) values(')$%^&', '{
  "@context": {

    "ngsi-ld": "https://uri.etsi.org/ngsi-ld/",
    "geojson": "https://purl.org/geojson/vocab#",
    "id": "@id",
    "type": "@type",

	"Attribute": "ngsi-ld:Attribute",
	"AttributeList": "ngsi-ld:AttributeList",
    "ContextSourceNotification": "ngsi-ld:ContextSourceNotification",
    "ContextSourceRegistration": "ngsi-ld:ContextSourceRegistration",
    "Date": "ngsi-ld:Date",
    "DateTime": "ngsi-ld:DateTime",
	"EntityType": "ngsi-ld:EntityType",
	"EntityTypeInfo": "ngsi-ld:EntityTypeInfo",
	"EntityTypeList": "ngsi-ld:EntityTypeList",
    "Feature": "geojson:Feature",
    "FeatureCollection": "geojson:FeatureCollection",
    "GeoProperty": "ngsi-ld:GeoProperty",
    "GeometryCollection": "geojson:GeometryCollection",
    "LineString": "geojson:LineString",
	"LanguageProperty": "ngsi-ld:LanguageProperty",
    "MultiLineString": "geojson:MultiLineString",
    "MultiPoint": "geojson:MultiPoint",
    "MultiPolygon": "geojson:MultiPolygon",
    "Notification": "ngsi-ld:Notification",
    "Point": "geojson:Point",
    "Polygon": "geojson:Polygon",
    "Property": "ngsi-ld:Property",
    "Relationship": "ngsi-ld:Relationship",
    "Subscription": "ngsi-ld:Subscription",
    "TemporalProperty": "ngsi-ld:TemporalProperty",
    "Time": "ngsi-ld:Time",

    "accept": "ngsi-ld:accept",
	"attributeCount": "attributeCount",
	"attributeDetails": "attributeDetails",
    "attributeList": {
      "@id": "ngsi-ld:attributeList",
      "@type": "@vocab"
    },
    "attributeName": {
	  "@id": "ngsi-ld:attributeName",
      "@type": "@vocab"
	},
    "attributeNames": {
      "@id": "ngsi-ld:attributeNames",
      "@type": "@vocab"
    },
    "attributeTypes": {
      "@id": "ngsi-ld:attributeTypes",
      "@type": "@vocab"
    },
    "attributes": {
      "@id": "ngsi-ld:attributes",
      "@type": "@vocab"
    },
	"attrs": "ngsi-ld:attrs",
    "avg": {
      "@id": "ngsi-ld:avg",
      "@container": "@list"
    },
    "bbox": {
      "@container": "@list",
      "@id": "geojson:bbox"
    },
    "cacheDuration": "ngsi-ld:cacheDuration",

    "contextSourceInfo": "ngsi-ld:contextSourceInfo",
    "cooldown": "ngsi-ld:cooldown",
	"coordinates": {
      "@container": "@list",
      "@id": "geojson:coordinates"
    },
    "createdAt": {
      "@id": "ngsi-ld:createdAt",
      "@type": "DateTime"
    },
    "csf": "ngsi-ld:csf",
    "data": "ngsi-ld:data",
    "datasetId": {
      "@id": "ngsi-ld:datasetId",
      "@type": "@id"
    },
    "deletedAt": {
      "@id": "ngsi-ld:deletedAt",
      "@type": "DateTime"
    }, 
    "description": "http://purl.org/dc/terms/description",
    "detail": "ngsi-ld:detail",
    "distinctCount": {
      "@id": "ngsi-ld:distinctCount",
      "@container": "@list"
    },
    "endAt": {
      "@id": "ngsi-ld:endAt",
      "@type": "DateTime"
    },
    "endTimeAt": {
      "@id": "ngsi-ld:endTimeAt",
      "@type": "DateTime"
    },
    "endpoint": "ngsi-ld:endpoint",
    "entities": "ngsi-ld:entities",
    "entityCount": "ngsi-ld:entityCount",
    "entityId": {
      "@id": "ngsi-ld:entityId",
      "@type": "@id"
    },
    "error": "ngsi-ld:error",
    "errors": "ngsi-ld:errors",
    "expiresAt": {
      "@id": "ngsi-ld:expiresAt",
      "@type": "DateTime"
    },
    "features": {
      "@container": "@set",
      "@id": "geojson:features"
    },
    "format": "ngsi-ld:format",
    "geoQ": "ngsi-ld:geoQ",
    "geometry": "geojson:geometry",
    "geoproperty": "ngsi-ld:geoproperty",
    "georel": "ngsi-ld:georel",
    "idPattern": "ngsi-ld:idPattern",
    "information": "ngsi-ld:information",
    "instanceId": {
      "@id": "ngsi-ld:instanceId",
      "@type": "@id"
    },
    "isActive": "ngsi-ld:isActive",
    "key": "ngsi-ld:hasKey",
	"lang": "ngsi-ld:lang",
	"languageMap": {
      "@id": "ngsi-ld:hasLanguageMap",
      "@container": "@language"
    },
	"languageMaps": {
      "@id": "ngsi-ld:hasLanguageMaps",
      "@container": "@list"
    },
    "lastFailure": {
      "@id": "ngsi-ld:lastFailure",
      "@type": "DateTime"
    },
    "lastNotification": {
      "@id": "ngsi-ld:lastNotification",
      "@type": "DateTime"
    },
    "lastSuccess": {
      "@id": "ngsi-ld:lastSuccess",
      "@type": "DateTime"
    },
    "localOnly": "ngsi-ld:localOnly",
    "location": "ngsi-ld:location",
    "management": "ngsi-ld:management",
    "managementInterval": "ngsi-ld:managementInterval",
    "max": {
      "@id": "ngsi-ld:max",
      "@container": "@list"
    },
    "min": {
      "@id": "ngsi-ld:min",
      "@container": "@list"
    },
	"mode": "ngsi-ld:mode",
    "modifiedAt": {
      "@id": "ngsi-ld:modifiedAt",
      "@type": "DateTime"
    },
    "notification": "ngsi-ld:notification",
    "notificationTrigger": "ngsi-ld:notificationTrigger",
    "notifiedAt": {
      "@id": "ngsi-ld:notifiedAt",
      "@type": "DateTime"
    },
    "notifierInfo": "ngsi-ld:notifierInfo",
    "notUpdated": "ngsi-ld:notUpdated",
    "object": {
      "@id": "ngsi-ld:hasObject",
      "@type": "@id"
    },
    "objects": {
      "@id": "ngsi-ld:hasObjects",
      "@container": "@list"
    },
    "observationInterval": "ngsi-ld:observationInterval",
    "observationSpace": "ngsi-ld:observationSpace",
    "observedAt": {
      "@id": "ngsi-ld:observedAt",
      "@type": "DateTime"
    },
    "operationSpace": "ngsi-ld:operationSpace",
	"operations": "ngsi-ld:operations",
	"previousLanguageMap": {
      "@id": "ngsi-ld:hasPreviousLanguageMap",
      "@container": "@language"
    },
    "previousObject": {
      "@id": "ngsi-ld:hasPreviousObject",
      "@type": "@id"
    },
    "previousValue": "ngsi-ld:hasPreviousValue",
    "properties": "geojson:properties",
    "propertyNames": {
      "@id": "ngsi-ld:propertyNames",
      "@type": "@vocab"
    },
    "q": "ngsi-ld:q",
    "reason": "ngsi-ld:reason",
	"receiverInfo": "ngsi-ld:receiverInfo",
	"refreshRate": "ngsi-ld:refreshRate",
	"registrationId": "ngsi-ld:registrationId",
    "registrationName": "ngsi-ld:registrationName",
    "relationshipNames": {
      "@id": "ngsi-ld:relationshipNames",
      "@type": "@vocab"
    },
	"scope": "ngsi-ld:scope",
	"scopeQ": "ngsi-ld:scopeQ",
	"showChanges": "ngsi-ld:showChanges",
    "startAt": {
      "@id": "ngsi-ld:startAt",
      "@type": "DateTime"
    },
    "status": "ngsi-ld:status",
    "stddev": {
      "@id": "ngsi-ld:stddev",
      "@container": "@list"
    },
    "subscriptionId": {
      "@id": "ngsi-ld:subscriptionId",
      "@type": "@id"
    },
    "subscriptionName": "ngsi-ld:subscriptionName",
    "success": {
      "@id": "ngsi-ld:success",
      "@type": "@id"
    },
    "sum": {
      "@id": "ngsi-ld:sum",
      "@container": "@list"
    },
    "sumsq": {
      "@id": "ngsi-ld:sumsq",
      "@container": "@list"
    },
    "sysAttrs": "ngsi-ld:sysAttrs",
    "temporalQ": "ngsi-ld:temporalQ",
	"tenant": {
      "@id": "ngsi-ld:tenant",
      "@type": "@id"
    },
    "throttling": "ngsi-ld:throttling",
    "timeAt": {
      "@id": "ngsi-ld:timeAt",
      "@type": "DateTime"
    },
    "timeInterval": "ngsi-ld:timeInterval",
    "timeout": "ngsi-ld:timeout",
    "timeproperty": "ngsi-ld:timeproperty",
    "timerel": "ngsi-ld:timerel",
    "timesFailed": "ngsi-ld:timesFailed",
    "timesSent": "ngsi-ld:timesSent",
	"title": "http://purl.org/dc/terms/title",
    "totalCount": {
      "@id": "ngsi-ld:totalCount",
      "@container": "@list"
    },
    "triggerReason": "ngsi-ld:triggerReason",
	"typeList": {
      "@id": "ngsi-ld:typeList",
      "@type": "@vocab"
    },
    "typeName": {
      "@id": "ngsi-ld:typeName",
      "@type": "@vocab"
    },
    "typeNames": {
      "@id": "ngsi-ld:typeNames",
      "@type": "@vocab"
    },
    "unchanged": "ngsi-ld:unchanged",
    "unitCode": "ngsi-ld:unitCode",
    "updated": "ngsi-ld:updated",
    "uri": "ngsi-ld:uri",
    "value": "ngsi-ld:hasValue",
    "values": {
      "@id": "ngsi-ld:hasValues",
      "@container": "@list"
    },
    "watchedAttributes": {
      "@id": "ngsi-ld:watchedAttributes",
      "@type": "@vocab"
    },
    "@vocab": "https://uri.etsi.org/ngsi-ld/default-context/"
  }
}
'::jsonb, 'hosted');