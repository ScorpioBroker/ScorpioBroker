begin;

delete from csource where id like 'urn:ngsi-ld:ContextSourceRegistration:infotest%';

insert into csource (id, data) values (
'urn:ngsi-ld:ContextSourceRegistration:infotest_type',
'
  {
    "@id": "urn:ngsi-ld:ContextSourceRegistration:infotest_type", 
    "https://uri.etsi.org/ngsi-ld/information": [
      {
        "https://uri.etsi.org/ngsi-ld/entities": [
          {
            "@type": [
              "http://example.org/vehicle/Vehicle"
            ]
          }
        ]
      }
    ], 
    "https://uri.etsi.org/ngsi-ld/endpoint": [
      {
        "@value": "http://my.csource.org:1026"
      }
    ], 
    "@type": [
      "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
    ]
  }
'
);

insert into csource (id, data) values (
'urn:ngsi-ld:ContextSourceRegistration:infotest_type_id',
'
  {
    "@id": "urn:ngsi-ld:ContextSourceRegistration:infotest_type_id", 
    "https://uri.etsi.org/ngsi-ld/information": [
      {
        "https://uri.etsi.org/ngsi-ld/entities": [
          {
            "@id": "urn:ngsi-ld:Vehicle:A456", 
            "@type": [
              "http://example.org/vehicle/Vehicle"
            ]
          }
        ]
      }
    ], 
    "https://uri.etsi.org/ngsi-ld/endpoint": [
      {
        "@value": "http://my.csource.org:1026"
      }
    ], 
    "@type": [
      "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
    ]
  }
');

insert into csource (id, data) values (
'urn:ngsi-ld:ContextSourceRegistration:infotest_type_multiple_ids',
'
 {
    "@id": "urn:ngsi-ld:ContextSourceRegistration:infotest_type_multiple_ids", 
    "https://uri.etsi.org/ngsi-ld/information": [
      {
        "https://uri.etsi.org/ngsi-ld/entities": [
          {
            "@id": "urn:ngsi-ld:Vehicle:A456", 
            "@type": [
              "http://example.org/vehicle/Vehicle"
            ]
          }, 
          {
            "@id": "urn:ngsi-ld:Vehicle:A789", 
            "@type": [
              "http://example.org/vehicle/Vehicle"
            ]
          }
        ]
      }
    ], 
    "https://uri.etsi.org/ngsi-ld/endpoint": [
      {
        "@value": "http://my.csource.org:1026"
      }
    ], 
    "@type": [
      "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
    ]
  }
');

insert into csource (id, data) values (
'urn:ngsi-ld:ContextSourceRegistration:infotest_type_idPattern',
'
  {
    "@id": "urn:ngsi-ld:ContextSourceRegistration:infotest_type_idPattern", 
    "https://uri.etsi.org/ngsi-ld/information": [
      {
        "https://uri.etsi.org/ngsi-ld/entities": [
          {
            "@type": [
              "http://example.org/vehicle/Vehicle"
            ], 
            "https://uri.etsi.org/ngsi-ld/idPattern": [
              {
                "@value": "urn:ngsi-ld:Vehicle.*"
              }
            ]
          }
        ]
      }
    ], 
    "https://uri.etsi.org/ngsi-ld/endpoint": [
      {
        "@value": "http://my.csource.org:1026"
      }
    ], 
    "@type": [
      "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
    ]
  }
');

insert into csource (id, data) values (
'urn:ngsi-ld:ContextSourceRegistration:infotest_type_id_idPattern',
'
  {
    "@id": "urn:ngsi-ld:ContextSourceRegistration:infotest_type_id_idPattern", 
    "https://uri.etsi.org/ngsi-ld/information": [
      {
        "https://uri.etsi.org/ngsi-ld/entities": [
          {
            "@id": "urn:ngsi-ld:Vehicle:A456", 
            "@type": [
              "http://example.org/vehicle/Vehicle"
            ], 
            "https://uri.etsi.org/ngsi-ld/idPattern": [
              {
                "@value": "urn:ngsi-ld:Vehicle.*"
              }
            ]
          }
        ]
      }
    ], 
    "https://uri.etsi.org/ngsi-ld/endpoint": [
      {
        "@value": "http://my.csource.org:1026"
      }
    ], 
    "@type": [
      "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
    ]
  }
');

insert into csource (id, data) values (
'urn:ngsi-ld:ContextSourceRegistration:infotest_attributes',
'   {
    "@id": "urn:ngsi-ld:ContextSourceRegistration:infotest_attributes", 
    "https://uri.etsi.org/ngsi-ld/information": [
      {
        "https://uri.etsi.org/ngsi-ld/properties": [
          {
            "@id": "http://example.org/vehicle/brandName"
          }, 
          {
            "@id": "http://example.org/vehicle/speed"
          }
        ]
      }, 
      {
        "https://uri.etsi.org/ngsi-ld/properties": [
          {
            "@id": "http://example.org/vehicle/fuelType"
          }
        ]
      }, 
      {
        "https://uri.etsi.org/ngsi-ld/relationships": [
          {
            "@id": "http://example.org/common/isParked"
          }
        ]
      }
    ], 
    "https://uri.etsi.org/ngsi-ld/endpoint": [
      {
        "@value": "http://my.csource.org:1026"
      }
    ], 
    "@type": [
      "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
    ]
  }

');

insert into csource (id, data) values (
'urn:ngsi-ld:ContextSourceRegistration:infotest_entity_type_attributes',
'
  {
    "@id": "urn:ngsi-ld:ContextSourceRegistration:infotest_entity_type_attributes", 
    "https://uri.etsi.org/ngsi-ld/information": [
      {
        "https://uri.etsi.org/ngsi-ld/properties": [
          {
            "@id": "http://example.org/vehicle/brandName"
          }, 
          {
            "@id": "http://example.org/vehicle/speed"
          }
        ], 
        "https://uri.etsi.org/ngsi-ld/entities": [
          {
            "@type": [
              "http://example.org/vehicle/Vehicle"
            ]
          }
        ], 
        "https://uri.etsi.org/ngsi-ld/relationships": [
          {
            "@id": "http://example.org/common/isParked"
          }
        ]
      }
    ], 
    "https://uri.etsi.org/ngsi-ld/endpoint": [
      {
        "@value": "http://my.csource.org:1026"
      }
    ], 
    "@type": [
      "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
    ]
  }
');

insert into csource (id, data) values (
'urn:ngsi-ld:ContextSourceRegistration:infotest_many_elements',
'
 {
    "@id": "urn:ngsi-ld:ContextSourceRegistration:infotest_many_elements", 
    "https://uri.etsi.org/ngsi-ld/information": [
      {
        "https://uri.etsi.org/ngsi-ld/properties": [
          {
            "@id": "http://example.org/vehicle/brandName"
          }, 
          {
            "@id": "http://example.org/vehicle/speed"
          }
        ], 
        "https://uri.etsi.org/ngsi-ld/entities": [
          {
            "@type": [
              "http://example.org/vehicle/Vehicle"
            ]
          }
        ]
      }, 
      {
        "https://uri.etsi.org/ngsi-ld/relationships": [
          {
            "@id": "http://example.org/common/isParked"
          }
        ], 
        "https://uri.etsi.org/ngsi-ld/entities": [
          {
            "@type": [
              "http://example.org/vehicle/Vehicle"
            ]
          }
        ]
      }, 
      {
        "https://uri.etsi.org/ngsi-ld/properties": [
          {
            "@id": "http://example.org/room/temperature"
          }
        ], 
        "https://uri.etsi.org/ngsi-ld/entities": [
          {
            "@type": [
              "http://example.org/room/Room"
            ]
          }
        ]
      }, 
      {
        "https://uri.etsi.org/ngsi-ld/properties": [
          {
            "@id": "http://example.org/vehicle/brandName"
          }
        ], 
        "https://uri.etsi.org/ngsi-ld/entities": [
          {
            "@id": "urn:ngsi-ld:Vehicle:A456", 
            "@type": [
              "http://example.org/vehicle/Vehicle"
            ]
          }
        ]
      }
    ], 
    "https://uri.etsi.org/ngsi-ld/endpoint": [
      {
        "@value": "http://my.csource.org:1026"
      }
    ], 
    "@type": [
      "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
    ]
  }
');

commit;