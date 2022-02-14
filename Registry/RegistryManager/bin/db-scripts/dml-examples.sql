begin;

truncate csource cascade;

-- source: csource-expanded.jsonld
insert into CSource (id, data) values ('urn:ngsi-ld:ContextSourceRegistration:csr1a3456', 
'
{
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
      ], 
      "https://uri.etsi.org/ngsi-ld/relationships": [
        {
          "@id": "http://example.org/common/isParked"
        }
      ]
    }, 
    {
      "https://uri.etsi.org/ngsi-ld/entities": [
        {
          "@type": [
            "http://example.org/parking/OffStreetParking"
          ], 
          "https://uri.etsi.org/ngsi-ld/idPattern": [
            {
              "@value": ".*downtown$"
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
  "https://uri.etsi.org/ngsi-ld/expires": [
    {
      "@type": "https://uri.etsi.org/ngsi-ld/DateTime", 
      "@value": "2030-11-29T14:53:15"
    }
  ], 
  "https://uri.etsi.org/ngsi-ld/description": [
    {
      "@value": "DescriptionExample"
    }
  ], 
  "https://uri.etsi.org/ngsi-ld/name": [
    {
      "@value": "NameExample"
    }
  ], 
  "https://uri.etsi.org/ngsi-ld/location": [
    {
      "@value": "{ \"type\": \"Point\", \"coordinates\": [ 8.684783577919006, 49.406131991436396 ] }"
    }
  ], 
  "@id": "urn:ngsi-ld:ContextSourceRegistration:csr1a3456", 
  "@type": [
    "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
  ]
}
'
);

insert into CSource (id, data) values ('urn:ngsi-ld:ContextSourceRegistration:csr1a3456_typeonly', 
'
{
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
  "https://uri.etsi.org/ngsi-ld/expires": [
    {
      "@type": "https://uri.etsi.org/ngsi-ld/DateTime", 
      "@value": "2030-11-29T14:53:15"
    }
  ], 
  "https://uri.etsi.org/ngsi-ld/description": [
    {
      "@value": "DescriptionExample"
    }
  ], 
  "https://uri.etsi.org/ngsi-ld/name": [
    {
      "@value": "NameExample"
    }
  ], 
  "https://uri.etsi.org/ngsi-ld/location": [
    {
      "@value": "{ \"type\": \"Point\", \"coordinates\": [ 8.684783577919006, 49.406131991436396 ] }"
    }
  ], 
  "@id": "urn:ngsi-ld:ContextSourceRegistration:csr1a3456_typeonly", 
  "@type": [
    "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
  ]
}
'
);

-- Federation and Geographical csources

-- Open the file "tests-geo-reg-geojson.io-source.json" in http://geojson.io to graphically see the examples below

-- Common attributes
select '
  "https://uri.etsi.org/ngsi-ld/information": [
    {
      "https://uri.etsi.org/ngsi-ld/entities": [
        {
          "https://uri.etsi.org/ngsi-ld/idPattern": [
            {
              "@value": ".*"
            }
          ],
          "@type": [
            "http://example.org/vehicle/Vehicle"
          ]
        }
      ]
    }
  ],
  "https://uri.etsi.org/ngsi-ld/expires": [
    {
      "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
      "@value": "2030-11-29T14:53:15"
    }
  ],
  "@type": [
    "https://uri.etsi.org/ngsi-ld/ContextSourceRegistration"
  ],
  "https://uri.etsi.org/ngsi-ld/name": [
    {
      "@value": "NameExample"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/description": [
    {
      "@value": "DescriptionExample"
    }
  ]
' AS common_attributes \gset

\set quoted_common_attributes '\'' :common_attributes '\''

-- insert

-- FedBroker
insert into CSource (id, data) values ('urn:ngsi-ld:TestFed:FedBroker', 
('{
  "@id": "urn:ngsi-ld:TestFed:FedBroker",
  "https://uri.etsi.org/ngsi-ld/location": [
    {
      "@value": "{ \"type\": \"Polygon\", \"coordinates\": [ [ [ 8.2342529296875, 49.21759710517596 ], [ 8.957977294921875, 49.21759710517596 ], [ 8.957977294921875, 49.681846899401286 ], [ 8.2342529296875, 49.681846899401286 ], [ 8.2342529296875, 49.21759710517596 ] ] ] }"
    }
  ],
  "https://uri.etsi.org/ngsi-ld/endpoint": [
    {
      "@value": "http://localhost:10001"
    }
  ],
' || :quoted_common_attributes || '
}')::jsonb
);

-- Sinsheim + wald (almost neckar)
insert into CSource (id, data) values ('urn:ngsi-ld:TestFedReg:Broker1', 
('{
    "@id": "urn:ngsi-ld:TestFedReg:Broker1",
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@value": "{ \"type\": \"Polygon\", \"coordinates\": [ [ [ 8.751983642578125, 49.23463749585336 ], [ 8.93463134765625, 49.23463749585336 ], [ 8.93463134765625, 49.35286116650209 ], [ 8.751983642578125, 49.35286116650209 ], [ 8.751983642578125, 49.23463749585336 ] ] ] }"
      }
    ],
  "https://uri.etsi.org/ngsi-ld/endpoint": [
    {
      "@value": "http://localhost:10002"
    }
  ],
' || :quoted_common_attributes || '
}')::jsonb
);


-- Worms
insert into CSource (id, data) values ('urn:ngsi-ld:TestFedReg:Broker2', 
('{
    "@id": "urn:ngsi-ld:TestFedReg:Broker2",
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@value": "{ \"type\": \"Polygon\", \"coordinates\": [ [ [ 8.28643798828125, 49.60715036117516 ], [ 8.399047851562498, 49.60715036117516 ], [ 8.399047851562498, 49.664961282899974 ], [ 8.28643798828125, 49.664961282899974 ], [ 8.28643798828125, 49.60715036117516 ] ] ] }"
      }
    ],
  "https://uri.etsi.org/ngsi-ld/endpoint": [
    {
      "@value": "http://localhost:10003"
    }
  ],
' || :quoted_common_attributes || '
}')::jsonb
);


-- Heidelberg
insert into CSource (id, data) values ('urn:ngsi-ld:TestFedReg:Broker3', 
('{
    "@id": "urn:ngsi-ld:TestFedReg:Broker3",
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@value": "{ \"type\": \"Polygon\", \"coordinates\": [ [ [ 8.5968017578125, 49.384160800744986 ], [ 8.76708984375, 49.384160800744986 ], [ 8.76708984375, 49.44134289100633 ], [ 8.5968017578125, 49.44134289100633 ], [ 8.5968017578125, 49.384160800744986 ] ] ] }"
      }
    ],
  "https://uri.etsi.org/ngsi-ld/endpoint": [
    {
      "@value": "http://localhost:10004"
    }
  ],
' || :quoted_common_attributes || '
}')::jsonb
);


-- Mannheim + Weinheim + HD + wald near Sinsheim
insert into CSource (id, data) values ('urn:ngsi-ld:TestFedReg:Broker4', 
('{
    "@id": "urn:ngsi-ld:TestFedReg:Broker4",
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@value": "{ \"type\": \"Polygon\", \"coordinates\": [ [ [ 8.34686279296875, 49.298262740098345 ], [ 8.850860595703125, 49.298262740098345 ], [ 8.850860595703125, 49.55283460376055 ], [ 8.34686279296875, 49.55283460376055 ], [ 8.34686279296875, 49.298262740098345 ] ] ] }"
      }
    ],
  "https://uri.etsi.org/ngsi-ld/endpoint": [
    {
      "@value": "http://localhost:10005"
    }
  ],
' || :quoted_common_attributes || '
}')::jsonb
);

commit;

