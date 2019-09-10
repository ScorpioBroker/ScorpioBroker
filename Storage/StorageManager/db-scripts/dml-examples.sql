begin;

truncate entity;

-- source: vehicle.jsonld
insert into Entity (id, data) values ('urn:ngsi-ld:Vehicle:A4567', 
'
{
  "@id": "urn:ngsi-ld:Vehicle:A4567",
  "http://example.org/vehicle/brandName": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "Mercedes"
        }
      ]
    }
  ],
  "https://uri.etsi.org/ngsi-ld/createdAt": [
    {
      "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
      "@value": "2017-07-29T12:00:04"
    }
  ],
  "http://example.org/common/isParked": [
    {
      "https://uri.etsi.org/ngsi-ld/hasObject": [
        {
          "@id": "urn:ngsi-ld:OffStreetParking:Downtown1"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/observedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2017-07-29T12:00:04"
        }
      ],
      "http://example.org/common/providedBy": [
        {
          "https://uri.etsi.org/ngsi-ld/hasObject": [
            {
              "@id": "urn:ngsi-ld:Person:Bob"
            }
          ],
          "@type": [
            "https://uri.etsi.org/ngsi-ld/Relationship"
          ]
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Relationship"
      ]
    }
  ],
  "https://uri.etsi.org/ngsi-ld/location": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "{ \"type\":\"Point\", \"coordinates\":[ -8.5, 41.2 ] }"
        }
      ]
    }
  ],
  "http://example.org/vehicle/speed": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": 80
        }
      ]
    }
  ],
  "@type": [
    "http://example.org/vehicle/Vehicle"
  ]
}
'
);

-- testing json types...
-- source: all_datatypes.jsonld
insert into Entity (id, data) values ('urn:ngsi-ld:Test:all_datatypes', 
' 
  {
    "http://example.org/arrayExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": 999
          },
          {
            "@value": true
          },
          {
            "@value": "a"
          },
          {
            "@value": "b"
          },
          {
            "@value": "Foo"
          },
          {
            "https://example.org/streetAddress": [
              {
                "@value": "Franklinstrasse"
              }
            ]
          }
        ]
      }
    ],
    "http://example.org/dateExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@type": "https://uri.etsi.org/ngsi-ld/Date",
            "@value": "2018-12-04"
          }
        ]
      }
    ],
    "http://example.org/dateTimeExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
            "@value": "2018-12-04T12:00:00Z"
          }
        ]
      }
    ],
    "http://example.org/falseExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": false
          }
        ]
      }
    ],
    "@id": "urn:ngsi-ld:Test:all_datatypes",
    "http://example.org/manyLevelsExample": [
      {
        "http://example.org/sub1": [
          {
            "https://uri.etsi.org/ngsi-ld/hasObject": [
              {
                "@id": "urn:ngsi-ld:B"
              }
            ],
            "http://example.org/sub2": [
              {
                "http://example.org/sub3": [
                  {
                    "@type": [
                      "https://uri.etsi.org/ngsi-ld/Property"
                    ],
                    "https://uri.etsi.org/ngsi-ld/hasValue": [
                      {
                        "@value": "D"
                      }
                    ]
                  }
                ],
                "@type": [
                  "https://uri.etsi.org/ngsi-ld/Property"
                ],
                "https://uri.etsi.org/ngsi-ld/hasValue": [
                  {
                    "@value": "C"
                  }
                ]
              }
            ],
            "@type": [
              "https://uri.etsi.org/ngsi-ld/Relationship"
            ]
          }
        ],
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "A"
          }
        ]
      }
    ],
    "http://example.org/multiLevelObjectExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "https://example.org/streetAddress": [
              {
                "https://example.org/houseNumber": [
                  {
                    "@value": 65
                  }
                ],
                "https://example.org/streetName": [
                  {
                    "@value": "Main Street"
                  }
                ]
              }
            ]
          }
        ]
      }
    ],
    "http://example.org/numberExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": 100
          }
        ]
      }
    ],
    "http://example.org/objectExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "https://example.org/addressLocality": [
              {
                "@value": "Duck Village"
              }
            ],
            "https://example.org/addressRegion": [
              {
                "@value": "Metropolis"
              }
            ],
            "https://example.org/postalCode": [
              {
                "@value": 42000
              }
            ],
            "https://example.org/streetAddress": [
              {
                "@value": "Main Street 65"
              }
            ]
          }
        ]
      }
    ],
    "http://example.org/observedAtDateTimeExample": [
      {
        "https://uri.etsi.org/ngsi-ld/observedAt": [
          {
            "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
            "@value": "2018-12-04T12:00:00Z"
          }
        ],
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "Foo"
          }
        ]
      }
    ],
    "http://example.org/otherValueExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": true
          }
        ]
      }
    ],
    "http://example.org/relationshipExample": [
      {
        "https://uri.etsi.org/ngsi-ld/hasObject": [
          {
            "@id": "urn:ngsi-ld:relationshipExample"
          }
        ],
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Relationship"
        ]
      }
    ],
    "http://example.org/stringExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "Mercedes"
          }
        ]
      }
    ],
    "http://example.org/timeExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@type": "https://uri.etsi.org/ngsi-ld/Time",
            "@value": "12:00:00Z"
          }
        ]
      }
    ],
    "http://example.org/topLevelExample": [
      {
        "http://example.org/subPropertyExample": [
          {
            "@type": [
              "https://uri.etsi.org/ngsi-ld/Property"
            ],
            "https://uri.etsi.org/ngsi-ld/hasValue": [
              {
                "@value": 5
              }
            ]
          }
        ],
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": 10
          }
        ]
      }
    ],
    "http://example.org/trueExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": true
          }
        ]
      }
    ],
    "@type": [
      "urn:ngsi-ld:Test"
    ],
    "http://example.org/uriExample": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "http://www.example.com"
          }
        ]
      }
    ]
  }

'
);

-- mixing properties, relationships and types
-- here attr1 is a property and attr2 is a relationship
insert into Entity (id, data) values ('urn:ngsi-ld:Test:entity2', 
'{
    "@id": "urn:ngsi-ld:Test:entity2",
    "@type": [
      "https://json-ld.org/playground/Test"
    ],
    "http://example.org/attr1": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "urn:ngsi-ld:test"
          }
        ]
      }
    ],
    "http://example.org/attr2": [
      {
        "https://uri.etsi.org/ngsi-ld/hasObject": [
          {
            "@id": "urn:ngsi-ld:test"
          }
        ],
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Relationship"
        ]
      }
    ]
  }'
);

-- here attr2 is a property and attr1 is a relationship
insert into Entity (id, data) values ('urn:ngsi-ld:Test:entity3', 
'{
    "@id": "urn:ngsi-ld:Test:entity3",
    "@type": [
      "https://json-ld.org/playground/Test"
    ],
    "http://example.org/attr1": [
      {
        "https://uri.etsi.org/ngsi-ld/hasObject": [
          {
            "@id": "urn:ngsi-ld:test"
          }
        ],
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Relationship"
        ]
      }
    ],
    "http://example.org/attr2": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "urn:ngsi-ld:test"
          }
        ]
      }
    ]
  }
'
);

-- here attr1 has an invalid ngsi-ld type (should not exist in the database)
insert into Entity (id, data) values ('urn:ngsi-ld:Test:entity4', 
'{
    "@id": "urn:ngsi-ld:Test:entity4",
    "@type": [
      "https://json-ld.org/playground/Test"
    ],
    "http://example.org/attr1": [
      {
        "@type": [
          "https://json-ld.org/playground/Invalid"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "urn:ngsi-ld:test"
          }
        ]
      }
    ]
  }'
);

-- here attr1 is a property and stores a number
insert into Entity (id, data) values ('urn:ngsi-ld:Test:entity5', 
'{
    "@id": "urn:ngsi-ld:Test:entity5",
    "@type": [
      "https://json-ld.org/playground/Test"
    ],
    "http://example.org/attr1": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/Property"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": 10
          }
        ]
      }
    ]
  }'
);

-- geographical
-- "@context": "http://forge.etsi.org/gitlab/NGSI-LD/NGSI-LD/raw/master/coreContext/ngsi-ld-core-context.jsonld",

insert into Entity (id, data) values ('urn:ngsi-ld:Test:NecLabsHeidelberg', 
'{
    "@id": "urn:ngsi-ld:Test:NecLabsHeidelberg",
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "{ \"type\": \"Point\", \"coordinates\": [ 8.684783577919006, 49.406131991436396 ] }"
          }
        ]
      }
    ],
    "@type": [
      "https://json-ld.org/playground/Test"
    ]
  }'
);

insert into Entity (id, data) values ('urn:ngsi-ld:Test:AldiBergheim', 
'{
    "@id": "urn:ngsi-ld:Test:AldiBergheim",
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": " { \"type\": \"Point\", \"coordinates\": [ 8.689231, 49.407524 ] }"
          }
        ]

      }
    ],
    "http://example.org/parkingLotLocation": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "{ \"type\": \"Point\", \"coordinates\": [ 8.688387, 49.407354 ] }"        
          }
        ]
      }
    ],
    "@type": [
      "https://json-ld.org/playground/Test"
    ]
  }'
);

insert into Entity (id, data) values ('urn:ngsi-ld:Test:ReweDasCarre', 
'{
    "@id": "urn:ngsi-ld:Test:ReweDasCarre",
    "@type": [
      "https://json-ld.org/playground/Test"
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":       "{ \"type\": \"Point\", \"coordinates\": [ 8.691790, 49.407366 ] }"
          }
        ]
      }
    ]
  }'
);

insert into Entity (id, data) values ('urn:ngsi-ld:Test:KarlsruherInstitutFurTechnologie', 
'{
    "@id": "urn:ngsi-ld:Test:KarlsruherInstitutFurTechnologie",
    "@type": [
      "https://json-ld.org/playground/Test"
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":     "{ \"type\": \"Point\", \"coordinates\": [ 8.416891, 49.011942 ] }"
          }
        ]
      }
    ]
  }'
);

insert into Entity (id, data) values ('urn:ngsi-ld:Test:HeidelbergCity', 
'{
    "@id": "urn:ngsi-ld:Test:HeidelbergCity",
    "@type": [
      "https://json-ld.org/playground/Test"
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
                "{ \"type\": \"Polygon\", \"coordinates\": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] }" 
         }
        ]
      }
    ]
  }'
);

insert into Entity (id, data) values ('urn:ngsi-ld:Test:PathToNecParkingLot', 
'{
    "@id": "urn:ngsi-ld:Test:PathToNecParkingLot",
    "@type": [
      "https://json-ld.org/playground/Test"
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
          "{ \"type\": \"LineString\", \"coordinates\": [[8.684746026992798,49.406154680259746],[8.68467628955841,49.40620703904283],[8.684748709201813,49.40628732240177],[8.684737980365753,49.40635364333839]] }" 
          }
        ]    
      }
    ]
  }'
);

insert into Entity (id, data) values ('urn:ngsi-ld:Test:NecLabsHeidelbergBuilding', 
'{
    "@id": "urn:ngsi-ld:Test:NecLabsHeidelbergBuilding",
    "@type": [
      "https://json-ld.org/playground/Test"
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
              "{ \"type\": \"Polygon\", \"coordinates\": [[[8.684628009796143,49.406062179606515],[8.685507774353027,49.4062262372493],[8.68545413017273,49.40634491690448],[8.684579730033875,49.40617736907259],[8.684628009796143,49.406062179606515]]] }" 
          }
        ]
      }
            
    ]
  }'
);

insert into Entity (id, data) values ('urn:ngsi-ld:Test:DroneAreaCoverage', 
'{
    "@id": "urn:ngsi-ld:Test:DroneAreaCoverage",
    "@type": [
      "https://json-ld.org/playground/Test"
    ],
    "https://uri.etsi.org/ngsi-ld/location": [
      {
        "@type": [
          "https://uri.etsi.org/ngsi-ld/GeoProperty"
        ],
        "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value": "{ \"type\": \"Polygon\", \"coordinates\": [[[8.685226142406464,49.406259397770064],[8.685590922832489,49.406259397770064],[8.685590922832489,49.40641472833234],[8.685226142406464,49.40641472833234],[8.685226142406464,49.406259397770064]]] }" 
          }
        ]
      }
    ]
  }'
);



insert into Entity (id, data) values ('urn:ngsi-ld:Test:GeometryTypes', 
'{
  "@id": "urn:ngsi-ld:Test:GeometryTypes",
  "@type": [
    "https://json-ld.org/playground/Test"
  ],
  "http://example.org/lineStringExample": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
              "{ \"type\": \"LineString\", \"coordinates\": [ [100.0, 0.0], [101.0, 1.0] ] }"
          }
        ]
    }
  ],
  "http://example.org/multiLineStringExample": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
              "{ \"type\": \"MultiLineString\", \"coordinates\": [ [ [100.0, 0.0], [101.0, 1.0] ], [ [102.0, 2.0], [103.0, 3.0] ] ] }"
          }
        ]
    }
  ],
  "http://example.org/multiPointsExample": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
              "{ \"type\": \"MultiPoint\", \"coordinates\": [ [100.0, 0.0], [101.0, 1.0] ] }"
          }
        ]
    }
  ],
  "http://example.org/multiPolygonExample": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
              "{ \"type\": \"MultiPolygon\", \"coordinates\": [ [ [ [102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0] ] ], [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ], [ [100.2, 0.2], [100.2, 0.8], [100.8, 0.8], [100.8, 0.2], [100.2, 0.2] ] ] ] }"
          }
        ]
    }
  ],
  "http://example.org/pointExample": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
            "{ \"type\": \"Point\", \"coordinates\": [100.0, 0.0] }"
          }
        ]
    }
  ],
  "http://example.org/polygonExample": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
              "{ \"type\": \"Polygon\", \"coordinates\": [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ] }"
          }
        ]
    }
  ],
  "http://example.org/polygonWithHolesExample": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
              "{ \"type\": \"Polygon\", \"coordinates\": [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ], [ [100.8, 0.8], [100.8, 0.2], [100.2, 0.2], [100.2, 0.8], [100.8, 0.8] ] ] }"
          }
        ]
    }
  ]

}'
);



commit;

