This section covers all the basic configuration needed for the Scorpio broker. This can be used as the basic template for the various micro-services of the Scorpio.

Description of various configuration parameters
**************************************************
1. server:- In this, the user can define the various server related parameters like **port** and the maximum **number of threads** for tomcat server.

.. code-block:: JSON

 server:
  port: 1026
  tomcat:
    max:
      threads: 50
	  
2. Entity:- Used in entity manager to define the various topics for the Kafka.

.. code-block:: JSON

 entity:
   topic: ENTITY
   create:
    topic: ENTITY_CREATE
   append:
    topic: ENTITY_APPEND
   update:
    topic: ENTITY_UPDATE
   delete:
    topic: ENTITY_DELETE
   index:
    topic: ENTITY_INDEX

3. batchoperations:- Used to define the limit for the various CRUD operations.

.. code-block:: JSON

 batchoperations:
   maxnumber:
    create: 1000
    update: 1000
    upsert: 1000
    delete: 1000

4. bootstrap:- Used to define the path for the Kafka broker.

.. code-block:: JSON

 bootstrap:
   servers: localhost:9092

5. csources:- Used to define the topic for the context source registration.

.. code-block:: JSON

  registration:
    topic: CONTEXT_REGISTRY

6. append:- Used to define the entity append overwrite options.

.. code-block:: JSON

 append:
   overwrite: noOverwrite

7. management:-

.. code-block:: JSON

 management:
  endpoints:
    web:
      exposure:
        include: "*"
  endpoint:
    restart:
      enabled: true

8. spring:- Used to define the basic details of the project like service name as well as to provide the configuration details for Kafka, flyway, data source, and cloud.

.. code-block:: JSON

 spring:
  application:
    name: query-manager
  main:
    lazy-initialization: true
  kafka:
    admin:
      properties:
        cleanup:
          policy: compact
  flyway:
    baselineOnMigrate: true
  cloud:
    stream:
      kafka:
        binder:
          brokers: localhost:9092
      bindings:
         ATCONTEXT_WRITE_CHANNEL:
          destination: ATCONTEXT
          contentType: application/json
  datasource:
    url: "jdbc:postgresql://127.0.0.1:5432/ngb?ApplicationName=ngb_querymanager"
    username: ngb
    password: ngb
    hikari:
      minimumIdle: 5
      maximumPoolSize: 20
      idleTimeout: 30000
      poolName: SpringBootHikariCP
      maxLifetime: 2000000
      connectionTimeout: 30000


9. query:- Used in query manager to define the Kafka topic for data query.

.. code-block:: JSON

 query:
  topic: QUERY
  result:
    topic: QUERY_RESULT

10. atcontext:- Used to define the URL for the context.

.. code-block:: JSON

 atcontext:
  url: http://localhost:9090/ngsi-ld/contextes/

11. Key:- Used to define the file for the deserialization.

.. code-block:: JSON

 key:
  deserializer: org.apache.kafka.common.serialization.StringDeserializer

12. reader:- Used to configure the database to the Scorpio broker, required to perform all the read operations.

.. code-block:: JSON

 reader:
  enabled: true
  datasource:
    url: "jdbc:postgresql://localhost:5432/ngb?ApplicationName=ngb_storagemanager_reader"
    username: ngb
    password: ngb
    hikari:
      minimumIdle: 5
      maximumPoolSize: 20
      idleTimeout: 30000
      poolName: SpringBootHikariCP_Reader
      maxLifetime: 2000000
      connectionTimeout: 30000

13. writer:- Used to configure the database to the Scorpio broker, required to perform all the write operations.

.. code-block:: JSON

 writer:
  enabled: true
  datasource:
    url: "jdbc:postgresql://localhost:5432/ngb?ApplicationName=ngb_storagemanager_writer"
    username: ngb
    password: ngb
    hikari:
      minimumIdle: 5
      maximumPoolSize: 20
      idleTimeout: 30000
      poolName: SpringBootHikariCP_Writer
      maxLifetime: 2000000
      connectionTimeout: 30000
