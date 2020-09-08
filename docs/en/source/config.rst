*****************************
Config parameters for Scorpio
*****************************

This section covers all the basic configuration needed for the Scorpio broker. This can be used as the basic template for the various micro-services of the Scorpio.

Description of various configuration parameters
###############################################

1. server:- In this, the user can define the various server related parameters like **port** and the maximum **number of threads** for the internal tomcat server. This is related to the microservice communication. Be careful with changes.

.. code-block:: JSON

 server:
  port: XXXX
  tomcat:
    max:
      threads: XX
	  
2. Entity Topics:- These are the topics which are used for the internal communication of Scorpio on Kafka. If you change this you need to change things in the source code too.

.. code-block:: JSON

 entity:
   topic: XYZ
   create:
    topic: XYZ
   append:
    topic: XYZ
   update:
    topic: XYZ
   delete:
    topic: XYZ
   index:
    topic: XYZ

3. batchoperations:- Used to define the limit for the batch operations defined by NGSI-LD operations. This is http server config and hardware related. Change with caution.

.. code-block:: JSON

 batchoperations:
   maxnumber:
    create: XXXX
    update: XXXX
    upsert: XXXX
    delete: XXXX

4. bootstrap:- Used to define the URL for the Kafka broker. Change only if you have changed the setup of Kafka

.. code-block:: JSON

 bootstrap:
   servers: URL

5. csources Topics:- These are the topics which are used for the internal communication of Scorpio on Kafka. If you change this you need to change things in the source code too.

.. code-block:: JSON

  registration:
    topic: CONTEXT_REGISTRY

6. append:- Used to define the entity append overwrite option. Change with only with extreme caution.

.. code-block:: JSON

 append:
   overwrite: noOverwrite


7. spring:- Used to define the basic details of the project like service name as well as to provide the configuration details for Kafka, flyway, data source, and cloud. DO NOT CHANGE THOSE UNLESS YOU KNOW WHAT YOU ARE DOING!

.. code-block:: JSON

 spring:
  application:
    name: serviceName
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


8. query Topics:- These are the topics which are used for the internal communication of Scorpio on Kafka. If you change this you need to change things in the source code too.

.. code-block:: JSON

 query:
  topic: QUERY
  result:
    topic: QUERY_RESULT

9. atcontext:- Used to define the URL for served context by scorpio for scenarios where a mixed context is provided via a header.

.. code-block:: JSON

 atcontext:
  url: http://<ScorpioHost>:<ScorpioPort>/ngsi-ld/contextes/

10. Key:- Used to define the file for the deserialization. DO NOT CHANGE!

.. code-block:: JSON

 key:
  deserializer: org.apache.kafka.common.serialization.StringDeserializer

11. reader:- Used to configure the database to the Scorpio broker, required to perform all the read operations. This example is based on the default config for a local installed Postgres DB

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

12. writer:- Used to configure the database to the Scorpio broker, required to perform all the write operations. This example is based on the default config for a local installed Postgres DB.

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
