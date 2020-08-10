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

3. batchoperations:- Used to define the limit for the various CRUD operations.

.. code-block:: JSON

4. bootstrap:- Used to define the path for the Kafka broker.

.. code-block:: JSON

5. csources:- Used to define the topic for the context source registration.

.. code-block:: JSON

6. append:- Used to define the entity append overwrite options.

.. code-block:: JSON

7. management:-

.. code-block:: JSON

8. spring:- Used to define the basic details of the project like service name as well as to provide the configuration details for Kafka, flyway, data source, and cloud.

.. code-block:: JSON

9. query:- Used in query manager to define the Kafka topic for data query.

.. code-block:: JSON

10. atcontext:- Used to define the URL for the context.

.. code-block:: JSON

11. Key:- Used to define the file for the deserialization.

.. code-block:: JSON

12. reader:- Used to configure the database to the Scorpio broker, required to perform all the read operations.

.. code-block:: JSON

13. writer:- Used to configure the database to the Scorpio broker, required to perform all the write operations.

.. code-block:: JSON