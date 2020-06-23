****************************
Getting a docker container 
****************************

The current maven build supports two types of docker container generations from the build using maven profiles to trigger it.

The first profile is called 'docker' and can be called like this
 
.. code-block:: bash

    mvn clean package -DskipTests -Pdocker

this will generate individual docker containers for each microservice. The corresponding docker-compose file is `docker-compose-dist.yml`


The second profile is called 'docker-aaio' (for almost all in one). This will generate one single docker container for all components of the broker except the Kafka message bus and the Postgres database.

To get the aaio version run the maven build like this 

.. code-block:: bash

    mvn clean package -DskipTests -Pdocker-aaio
 
The corresponding docker-compose file is `docker-compose-aaio.yml`

General remark for the Kafka docker image and docker-compose
============================================================

The Kafka docker container requires you to provide the environment variable `KAFKA_ADVERTISED_HOST_NAME`. This has to be changed in the docker-compose files to match your docker host IP. You can use `127.0.0.1` however this will disallow you to run Kafka in a cluster mode.

For further details please refer to https://hub.docker.com/r/wurstmeister/kafka 

Running docker build outside of Maven
=====================================

If you want to have the build of the jars separated from the docker build you need to provide certain VARS to docker. 
The following list shows all the vars and their intended value if you run docker build from the root dir

  
 - BUILD_DIR_ACS = Core/AtContextServer
 
 - BUILD_DIR_SCS = SpringCloudModules/config-server
 
 - BUILD_DIR_SES = SpringCloudModules/eureka
 
 - BUILD_DIR_SGW = SpringCloudModules/gateway
 
 - BUILD_DIR_HMG = History/HistoryManager
 
 - BUILD_DIR_QMG = Core/QueryManager
 
 - BUILD_DIR_RMG = Registry/RegistryManager
 
 - BUILD_DIR_EMG = Core/EntityManager
 
 - BUILD_DIR_STRMG = Storage/StorageManager
 
 - BUILD_DIR_SUBMG = Core/SubscriptionManager

 - JAR_FILE_BUILD_ACS = AtContextServer-${project.version}.jar
 
 - JAR_FILE_BUILD_SCS = config-server-${project.version}.jar
 
 - JAR_FILE_BUILD_SES = eureka-server-${project.version}.jar
 
 - JAR_FILE_BUILD_SGW = gateway-${project.version}.jar
 
 - JAR_FILE_BUILD_HMG = HistoryManager-${project.version}.jar
 
 - JAR_FILE_BUILD_QMG = QueryManager-${project.version}.jar
 
 - JAR_FILE_BUILD_RMG = RegistryManager-${project.version}.jar
 
 - JAR_FILE_BUILD_EMG = EntityManager-${project.version}.jar
 
 - JAR_FILE_BUILD_STRMG = StorageManager-${project.version}.jar
 
 - JAR_FILE_BUILD_SUBMG = SubscriptionManager-${project.version}.jar

 - JAR_FILE_RUN_ACS = AtContextServer.jar
 
 - JAR_FILE_RUN_SCS = config-server.jar
 
 - JAR_FILE_RUN_SES = eureka-server.jar
 
 - JAR_FILE_RUN_SGW = gateway.jar
 
 - JAR_FILE_RUN_HMG = HistoryManager.jar
 
 - JAR_FILE_RUN_QMG = QueryManager.jar
 
 - JAR_FILE_RUN_RMG = RegistryManager.jar
 
 - JAR_FILE_RUN_EMG = EntityManager.jar
 
 - JAR_FILE_RUN_STRMG = StorageManager.jar
 
 - JAR_FILE_RUN_SUBMG = SubscriptionManager.jar
