***********************************
Starting Scorpio via docker-compose 
***********************************

Start commands to copy
######################

Looking for the easiest way to start Scorpio? This is it.
::

	curl https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/development/docker-compose-aaio.yml
	sudo docker-compose -f docker-compose-aaio.yml up


Introduction
############
The easiest way to start Scorpio is to use docker-compose. We provide 2 main docker-compose files which rely on dockerhub. 
docker-compose-aaio.yml and docker-compose-dist.yml. You can use this files directly as they are to start Scorpio
When you want to run Scorpio in the distributed variant exchange the yml file in the command above.

docker-compose-aaio.yml
#######################

AAIO here stands for almost all in one. In this variant the core components of Scorpio and the Spring Cloud components are started within one container. Additional containers are only Kafka and Postgres. For testing and small to medium size deployments this is most likely what you want to use.

docker-compose-dist.yml
#######################

In this variant each Scorpio component is started in a different container. This makes it highly flexible and allows you to replace individual components or to start new instances of some core components. 

Configure docker image via environment variables
################################################

There are multiple ways to enter environment variables into docker. We will not got through all of them but only through the docker-compose files. However the scorpio relevant parts apply to all these variants. 
Configuration of Scorpio is done via the Spring Cloud configuration system. For a complete overview of the used parameters and the default values have a look at the application.yml for the AllInOneRunner here, https://github.com/ScorpioBroker/ScorpioBroker/blob/development/AllInOneRunner/src/main/resources/application-aaio.yml.
To provide a new setting you can provide those via an environment entry in the docker-compose file. The variable we want to set is called spring_args.
Since we only want to set this option for the Scorpio container we make it a sub part of the Scorpio Container entry like this 
::

	scorpio:
	  image: scorpiobroker/scorpio:scorpio-aaio_1.0.0
	  ports:
	    - "9090:9090"
	  depends_on:
	    - kafka
	    - postgres
	  environment:
	    spring_args: --maxLimit=1000

With this we would set the maximum limit for a query reply to 1000 instead of the default 500.

Be quit! docker
###############

Some docker containers can be quite noisy and you don't want all of that output. The easy solution is to add this 
::

	logging:
      driver: none

in the docker-compose file to respective container config. E.g. to make Kafka quite.
::

	kafka:
	  image: wurstmeister/kafka
	  hostname: kafka
	  ports:
	    - "9092"
	  environment:
	    KAFKA_ADVERTISED_HOST_NAME: kafka
	    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
	    KAFKA_ADVERTISED_PORT: 9092
	    KAFKA_LOG_RETENTION_MS: 10000
	    KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 5000
	  volumes:
	    - /var/run/docker.sock:/var/run/docker.sock
	  depends_on:
	    - zookeeper
	  logging:
	    driver: none

************************
Configuration Parameters
************************

Scorpio uses the Spring Cloud/Boot configuration system. This is done via the application.yml files in the corresponding folders.
The AllInOneRunner has a complete set of all available configuration options in them.

Those can be overwriten via the command line or in the docker case as described above.

+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| Config Option     | Description                                     | Default Value                                                                  | 
+===================+=================================================+================================================================================+
| atcontext.url     | the url to be used for the internal context     | http://localhost:9090/ngsi-ld/contextes/                                       | 
|                   | server                                          |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| bootstrap.servers | the host and port of the internal kafka         | kafka:9092 (default used for docker)                                           | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| broker.id a       | unique id for the broker. needed for federation | Broker1                                                                        | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| broker.parent.    | url for the parent broker in a federation setup | SELF (meaning no federation)                                                   | 
| location.url      |                                                 |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| broker.           | GeoJSON description of the coverage. used for   | empty                                                                          | 
| geoCoverage       | registration in a federation setup.             |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| defaultLimit      | The default limit for a query if no limit is    | 50                                                                             | 
|                   | provided                                        |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| maxLimit          | The maximum number of results in a query        | 500                                                                            | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| reader.datasource | If you change the postgres setup here you set   | ngb                                                                            | 
| .hikari.password  | the password                                    |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| reader.datasource | JDBC URL to postgres                            | jdbc:postgresql://postgres:5432/ngb?ApplicationName=ngb_storagemanager_reader  | 
| .hikari.url       |                                                 |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| reader.datasource | username for the postgres db                    | ngb                                                                            | 
| .hikari.username  |                                                 |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| writer.datasource | If you change the postgres setup here you set   | ngb                                                                            | 
| .hikari.password  | the password                                    |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| writer.datasource | JDBC URL to postgres                            | jdbc:postgresql://postgres:5432/ngb?ApplicationName=ngb_storagemanager_writer  | 
| .hikari.url       |                                                 |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+
| writer.datasource | username for the postgres db                    | ngb                                                                            | 
| .hikari.username  |                                                 |                                                                                | 
+-------------------+-------------------------------------------------+--------------------------------------------------------------------------------+



****************************
Building Scorpio from source
****************************

Scorpio is developed in Java using SpringCloud as microservice framework
and Apache Maven as build tool. Some of the tests require a running
Apache Kafka messagebus (further instruction are in the Setup chapter).
If you want to skip those tests you can run
``mvn clean package -DskipTests`` to just build the individual
microservices.

General Remarks on Building
###########################

Further down this document you will get exact build commands/arguments
for the different flavors. This part will give you an overview on how
the different arguments work.

Maven Profiles
--------------
There currently three available Maven build profiles 

Default
~~~~~~~
If you provide no -P argument Maven will produce individual jar files for the microservices and the AllInOneRunner with each "full" microservice packaged (this will result in ca. 500 MB size for the AllInOneRunner)

docker
~~~~~~
This will trigger the Maven to build docker containers for each
microservice.

docker-aaio
~~~~~~~~~~~
This will trigger the Maven to build one docker container, containing
the AllInOneRunner and the spring cloud components (eureka, configserver
and gateway)

Maven arguments
~~~~~~~~~~~~~~~
These arguments are provided via -D in the command line. 

skipTests
~~~~~~~~~ 
Generally recommended if you want to speed
up the build or you don't have a kafka instance running, which is
required by some of the tests. 

skipDefault 
~~~~~~~~~~~
This is a special argument for the Scorpio build. This argument will disable springs
repacking for the individual microservices and will allow for a smaller
AllInOneRunner jar file. This argument shoulnd ONLY be used in
combination with the docker-aaio profile.

Spring Profiles
---------------

Spring supports also profiles which can be activated when launching a
jar file. Currently there 3 profiles actively used in Scorpio. The
default profiles assume the default setup to be a individual
microservices. The exception is the AllInOneRunner which as default
assumes to be running in the docker-aaio setup.

Currently you should be able to run everything with a default profile
except the gateway in combination with the AllInOneRunner. In order to
use these two together you need to start the gateway with the aaio
spring profile. This can be done by attaching this to your start command
-Dspring.profiles.active=aaio.

Additonally some components have a dev profile available which is purely
meant for development purposes and should only be used for such.

Setup
#####

Scorpio requires two components to be installed.

Postgres
--------

Please download the `Postgres DB <https://www.postgresql.org/>`__ and
the `Postgis <https://postgis.net>`__ extension and follow the
instructions on the websites to set them up.

Scorpio has been tested and developed with Postgres 10.

The default username and password which Scorpio uses is "ngb". If you
want to use a different username or password you need to provide them as
parameter when starting the StorageManager and the RegistryManager.

e.g.

.. code:: console

    java -jar Storage/StorageManager/target/StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar --reader.datasource.username=funkyusername --reader.datasource.password=funkypassword

OR

.. code:: console

    java -jar Registry/RegistryManager/target/RegistryManager-<VERSIONNUMBER>-SNAPSHOT.jar --spring.datasource.username=funkyusername --spring.datasource.password=funkypassword

Don't forget to create the corresponding user ("ngb" or the different
username you chose) in postgres. It will be used by the SpringCloud
services for database connection. While in terminal, log in to the psql
console as postgres user:

.. code:: console

    sudo -u postgres psql

Then create a database "ngb":

.. code:: console

    postgres=# create database ngb;

Create a user "ngb" and make him a superuser:

.. code:: console

    postgres=# create user ngb with encrypted password 'ngb';
    postgres=# alter user ngb with superuser;

Grant privileges on database:

.. code:: console

    postgres=# grant all privileges on database ngb to ngb;

Also create an own database/schema for the Postgis extension:

.. code:: console

    postgres=# CREATE DATABASE gisdb;
    postgres=# \connect gisdb;
    postgres=# CREATE SCHEMA postgis;
    postgres=# ALTER DATABASE gisdb SET search_path=public, postgis, contrib;
    postgres=# \connect gisdb;
    postgres=# CREATE EXTENSION postgis SCHEMA postgis;

Apache Kafka
------------

Scorpio uses `Apache Kafka <https://kafka.apache.org/>`__ for the
communication between the microservices.

Scorpio has been tested and developed with Kafka version 2.12-2.1.0

Please download `Apache Kafka <https://kafka.apache.org/downloads>`__
and follow the instructions on the website.

In order to start kafka you need to start two components: Start
zookeeper with

.. code:: console

    <kafkafolder>/bin/[Windows]/zookeeper-server-start.[bat|sh] <kafkafolder>/config/zookeeper.properties

Start kafkaserver with

.. code:: console

    <kafkafolder>/bin/[Windows]/kafka-server-start.[bat|sh] <kafkafolder>/config/server.properties

For more details please visit the Kafka
`website <https://kafka.apache.org/>`__.

Getting a docker container
~~~~~~~~~~~~~~~~~~~~~~~~~~

The current maven build supports two types of docker container
generations from the build using maven profiles to trigger it.

The first profile is called 'docker' and can be called like this

.. code:: console

    sudo mvn clean package -DskipTests -Pdocker

this will generate individual docker containers for each micro service.
The corresponding docker-compose file is ``docker-compose-dist.yml``

The second profile is called 'docker-aaio' (for almost all in one). This
will generate one single docker container for all components the broker
except the kafka message bus and the postgres database.

To get the aaio version run the maven build like this

.. code:: console

    sudo mvn clean package -DskipTests -DskipDefault -Pdocker-aaio

The corresponding docker-compose file is ``docker-compose-aaio.yml``

Starting the docker container
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To start the docker container please use the corresponding
docker-compose files. I.e.

.. code:: console

    sudo docker-composer -f docker-compose-aaio.yml up

to stop the container properly execute

.. code:: console

    sudo docker-composer -f docker-compose-aaio.yml down

General remark for the Kafka docker image and docker-compose
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Kafka docker container requires you to provide the environment
variable ``KAFKA_ADVERTISED_HOST_NAME``. This has to be changed in the
docker-compose files to match your docker host IP. You can use
``127.0.0.1`` however this will disallow you to run Kafka in a cluster
mode.

For further details please refer to
https://hub.docker.com/r/wurstmeister/kafka

Running docker build outside of Maven
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to have the build of the jars separated from the docker
build you need to provide certain VARS to docker. The following list
shows all the vars and their intended value if you run docker build from
the root dir

-  ``BUILD_DIR_ACS = Core/AtContextServer``

-  ``BUILD_DIR_SCS = SpringCloudModules/config-server``

-  ``BUILD_DIR_SES = SpringCloudModules/eureka``

-  ``BUILD_DIR_SGW = SpringCloudModules/gateway``

-  ``BUILD_DIR_HMG = History/HistoryManager``

-  ``BUILD_DIR_QMG = Core/QueryManager``

-  ``BUILD_DIR_RMG = Registry/RegistryManager``

-  ``BUILD_DIR_EMG = Core/EntityManager``

-  ``BUILD_DIR_STRMG = Storage/StorageManager``

-  ``BUILD_DIR_SUBMG = Core/SubscriptionManager``

-  ``JAR_FILE_BUILD_ACS = AtContextServer-${project.version}.jar``

-  ``JAR_FILE_BUILD_SCS = config-server-${project.version}.jar``

-  ``JAR_FILE_BUILD_SES = eureka-server-${project.version}.jar``

-  ``JAR_FILE_BUILD_SGW = gateway-${project.version}.jar``

-  ``JAR_FILE_BUILD_HMG = HistoryManager-${project.version}.jar``

-  ``JAR_FILE_BUILD_QMG = QueryManager-${project.version}.jar``

-  ``JAR_FILE_BUILD_RMG = RegistryManager-${project.version}.jar``

-  ``JAR_FILE_BUILD_EMG = EntityManager-${project.version}.jar``

-  ``JAR_FILE_BUILD_STRMG = StorageManager-${project.version}.jar``

-  ``JAR_FILE_BUILD_SUBMG = SubscriptionManager-${project.version}.jar``

-  ``JAR_FILE_RUN_ACS = AtContextServer.jar``

-  ``JAR_FILE_RUN_SCS = config-server.jar``

-  ``JAR_FILE_RUN_SES = eureka-server.jar``

-  ``JAR_FILE_RUN_SGW = gateway.jar``

-  ``JAR_FILE_RUN_HMG = HistoryManager.jar``

-  ``JAR_FILE_RUN_QMG = QueryManager.jar``

-  ``JAR_FILE_RUN_RMG = RegistryManager.jar``

-  ``JAR_FILE_RUN_EMG = EntityManager.jar``

-  ``JAR_FILE_RUN_STRMG = StorageManager.jar``

-  ``JAR_FILE_RUN_SUBMG = SubscriptionManager.jar``

Starting of the components
##########################

After the build start the individual components as normal Jar files.

Start the SpringCloud services by running

.. code:: console

    java -jar SpringCloudModules/eureka/target/eureka-server-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar SpringCloudModules/gateway/target/gateway-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar SpringCloudModules/config-server/target/config-server-<VERSIONNUMBER>-SNAPSHOT.jar

Start the broker components

.. code:: console

    java -jar Storage/StorageManager/target/StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar Core/QueryManager/target/QueryManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar Registry/RegistryManager/target/RegistryManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar Core/EntityManager/target/EntityManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar History/HistoryManager/target/HistoryManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar Core/SubscriptionManager/target/SubscriptionManager-<VERSIONNUMBER>-SNAPSHOT.jar
    java -jar Core/AtContextServer/target/AtContextServer-<VERSIONNUMBER>-SNAPSHOT.jar

Changing config
---------------

All configurable options are present in application.properties files. In
order to change those you have two options. Either change the properties
before the build or you can override configs by add
``--<OPTION_NAME>=<OPTION_VALUE)`` e.g.

.. code:: console

    java -jar Storage/StorageManager/target/StorageManager-<VERSIONNUMBER>-SNAPSHOT.jar --reader.datasource.username=funkyusername --reader.datasource.password=funkypassword`

Enable CORS support
-------------------

You can enable cors support in the gateway by providing these
configuration options - gateway.enablecors - default is False. Set to
true for general enabling - gateway.enablecors.allowall - default is
False. Set to true to enable CORS from all origins, allow all headers
and all methods. Not secure but still very often used. -
gateway.enablecors.allowedorigin - A comma separated list of allowed
origins - gateway.enablecors.allowedheader - A comma separated list of
allowed headers - gateway.enablecors.allowedmethods - A comma separated
list of allowed methods - gateway.enablecors.allowallmethods - default
is False. Set to true to allow all methods. If set to true it will
override the allowmethods entry

Troubleshooting
###############

Missing JAXB dependencies
-------------------------

When starting the eureka-server you may facing the

**java.lang.TypeNotPresentException: Type javax.xml.bind.JAXBContext not
present** exception. It's very likely that you are running Java 11 on
your machine then. Starting from Java 9 package ``javax.xml.bind`` has
been marked deprecated and was finally completely removed in Java 11.

In order to fix this issue and get eureka-server running you need to
manually add below JAXB Maven dependencies to
``ScorpioBroker/SpringCloudModules/eureka/pom.xml`` before starting:

.. code:: xml

    ...
    <dependencies>
            ...
            <dependency>
                    <groupId>com.sun.xml.bind</groupId>
                    <artifactId>jaxb-core</artifactId>
                    <version>2.3.0.1</version>
            </dependency>
            <dependency>
                    <groupId>javax.xml.bind</groupId>
                    <artifactId>jaxb-api</artifactId>
                    <version>2.3.1</version>
            </dependency>
            <dependency>
                    <groupId>com.sun.xml.bind</groupId>
                    <artifactId>jaxb-impl</artifactId>
                    <version>2.3.1</version>
            </dependency>
            ...
    </dependencies>
    ...

This should be fixed now using conditional dependencies.
