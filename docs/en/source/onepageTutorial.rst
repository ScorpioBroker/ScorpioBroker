*****************************************
Basic Guide
*****************************************

Architectural Overview
############################
Scorpio Broker is a reference implementation of NGSI-LD APIs. Scorpio Broker provides an implementation of REST API endpoints for various data context operations that conform to NGSI-LD API specification. Scorpio Broker component has been implemented based on modular, Microservices oriented, scalable, secure by design, easy to monitor/debug, fault-tolerant, and highly available architecture. Scorpio Broker based on NGSI-LD offers a unique feature of Link data context that provides self-contained (or referenced) dynamic schema definition (i.e. the context) for contained data in each message/entity. Thus allows the Scorpio Broker core processing to still remain unified even it gets dynamic context-driven data as its input from different types of data sources coupled(or designed for) with different schemas.

.. figure:: figures/architecture.png

The basic architecture of the Scorpio Broker consists of five layers, the first layer consists of the Scorpio Broker clients which act as the producers and consumers. The second layer act as an interface between the Scorpio Broker and the external world this layer comprises the NGSI-LD Compliant Interfaces, Service API Gateway, and Service Discovery & Registration. The third layer contains all the micro-services and is responsible for the majority of tasks like entity CRUD operations etc. The fourth layer acts as the interface which connects different micro-services from the storage. The fifth layer is a Resources layer which acts as the storage for Scorpio Broker.

*****************************************
Installation Guide
*****************************************

In order to set-up the environment of Scorpio broker, the following dependency needs to be configured:-

1. Server JDK.
2. Apache Kafka.
3. PostgreSQL


Windows
*****************************************

JDK Setup
##############

- Start the JDK installation and hit the “Change destination folder” checkbox, then click 'Install.'

**Note:-** Recommended version is JDK-11. Scorpio Broker is developed and tested with this version only.

.. figure:: figures/jdk-1.png

- Change the installation directory to any path without spaces in the folder name.


After you've installed Java in Windows, you must set the  JAVA_HOME  environment variable to point to the Java installation directory.

**Set the JAVA_HOME Variable**

To set the JAVA_HOME variable:

1. Find out where Java is installed. If you didn't change the path during installation, it will be something like this:

 *C:\Program Files\Java\jdk1.version*

2. - In Windows 7 right-click **My Computer** and select **Properties** > **Advanced**.

  OR

 - In Windows 8 go to **Control Panel** > **System** > **Advanced System Settings**.

3. Click the Environment Variables button.

4. Under System Variables, click New.

5. In the User Variable Name field, enter: **JAVA_HOME**

6. In the User Variable Value field, enter your JDK  path.

 (Java path and version may change according to the version of Kafka you are using)

7. Now click OK.

8. Search for a Path variable in the “System Variable” section in the “Environment Variables” dialogue box you just opened.

9. Edit the path and type *;%JAVA_HOME%\bin* at the end of the text already written there, just like the image below:

.. figure:: figures/jdk-3.png


- To confirm the Java installation, just open cmd and type “java –version.” You should be able to see the version of Java you just installed.

.. figure:: figures/jdk-4.png

If your command prompt somewhat looks like the image above, you are good to go. Otherwise, you need to recheck whether your setup version matches the correct OS architecture (x86, x64), or if the environment variables path is correct.


Setting Up Kafka
############################

1. Go to your Kafka config directory. For example:- **C:\kafka_2.12-2.1.0\config**
2. Edit the file “server.properties.”
3. Find and edit the line log.dirs=/tmp/kafka-logs” to “log.dir= C:\kafka_2.11-0.9.0.0\kafka-logs.
4. If your ZooKeeper is running on some other machine or cluster you can edit “zookeeper.connect:2181” to your custom IP and port. For this demo, we are using the same machine so there's no need to change. Also the Kafka port and broker.id are configurable in this file. Leave other settings as is.
5. Your Kafka will run on default port 9092 and connect to ZooKeeper’s default port, 2181.

**Note**: For running Kafka, zookeepers should run first. At the time of closing Kafka, zookeeper should be closed first than Kafka. Recommended version of kafka is kafka_2.12-2.1.0.


Running a Kafka Server
############################

Important: Please ensure that your ZooKeeper instance is up and running before starting a Kafka server.

1. Go to your Kafka installation directory:** C:\kafka_2.11-0.9.0.0\**
2. Open a command prompt here by pressing Shift + right-click and choose the “Open command window here” option).
3. Now type **.\bin\windows\kafka-server-start.bat .\config\server.properties** and press Enter,then
4. Type **.\bin\windows\kafka-server-start.bat .\config\server.properties** in new command window and hit enter.

Setting up PostgreSQL
############################

Step 1) Go to https://www.postgresql.org/download.

**Note**: Recommended version is postgres 10. 

Step 2) You are given two options:-

 1. Interactive Installer by EnterpriseDB
 2. Graphical Installer by BigSQL

BigSQL currently installs pgAdmin version 3 which is deprecated. It's best to choose EnterpriseDB which installs the latest version 4


Step 3)

 1. You will be prompted to the desired Postgre version and operating system. Select the Postgres 10, as Scorpio has been tested and developed with this version.

 2. Click the Download Button, Download will begin

Step 4) Open the downloaded .exe and Click next on the install welcome screen.


Step 5) 

 1. Change the Installation directory if required, else leave it to default

 2.Click Next


Step 6)

 1. You can choose the components you want to install in your system. You may uncheck Stack Builder

 2. Click on Next


Step 7)

 1. You can change the data location

 2.Click Next


Step 8)

 1. Enter the superuser password. Make a note of it

 2.Click Next


Step 9)

 1. Leave the port number as the default

 2.Click Next


Step 10)

 1. Check the pre-installation summary.

 2.Click Next

Step 11) Click the next button

Step 12) Once install is complete you will see the Stack Builder prompt

 1. Uncheck that option. We will use Stack Builder in more advance tutorials

 2.Click Finish

Step 13) To launch Postgre go to Start Menu and search pgAdmin 4

Step 14) You will see pgAdmin homepage

Step 15) Click on Servers > Postgre SQL 10 in the left tree

.. figure:: figures/dbconfig-1.png

Step 16)

 1.Enter superuser password set during installation

 2. Click OK

Step 17) You will see the Dashboard

.. figure:: figures/dbconfig-2.png

That's it to Postgre SQL installation.

Linux
*****************************************