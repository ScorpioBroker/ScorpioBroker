****************************
Kubernetes Deployment Guide
****************************

In order to deploy the Scorpio broker on the Kubernetes, the following dependency needs to be deployed:-

1. Postgres.
2. Kafka and Zookeeper.
3. Scorpio Broker microservices.

Postgres
##########

For Testing
************
For the quick set-up, the user can use the yaml files present in the dependencies folder of Kubernetes files. For this user need to follow the following steps:-
 1. Firstly the user needs to deploy the Postgres volume files.
 
 2. After the persistent volume and persistent volume claim are successfully created user needs to deploy the Postgres deployment file.

 3. Lastly user needs to deploy the service file.

For Production
***************
In order to achieve high availability and to reduce the effort of managing the multiple instances, we opted for crunchy data (https://www.crunchydata.com/) to fulfill all our needs for PostgreSQL.

To deploy the crunchy data Postgres follow the link https://www.crunchydata.com/ 

Once we get the running instance of Postgres, logged into the Postgres using the superuser and run the following commands to create a database and the required role: 

1. create database ngb;
2. create user ngb with password 'ngb';
3. alter database ngb owner to ngb;
4. grant all privileges on database ngb to ngb;
5. alter role ngb superuser;

After this, your PostgreSQL is ready to use for Scorpio Boker.

Note: Create a cluster using the command: 

**pgo create cluster postgres \--ccp-image=crunchy-postgres-gis-ha \--ccp-image-tag=centos7-12.5-3.0-4.5.1**

Kafka and zookeeper
######################

For Testing
************
To quickly deploy the Kafka and zookeeper, the user can use the deployment files present in the dependencies folder of the Kubernetes files. To deploy these files please follow the following steps:
 1. Deploy the zookeeper deployment file.

 2. Once the deployment is up and running, deploy the service using the service file.

 3. After the zookeeper service file is successfully deployed, create the PV and PVC for the Kafka using the Kafka storage file present in the dependencies folder.
 
 4. Now deploy the Kafka using the Kafka deployment files.
 
 5. Finally deploy the Kafka service file. (Only once Kafka deployment moved to running state else sometimes is throes error).

For Production
***************
To deploy a Kafka on production, we prefer to use helm since helm provides a hassle-free deployment experience for Kubernetes

To install helm in your Kubernetes cluster follow the link (https://helm.sh/docs/intro/install/). The preferred version is helm version 3+.

Once helm is installed use the following command to get the running Kafka cluster:

::

 helm repo add bitnami https://charts.bitnami.com/bitnami
 helm install kafka bitnami/kafka

For more information follow the link (https://artifacthub.io/packages/helm/bitnami/kafka)

Scorpio Broker
#################

For Testing
************
For testing and other lite usage, users can use the All-in-one-deployment(aaio) files(in this all the micro-services are deployed in the single docker container). For this user have two options:
 1. **Deployment through helm**: The first step is to get the helm chart of aaio deployment of the Scorpio broker, please download the helm package from GitHub.

  Now run the command 

  **helm install {release_name} <helm folder name>**

 2. **Deployment through YAML files**: user can use the YAML files present in the aaio deployment section and follow the steps:

   a. Make sure Kafka and Postgres are running, after that deploy the deployment file using the command **kubectl create -f <file name>**.
   
   b. Once the deployment is up and running create the clusterIP or node port service as per the need.

For Production
***************
Once we get the running instance of PostgreSQL as well as Kafka, we are ready to deploy the Scorpio broker.

The first step is to get the helm chart of Scorpio broker, for this download the helm package from GitHub.(user can also use the YAML files if needed)
Now run the command 

**helm install {release_name} <helm folder name>**

Now run the ** kubectl get pods --all-namespace ** to verify that all the microservice of the Scorpio broker are in the running state.

**Note:** Please use only the latest docker images for the deployment since some older docker images might not work properly with kubernetes.
 
Now you are ready to use the Scorpio broker.