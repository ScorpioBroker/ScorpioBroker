********************************************************
Deployment Guide for Scorpio Broker on Kubernetes
********************************************************

In order to deploy the Scorpio broker on the Kubernetes, the following dependency needs to be deployed:-

1. Postgres.
2. Kafka and Zookeeper.
3. Scorpio Broker microservices.

For Quick Deployment
#####################

Postgres
************

Please follow the steps to deploy Postgres in your Kubernetes setup:-

1. Firstly the user needs to deploy the Postgres volume files. For this user can clone the Kubernetes files(ScorpioBroker > KubernetesFile > dependencies > volumes > postgres-storage.yaml) or can create a new file with the following code inside it.

 .. code-block:: yaml

  apiVersion: v1
  kind: PersistentVolume
  metadata:
   name: postgres-pv-volume
   labels:
     type: local
     app: postgres
  spec:
   storageClassName: manual
   capacity:
     storage: 5Gi
   accessModes:
     - ReadWriteMany
   hostPath:
     path: "/mnt/db"
  ---
  apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
   name: postgres-pv-claim
   labels:
     app: postgres
  spec:
   storageClassName: manual
   accessModes:
     - ReadWriteMany
   resources:
     requests:
       storage: 5Gi

 once the file is created apply it through the command:

 ::
  
  kubectl create -f <filename> 
 
 This will create a  persistent volume and persistent volume claim for Postgres and the user will get the message.
 ::

  persistentvolume/postgres-pv-volume created
  persistentvolumeclaim/postgres-pv-claim created

 User can check these by running the commands:

 ::
  
  kubectl get pv
  kubectl get pvc 
 
 .. figure:: figures/postPv.png

2. After the persistent volume and persistent volume claim are successfully created user needs to deploy the Postgres deployment file.

 .. code-block:: yaml

  apiVersion: apps/v1
  kind: Deployment
  metadata:
   labels:
      component: postgres
   name: postgres
  spec:
   replicas: 1
   selector:
    matchLabels:
      component: postgres
   strategy: {}
   template:
    metadata:
      labels:
       component: postgres
    spec:
      containers:
      - env:
        - name: POSTGRES_DB
          value: ngb
        - name: POSTGRES_PASSWORD
          value: ngb
        - name: POSTGRES_USER
          value: ngb
        image: mdillon/postgis
        imagePullPolicy: ""
        name: postgres
        ports:
        - containerPort: 5432
        resources: {}
        volumeMounts:
        - mountPath: /var/lib/postgresql/data
          name: postgredb
      restartPolicy: Always
      serviceAccountName: ""
      volumes:
        - name: postgredb
          persistentVolumeClaim:
            claimName: postgres-pv-claim
  status: {}

 once the file is created apply it through the command:

 ::
  
  kubectl create -f <filename> 

 This will create an instance of Postgres and the user will get the message.
 ::

  deployment.apps/postgres created

 User can check this by running the commands:

 ::
  
  kubectl get deployments
  
 .. figure:: figures/postDeploy.png

3. Lastly user needs to deploy the service file.

 .. code-block:: yaml

  apiVersion: v1
  kind: Service
  metadata:
    labels:
       component: postgres
    name: postgres
  spec:
    ports:
    - name: "5432"
      port: 5432
      targetPort: 5432
    selector:
        component: postgres
  status:
    loadBalancer: {}

 once the file is created apply it through the command:

 ::
  
  kubectl create -f <filename> 

 This will create a clusterIp service of Postgres and the user will get the message.
 ::

  service/postgres created


 User can check this by running the commands:

 ::
  
  kubectl get svc
  
 .. figure:: figures/postService.png

Kafka and zookeeper
************************

To quickly deploy the Kafka and zookeeper, the user can use the deployment files present in the dependencies folder of the Kubernetes files. To deploy these files please follow the following steps:

1. Deploy the zookeeper deployment file.

 .. code-block:: yaml

  apiVersion: apps/v1
  kind: Deployment
  metadata:
    labels:
      component: zookeeper
    name: zookeeper
  spec:
    progressDeadlineSeconds: 600
    replicas: 1
    revisionHistoryLimit: 10
    selector:
      matchLabels:
        component: zookeeper
    strategy:
      rollingUpdate:
        maxSurge: 25%
        maxUnavailable: 25%
      type: RollingUpdate
    template:
      metadata:
        creationTimestamp: null
        labels:
          component: zookeeper
      spec:
        containers:
        - image: zookeeper
          imagePullPolicy: Always
          name: zookeeper
          ports:
          - containerPort: 2181
            protocol: TCP
          resources:
            limits:
              cpu: 500m
              memory: 128Mi
            requests:
              cpu: 250m
              memory: 64Mi
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
        dnsPolicy: ClusterFirst
        restartPolicy: Always
        schedulerName: default-scheduler
        securityContext: {}
        terminationGracePeriodSeconds: 30
  status: {}

 once the file is created apply it through the command:

 ::
  
  kubectl create -f <filename> 

 This will create an instance of Zookeeper and the user will get the message.
 ::

  deployment.apps/zookeeper created

 User can check this by running the commands:

 ::
  
  kubectl get deployments
  
 .. figure:: figures/zookeeperDep.png

2. Once the deployment is up and running, deploy the service using the service file.

 .. code-block:: yaml

  apiVersion: v1
  kind: Service
  metadata:
    labels:
        component: zookeeper
    name: zookeeper
  spec:
    ports:
    - name: "2181"
      port: 2181
      targetPort: 2181
    selector:
        component: zookeeper
  status:
    loadBalancer: {}

 once the file is created apply it through the command:

 ::
  
  kubectl create -f <filename> 

 This will create an instance of Zookeeper and the user will get the message.
 ::

  service/zookeeper created

 User can check this by running the commands:

 ::
  
  kubectl get svc
  
 .. figure:: figures/zookSer.png

3. After the zookeeper service file is successfully deployed, create the PV and PVC for the Kafka using the Kafka storage file present in the dependencies folder.

 .. code-block:: yaml

  apiVersion: v1
  kind: PersistentVolume
  metadata:
    name: kafka-pv-volume
    labels:
     type: local
  spec:
    storageClassName: manual
    capacity:
      storage: 1Gi
    accessModes:
      - ReadWriteOnce
    hostPath:
      path: "/mnt/data"
  ---
  apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    labels:
        component: kafka-claim0
    name: kafka-claim0
  spec:
    storageClassName: manual
    accessModes:
    - ReadWriteOnce
    resources:
      requests:
        storage: 1Gi
  status: {}

 once the file is created apply it through the command:

 ::
  
  kubectl create -f <filename> 
 
 This will create a  persistent volume and persistent volume claim for Postgres and the user will get the message.
 ::


  persistentvolume/kafka-pv-volume created
  persistentvolumeclaim/kafka-claim0 created

 User can check these by running the commands:

 ::
  
  kubectl get pv
  kubectl get pvc 
 
 .. figure:: figures/kafkaPv.png

4. Now deploy the Kafka using the Kafka deployment files.

 .. code-block:: yaml

  apiVersion: apps/v1
  kind: Deployment
  metadata:
    name: kafka
  spec:
    replicas: 1
    selector:
      matchLabels:
        component: kafka
    strategy:
      type: Recreate
    template:
      metadata:
        labels:
          component: kafka
      spec:
        containers:
        - name: kafka 
          image: wurstmeister/kafka
          ports:
            - containerPort: 9092
          resources: {}
          volumeMounts:
          - mountPath: /var/run/docker.sock
            name: kafka-claim0
          env:
            - name: MY_POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: KAFKA_ADVERTISED_PORT
              value: "9092"
            - name: KAFKA_ZOOKEEPER_CONNECT
              value: zookeeper:2181
            - name: KAFKA_ADVERTISED_PORT
              value: "9092"
            - name: KAFKA_ADVERTISED_HOST_NAME
              value: $(MY_POD_IP)
        hostname: kafka
        restartPolicy: Always
        serviceAccountName: ""
        volumes:
        - name: kafka-claim0
          persistentVolumeClaim:
            claimName: kafka-claim0
  status: {}

 once the file is created apply it through the command:

 ::
  
  kubectl create -f <filename> 

 This will create an instance of Postgres and the user will get the message.
 ::

  deployment.apps/kafka created

 User can check this by running the commands:

 ::
  
  kubectl get deployments
  
 .. figure:: figures/kafkaDep.png
 
5. Finally deploy the Kafka service file. (Only once Kafka deployment moved to running state else sometimes is throes error).

 .. code-block:: yaml

  apiVersion: v1
  kind: Service
  metadata:
    labels:
      component: kafka
    name: kafka
  spec:
    ports:
    - name: "9092"
      port: 9092
      targetPort: 9092
    selector:
      component: kafka
  status:
    loadBalancer: {}

 once the file is created apply it through the command:

 ::
  
  kubectl create -f <filename> 

 This will create a clusterIp service of Postgres and the user will get the message.
 ::

  service/kafka created

 User can check this by running the commands:

 ::
  
  kubectl get svc
  
 .. figure:: figures/kafkaSer.png


Scorpio Broker
****************

For testing and other lite usage, users can use the All-in-one-deployment(aaio) files(in this all the micro-services are deployed in the single docker container). For this user have two options:

1. **Deployment through helm**: The first step is to get the helm chart of aaio deployment of the Scorpio broker, please download the helm package from GitHub(ScorpioBroker > KubernetesFile > aaio-deployment-files > helm ).

  Now run the command 

  **helm install {release_name} <helm folder name>**

2. **Deployment through YAML files**: user can use the YAML files present in the aaio deployment section and follow the steps:

 a. Make sure Kafka and Postgres are running, after that deploy the deployment file or the gien configuaration  using the command 

   .. code-block:: yaml

    apiVersion: apps/v1
    kind: Deployment
    metadata:
      labels:
          component: scorpio
      name: scorpio
    spec:
      replicas: 2
      selector:
        matchLabels:
          component: scorpio
      strategy: {}
      template:
        metadata:
          labels:
          component: scorpio
        spec:
          containers:
          - image: scorpiobroker/scorpio:scorpio-aaio_1.0.0
            imagePullPolicy: ""
            name: scorpio
            ports:
            - containerPort: 9090
            resources: {}
          restartPolicy: Always
          serviceAccountName: ""
          volumes: null
    status: {}

   once the file is created apply it through the command:

   ::
  
    kubectl create -f <filename> 

   This will create an instance of Scorpio Broker and the user will get the message.
   ::

    deployment.apps/scorpio created


    User can check this by running the commands:

   ::
  
    kubectl get deployments
  
   .. figure:: figures/scorpioAaioDeploy.png

   
 b. Once the deployment is up and running create the clusterIP or node port service as per the need.

   .. code-block:: yaml

    apiVersion: v1
    kind: Service
    metadata:
      labels:
          component: scorpio
      name: scorpio
    spec:
      ports:
      - name: "9090"
        port: 9090
        targetPort: 9090
      selector:
          component: scorpio
    status:
      loadBalancer: {}
    ----
    apiVersion: v1
    kind: Service
    metadata:
      labels:
          component: scorpio
      name: scorpio-node-port
    spec:
      type: NodePort
      ports:
      - port: 9090
        targetPort: 9090
        nodePort : 30000
      selector:
          component: scorpio

   once the file is created apply it through the command:

   ::
  
    kubectl create -f <filename> 

   This will create an instance of Postgres and the user will get the message.
   ::

    service/scorpio created
    service/scorpio-node-port created

    User can check this by running the commands:

   ::
  
    kubectl get deployments
  
   .. figure:: figures/scorpioSvc.png

   Now, if user have deployed the node post service user can access Scorpio Broker at 

   ::

   <ip address of master>:30000

For Production Deployment
##########################

Postgres
************

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
*********************

To deploy a Kafka on production, we prefer to use helm since helm provides a hassle-free deployment experience for Kubernetes

To install helm in your Kubernetes cluster follow the link (https://helm.sh/docs/intro/install/). The preferred version is helm version 3+.

Once helm is installed use the following command to get the running Kafka cluster:

::

 helm repo add bitnami https://charts.bitnami.com/bitnami
 helm install kafka bitnami/kafka

For more information follow the link (https://artifacthub.io/packages/helm/bitnami/kafka)

Scorpio Broker
****************

Once we get the running instance of PostgreSQL as well as Kafka, we are ready to deploy the Scorpio broker.

The first step is to get the helm chart of Scorpio broker, for this download the helm package from GitHub.(user can also use the YAML files if needed)
Now run the command 

**helm install {release_name} <helm folder name>**

Now run the **kubectl get pods --all-namespace** to verify that all the microservice of the Scorpio broker are in the running state.

**Note:** Please use only the latest docker images for the deployment since some older docker images might not work properly with Kubernetes.
 
Now you are ready to use the Scorpio broker.