Integrate Scorpio Broker with LoRaWAN IoT agent 
****************************************************

This tutorial introduces the concept of integration of Scorpio Broker with LoRaWAN IoT Agent. The IoT Agent for LoRaWAN protocol enables data and commands to be exchanged between IoT devices and the NGSI interface of a context broker using the LoRaWAN protocol.

This users manual provides guidance on deploy a dummy device and testing it without having a real account on TTN.

As a matter of fact, the LoRaWAN IoT Agent leverages the MQTT services provided by TTN or ChirpStack to retrieve data from the sensors and pushing them to the Context Broker. Essentially this means, that using a simple MQTT broker and a MQTT client you can actually simulate the connection of the LoRaWAN IoT Agent to TTN or ChirpStack.

In this tutorial we will proceed as follow:

1.  We will deploy a stack including the relevant services to test the end-to-end functionality of the LoRaWAN IoT Agent
2.  We will register 1 device group simulating a TTN device.
3.  We will send messages to the MQTT broker simulating the device sending data.
3.  We will verify that the data have been correctly decoded and forwarded to the Context Broker.

Deploy the testing IoT stack and registering a device group
=============================================================

To run this walkthrough you need to deploy:

-   LoRaWAN IoT Agent
-   Mosquitto MQTT
-   Scorpio Context Broker

Please clone the repository and create the necessary images like Scorpio Broker, IoT Agent and MQTT by running the commands as shown:

.. code-block:: console
    
     git clone https://github.com/ScorpioBroker/ScorpioBroker.git
	 git checkout iotagent-LoRaWAN
     cd ScorpioBroker/iotagent-lorawan

    docker-compose -f docker-compose.yml up -d

**Note:** version of docker-compose should be greater than 1.21.*.

After that you can run:

.. code-block:: console

	docker ps

to check if all the required components are running

Before you start the following steps, please check if your Scorpio Broker and IoT Agent is running properly.

check if the Scorpio Broker is running

.. code-block:: console

	curl <scorpio-brokerIP>:9090/actuator/health

check if the IoT Agent is running

.. code-block:: console

	curl <IoT-AgentIP>:4041/iot/about

The response for IoT Agent will look similar to the following:

.. code-block:: console

	{
		"libVersion": "2.21.0",
		"port": 4041,
		"baseRoot": "/",
		"version": "1.2.5"
	}

Testing the LoRaWAN IoT Agent with dummy devices
==================================================

**Step 1:** Register a dummy service representing Weather observation device
-----------------------------------------------------------------------------

Invoking group provision is always the first step in connecting devices, since this example provisions one device group simulating a TTN device. It tells the IoT Agent that a Weather observation device will be sending messages to the IOTA_HTTP_PORT (where the IoT Agent is listening for Northbound communications data coming from the IoT device)

For provisioning the Service in IoT Agent, use the following REST call:

.. code-block:: console 

	curl --location --request POST 'http://<IoT-AgentIP>:4041/iot/services' \
	--header 'fiware-service: smartgondor' \
	--header 'fiware-servicePath: /environment' \
	--header 'Content-Type: application/json' \
	--data-raw '{
		"services": [
			{
				"entity_type": "WeatherObserved",
				"apikey": "",
				"resource": "70B3D57ED00006B2",
				"attributes": [
					{
						"object_id": "temperature_1",
						"name": "temperature",
						"type": "Number"
					},
					{
						"object_id": "barometric_pressure_0",
						"name": "pressure",
						"type": "Number"
					},
					{
						"object_id": "relative_humidity_2",
						"name": "relative_humidity",
						"type": "Number"
					}
				],
				"internal_attributes": {
					"lorawan": {
						"application_server": {
							"host": "mqtt",
							"username": "admin",
							"password": "password",
							"provider": "TTN"
						},
						"app_eui": "70B3D57ED00006B2",
						"application_id": "demoTTN",
						"application_key": "BE6996EEE2B2D6AFFD951383C1F3C3BD",
						"data_model": "cayennelpp"
					}
				}
			}
		]
	}'

**Step 2:** Publish a message to the MQTT and verify that the value is passed to Scorpio Broker
-------------------------------------------------------------------------------------------------

This script will publish a JSON message to the MQTT broker as defined by TTN api, e.g.:

.. code-block:: console

	mosquitto_pub -h mqtt -u admin -P password -t v3/demoTTN/devices/myDevice/up -m '{
	  "app_id": "demoTTN",
	  "dev_id": "myDevice",
	  "hardware_serial": "0102030405060708",
	  "port": 1,
	  "counter": 2,
	  "is_retry": false,
	  "confirmed": false,
	  "payload_raw": "AHMnSwFnARYCaFADAGQEAQAFAdc="
	}'

The `payload_raw` field contains a base64 encoded version of the binary encoding of the CayenneLPP payload

**CayenneLPP -** The Cayenne Low Power Payload (LPP) is a format designed to integrate LoRaWAN nodes into IoT Platform. It is used to send sensor data in a packed way to The Things Network (TTN) platform.

The topic used by TTN v3 API has the following format: `v3/{application_id}/devices/{device_id}/up`

**Note:** Device data (alerts) are sent via an MQTT broker

**Step 3:** Retrieve the recorded measurement of Weather observation device
------------------------------------------------------------------------------

Execute the following command to retrieve the recorded measurement of Weather observation device from Scorpio Broker

.. code-block:: console 

	curl --location --request GET 'http://<scorpio-brokerIP>:9090/ngsi-ld/v1/entities/urn:ngsi-ld:WeatherObserved:urn:WeatherObserved:myDevice' \
	--header 'NGSILD-Tenant: smartgondor' \
	--header 'NGSILD-Path: /environment' \
	--header 'Content-Type: application/json'

Using the service group defined above, the CayenneLPP payload will be decoded and mapped to the NGSI format and forwarded to Scorpio Broker, the resulting NGSI entity should be something like:

.. code-block:: console 

	{
		"id": "urn:ngsi-ld:WeatherObserved:urn:WeatherObserved:myDevice",
		"type": "WeatherObserved",
		"pressure": {
			"type": "Property",
			"value": 1005.9,
			"observedAt": "2023-04-20T10:14:19.144Z"
		},
		"relative_humidity": {
			"type": "Property",
			"value": 40,
			"observedAt": "2023-04-20T10:14:19.144Z"
		},
		"temperature": {
			"type": "Property",
			"value": 27.8,
			"observedAt": "2023-04-20T10:14:19.144Z"
		}
	}

**Step 4:** Removing your stacks
----------------------------------

If you want to clean up you can do this with the following command:

.. code-block:: console 
 
	docker-compose -f docker-compose.yml down -v
