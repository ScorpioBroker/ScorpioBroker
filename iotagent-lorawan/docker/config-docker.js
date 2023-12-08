/*
 * Copyright 2019 Atos Spain S.A
 *
 * This file is part of iotagent-lora
 *
 * iotagent-lora is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * iotagent-lora is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with iotagent-lora.
 * If not, seehttp://www.gnu.org/licenses/.
 *
 */

const config = {};

config.iota = {
	/**
	 * Configures the log level. Appropriate values are: FATAL, ERROR, INFO, WARN and DEBUG.
	 */
	logLevel: 'DEBUG',

	/**
	 * When this flag is active, the IoTAgent will add the TimeInstant attribute to every entity created, as well
	 * as a TimeInstant metadata to each attribute, with the current timestamp.
	 */
	timestamp: true,

	/**
	 * Context Broker configuration. Defines the connection information to the instance of the Context Broker where
	 * the IoT Agent will send the device data.
	 */
	contextBroker: {
		/**
		 * Host where the Context Broker is located.
		 */
		host: 'scorpio',

		/**
		 * Port where the Context Broker is listening.
		 */
		port: '9090',

		/**
		 * Version of NGSI
		 */
		ngsiVersion: 'ld',

        /**
         * JSON LD Context
         */
        jsonLdContext: 'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld'
	},

	/**
	 * Configuration of the Northbound server of the IoT Agent.
	 */
	server: {
		/**
		 * Port where the IoT Agent will be listening for requests.
		 */
		port: 4041
	},

	/**
	 * Default resource of the IoT Agent. This value must be different for every IoT Agent connecting to the IoT
	 * Manager.
	 */
	defaultResource: '/iot/d',

	/**
	 * Defines the configuration for the Device Registry, where all the information about devices and configuration
	 * groups will be stored. There are currently just two types of registries allowed:
	 *
	 * - 'memory': transient memory-based repository for testing purposes. All the information in the repository is
	 *             wiped out when the process is restarted.
	 *
	 * - 'mongodb': persistent MongoDB storage repository. All the details for the MongoDB configuration will be read
	 *             from the 'mongoDb' configuration property.
	 */
	deviceRegistry: {
		type: 'mongodb'
	},

	/**
	 * Mongo DB configuration section. This section will only be used if the deviceRegistry property has the type
	 * 'mongodb'.
	 */
	mongodb: {
		/**
		 * Host where MongoDB is located. If the MongoDB used is a replicaSet, this property will contain a
		 * comma-separated list of the instance names or IPs.
		 */
		host: 'mongodb',

		/**
		 * Port where MongoDB is listening. In the case of a replicaSet, all the instances are supposed to be listening
		 * in the same port.
		 */
		port: '27017',

		/**
		 * Name of the Mongo database that will be created to store IOTAgent data.
		 */
		db: 'iotagentlora'

		/**
		 * Name of the set in case the Mongo database is configured as a Replica Set. Optional otherwise.
		 */
		// replicaSet: ''
	},

	/**
	 *  Types array for static configuration of services. Check documentation in the IoTAgent Library for Node.js for
	 *  further details:
	 *
	 *      https://github.com/telefonicaid/iotagent-node-lib#type-configuration
	 */
	types: {},

	/**
	 * Default service, for IOTA installations that won't require preregistration.
	 */
	service: 'howtoService',

	/**
	 * Default subservice, for IOTA installations that won't require preregistration.
	 */
	subservice: '/howto',

	/**
	 * URL Where the IOTA Will listen for incoming updateContext and queryContext requests (for commands and passive
	 * attributes). This URL will be sent in the Context Registration requests.
	 */
	providerUrl: 'http://iotagent-lora:4061',

	/**
	 * Default maximum expire date for device registrations.
	 */
	deviceRegistrationDuration: 'P1Y',

	/**
	 * Default type, for IOTA installations that won't require preregistration.
	 */
	defaultType: 'Thing'
};

module.exports = config;
