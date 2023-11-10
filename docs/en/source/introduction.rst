*******************************
Introduction
*******************************
Scorpio Broker is an NGSI-LD compliant context broker developed by NEC Laboratories Europe and NEC India as a part of the FIWARE platform. Basically Scorpio broker is a core component of **FiWARE/IoT** platform. It implements the full NGSI-LD API as specified by the ETSI Industry Specification Group on cross cutting Context Information Management (ETSI ISG CIM). The NGSI-LD API enables the management, access and discovery of context information. Context information consists of entities (e.g., a building) and their properties (e.g., address and geographic location) and relationships (e.g., owner). Thus, Scorpio Broker enables applications and services to request context information – what they need, when they need it and how they need it.

The functionalities of the NGSI-LD API include:
• Create, update, append and delete context information.
• Query context information, including filtering, geographic scoping and paging.
• Subscribe to changes in context information and receive asynchronous notifications.
• Register and discover sources of context information, which allows building distributed and federated deployments.

Scorpio Broker is a FIWARE Generic Enabler. Therefore, it can be integrated as part of any platform “Powered by FIWARE”. FIWARE is a curated framework of open-source platform components which can be assembled with other third-party platform components to accelerate the development of Smart Solutions.

Scorpio Broker makes use of the **microservice-based architecture**. The main version of Scorpio Broker is built with the **Quarkus framework**, which offers distinct advantages over existing IoT brokers, including reduced memory consumption, enhanced scalability, seamless cross-technology integration, etc.

Scorpio Broker based on NGSI-LD offers a unique feature of Link data context that provides self-contained (or referenced) **dynamic schema definition** (i.e. the context) for contained data in each message/entity.
Thus allows the Scorpio Broker core processing to still remain unified even it gets dynamic context-driven data as its input from different types of data sources coupled(or designed for) with different schemas. 

Key advantages of Scorpio Broker over other brokers:

- Uses micro-service architecture which enhances the performance drastically.

- The Scorpio Broker architecture is designed & implemented as a scalable, highly available, and load balanced.

- Use of LD which gives us the leverage of dynamic context.

- Usage of Kafka, allowing us the robust pub-sub service with the facility of scaling with no downtime.

- It provides fail-over resiliency.

- It provides load balancing to distribute the load on distributed infrastructure.

- It is modular enough to offer low coupling and high cohesion by design.

- It offers different storage integration without changing the application logic time and again.

- Support of different deployment architectures, i.e, Centralized, Distributed and Federated

- Integrated support for temporal operation