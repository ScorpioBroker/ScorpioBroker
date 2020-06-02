*******************************
Introduction
*******************************
In a today's era where people can't imagine there lives without internet same is with our devices, nowadays most of our devices are integrated with the IoT, which give us plethora of advantage but left us with few complexities as well.
One of these is making these devices interact with each other, where each device makes use of different schemas, so to mitigate this issue we have a one-stop solution. 

Scorpio broker is a java based pub-sub service designed and developed for the **FIWARE** platform, build on the top of **spring boot architecture** using **NGSI-LD** concepts.
Scorpio broker allows you to collect, process, notify, and store the IoT data with dynamic context with the use of linked data concepts.
It makes use of the **microservice-based architecture** which has its own advantages over the existing IoT brokers such as scalability, cross-technology integration, etc.

Scorpio Broker based on NGSI-LD offers a unique feature of Link data context that provides self-contained (or referenced) **dynamic schema definition** (i.e. the context) for contained data in each message/entity.
Thus allows the Scorpio Broker core processing to still remain unified even it gets dynamic context-driven data as its input from different types of data sources coupled(or designed for) with different schemas. 

Key advantages of Scorpio Broker over other brokers:

- Uses micro-service architecture which enhances the performance drastically.

- The Scorpio Broker architecture is designed & implemented as a scalable, highly available, and load balanced.

- Use of Ld which gives us the leverage of dynamic context.

- Usage of Kafka, allowing us the robust pub-sub service with the facility of scaling with no downtime.

- It provides fail-over resiliency.

- It provides load balancing to distribute the load on distributed infrastructure.

- It is modular enough to offer low coupling and high cohesion by design.

- It offers different storage integration without changing the application logic time and again.
