*******************************
Introduction
*******************************
In a today's era where people can't imagine there lies without internet same is with our devices also,nowadays most of the devices are integrated with the Iot,which give us plethora of advantage but left us with few complexity also.
One of this is making these devices interact with each other,where each device make use of different schemas,so to mitigate this issue we have one stop solution. 

Scorpio broker is a java based pub-sub service designed and developed for the **FIWARE** platform, build on the top of **spring boot architecture** using **NGSI-LD** concepts.
scorpio broker allow you to collect,process,notify and store the Iot data with dynamic context with the use of linked data concepts .
It make use of the **microservice based architecture** which has its own advantages over the existing Iot brokers such as scalability , cross-technology integration etc.

NGB based on NGSI-LD offers unique feature of Link data context that provides self-contained (or referenced) **dynamic schema definition** (i.e. the context) for contained data in each message/entity.
Thus allows the NGB core processing to be still remain unified even it gets dynamic context driven data as its input from different types of data sources coupled(or designed for) with different schemas. 

Key advantages of NGB over other brokers:

- Uses micro-service architecture which enhance the performance drastically.

- The NGB architecture is designed & implemented as a scalable, highly available and load balanced.

- Use of Ld which give us the leverage of dynamic context.

- Usage of kafka,allowing us the robust pub-sub service with the facility of scaling with no down time.

- It provides fail-over resiliency.

- It provides load balancing to distribute load on distributed infrastructure.

- It is modular enough to offer low coupling and high cohesion by design.

- It offers different storage integration without changing the application logic time and again.

