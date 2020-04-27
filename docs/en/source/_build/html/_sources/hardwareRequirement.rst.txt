*****************************************
Java 8 System Requirements
*****************************************

**Windows**

- Windows 10 (8u51 and above)
- Windows 8.x (Desktop)
- Windows 7 SP1
- Windows Vista SP2
- Windows Server 2008 R2 SP1 (64-bit)
- Windows Server 2012 and 2012 R2 (64-bit)
- RAM: 128 MB
- Disk space: 124 MB for JRE; 2 MB for Java Update
- Processor: Minimum Pentium 2 266 MHz processor
- Browsers: Internet Explorer 9 and above, Firefox

**Mac OS X**

- Intel-based Mac running Mac OS X 10.8.3+, 10.9+
- Administrator privileges for installation
- 64-bit browser
- A 64-bit browser (Safari, for example) is required to run Oracle Java on Mac.

**Linux**

- Oracle Linux 5.5+1
- Oracle Linux 6.x (32-bit), 6.x (64-bit)2
- Oracle Linux 7.x (64-bit)2 (8u20 and above)
- Red Hat Enterprise Linux 5.5+1, 6.x (32-bit), 6.x (64-bit)2
- Red Hat Enterprise Linux 7.x (64-bit)2 (8u20 and above)
- Suse Linux Enterprise Server 10 SP2+, 11.x
- Suse Linux Enterprise Server 12.x (64-bit)2 (8u31 and above)
- Ubuntu Linux 12.04 LTS, 13.x
- Ubuntu Linux 14.x (8u25 and above)
- Ubuntu Linux 15.04 (8u45 and above)
- Ubuntu Linux 15.10 (8u65 and above)
- Browsers: Firefox

*****************************************
ZooKeeper Requirements
*****************************************

ZooKeeper runs in Java, release 1.6 or greater (JDK 6 or greater). 
It runs as an ensemble of ZooKeeper servers. 
Three ZooKeeper servers is the minimum recommended size for an ensemble, and we also recommend that they run on separate machines. 
At Yahoo!, ZooKeeper is usually deployed on dedicated RHEL boxes, with dual-core processors, 2GB of RAM, and 80GB IDE hard drives.

*****************************************
Recommendations for Kafka
*****************************************

- Kafka Broker Node: eight cores, 64 GB to128 GB of RAM, two or more 8-TB SAS/SSD disks, and a 10- Gige Nic .

- Minimum of three Kafka broker nodes

- Hardware Profile: More RAM and faster speed disks are better; 10 Gige Nic is ideal.

- 75 MB/sec per node is a conservative estimate ( can go much higher if more RAM and reduced lag between writing/reading and therefore 10GB Nic is required ).

- With a minimum of three nodes in your cluster, you can expect 225 MB/sec data transfer.

- You can perform additional further sizing by using the following formula:

num_brokers = desired_throughput (MB/sec) / 75