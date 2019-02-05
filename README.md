# Distributed-File-System

This repository contains all the codes which was used to implement a Distributed File System in Linux using FUSE and XMLRPC. 

A distributed file system along with a client-server based architecture is implemented. It consists of one remote metaserver to
store the metadata of all the files/directories of the file system and N remote data servers(N<=5) to store the data of all the files of the system. To store data equally among all the servers as much as possible, data of all the files needs to be stored on not just one server but on multiple data servers. These multiple data servers are created for implementing the fault tolerance and redundancy principles i.e. if one or more servers fail, the file system should not crash but still be able to still give the data and when the server restarts, it should be able to regain all the data of the files which it was storing prior to its failure. Persistent storage must also be done for all these servers i.e. the data servers need to store their values on the local hard disk from which the server gets its data and updates the data to it as well. Also care must be taken that the data of the files retrieved from the servers by the client is not corrupt for
fault tolerance.
By solving the above problem statement, a distributed client-server based file system is created which takes care two of the major principles of an efficient system design: redundancy and fault tolerance.

Redundancy is taken care by storing multiple copies of the same data of the file on not just one but two other servers as well (i.e. one file has got three copies of it in this file system).
Fault tolerance is taken care by using the checksum of data for its verification and if the data from that server is corrupted, the client will take data from it’s next replicate copy and repeat the procedure. Also if the persistent storage fails, when the server restarts, it needs to restore all the data which it had prior to it’s failure. 
