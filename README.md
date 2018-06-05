# Benmo

A python implementation of Paxos for consensus on a public transaction ledger.

# What it does

Paxos is used for fault tolerant replication. As long as a majority of the network (a quorum) has not crashed, the nodes can elect a leader and achieve conensus on some proposed value.

In Benmo, each node has a balance which can be transferred to other nodes. The transactions are stored in a log, which is in turn stored in a "blockchain". A log is only added to the blockchain when an instance of Paxos reaches consensus on that log. Thus, every node has a consistent copy of the blockchain once an instance of Paxos finishes executing. 
Our program sets up a network between five nodes, all of whom idle in the "acceptor" state waiting for a node to become the leader. Once a user inputs a transaction at any of the nodes, it attempts to become the leader after a random amount of time has passed (20 - 60 seconds). The leader then communicates with all the other nodes and attempts to reach consensus per the standard Paxos protocol. 

Benmo supports simulated crash and partitioning failures. Crash failures are implemented by simply killing the process, and partitioning failures are simulated by having a node cease transmission on a particular link. If an acceptor node becomes unreachable in the middle of Paxos, the leader will cease communication with that node until Paxos finishes executing. If a leader node becomes unreachable in the middle of Paxos, the acceptor will terminate that instance of Paxos.

# How to use it

Before running Benmo, the user must execute initialize.py in the same directory -- this is because, on startup, nodes read their state from disk. All nodes are initiliazed with a balance of $100 and empty transaction logs and blockchains.

One can use "moneyTransfer" to send money from one node (debit_node) to another (credit_node). Additionally, one can simulate a crash failure at the current node by inputting "crashNode" -- this will terminate the process. A partitioning failure between two nodes can be simulated using "severLink" and inputting the node with which to cease communcication. Finally, the user can view the state of the current node by inputting one of the "print" commands listed. 

# How it could be improved

- Split the file up into several modules
   - One solution is to simply move all the leader and acceptor utility functions to another file, so the general Paxos       algorithm remains in the original file. Alternatively, all the leader and acceptor functions could be moved to separate files, but this might make the code harder to follow.
   
- Add support for an arbitrary (odd) number of nodes
  - This would be an easy change, but because the project is meant to be a proof of concept rather than a legitimate transaction manager, it is not integral.
  
- Rewrite the project in C
  - This could be done to improve scalability, as it would resolve global interpreter lock and greatly increase efficiency. However, this would greatly increase the complexity of the code. As mentioned above, this project is meant to be a proof of concept, so scalability is not a serious concern.

- Combine all the locks into one
  - Due to global interpreter lock, the impact on efficiency would be marginal. This change would make the code cleaner, but it's a poor design choice overall.
  
 
