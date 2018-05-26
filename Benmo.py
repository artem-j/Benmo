import socket, threading, time
import pickle
from _thread import *

nodeAddrMatrix = []

balance = 100
transactions = []
blockchain = []
depth = 0

ballotNum = (0, 0)
acceptNum = (0, 0)
acceptBlock = None
acks = []
accepts = []

transactionLock = threading.Lock()
blockchainLock = threading.Lock()
ballotLock = threading.Lock()
acceptLock = threading.Lock()

# Leader functions
def prepare(connection):
    global ballotNum
    global depth
    global acks

    prepare = ("prepare", ballotNum, depth)
    message = pickle.dumps(prepare)
    connection.sendall(message)

    data = connection.recv(1024)
    if not data:
        return

    ack = pickle.loads(data)
    acks.append(ack)


def getPriorityBlock():
    global acks
    global ballotNum

    priorityBlock = (None, (0, 0))
    for ack in acks:
        header, ackBallot, prevAcceptNum, prevAcceptBlock, ackDepth = ack

        if prevAcceptBlock is not None:
            if prevAcceptNum[0] > priorityBlock[1][0] or (prevAcceptNum[0] == priorityBlock[1][0] and prevAcceptBlock[1] > priorityBlock[1][1]):
                priorityBlock = (prevAcceptBlock, prevAcceptNum)

    if priorityBlock[0] is None:
        proposal = transactions
        ballot = ballotNum
    else:
        proposal = priorityBlock[0]
        ballot = priorityBlock[1]

    return (proposal, ballot)


def propose(connection, proposal, ballot):
    global depth

    accept = ("accept", ballot, depth, proposal)  # Continue with proposal if chain updated to match depth?
    message = pickle.dumps(accept)
    connection.sendall(message)

    data = connection.recv(1024)
    if not data:
        return

    accepted = pickle.loads(data)
    accepts.append(accepted)


def makeDecision(proposal, ballot):
    global acceptBlock
    global acceptNum
    global transactions
    global blockchain

    acceptLock.acquire()
    acceptBlock = proposal
    acceptNum = ballot
    acceptLock.release()

    blockchain.append(proposal)

    transactionLock.acquire()
    if proposal == transactions:
        transactions = []
    else:
        start_new_thread(flushQueue, ())
    transactionLock.release()


def sendDecision(connection, decidedNum, decidedDepth, decidedBlock):
    decision = ("decision", decidedNum, decidedDepth, decidedBlock)
    message = pickle.dumps(decision)
    connection.sendall(message)


def leader():
    global nodeAddrs
    global ballotNum
    global depth
    global acceptBlock
    global acceptNum
    global acks
    global accepts

    ballotNum = (ballotNum[0] + 1, ballotNum[1])
    connections = []
    threads = []

    # Contact all the nodes and call prepare to become leader
    for row in nodeAddrMatrix:
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.connect(row[myID])
        connections.append(connection)

        thread = threading.Thread(name = "prepare", target = prepare, args = (connection,))
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()
    threads = []
    #if (len(acks) < 3): change 3 to quorum size later
        #return

    priorityBlock = getPriorityBlock()
    proposal = priorityBlock[0]
    ballot = priorityBlock[1] # do we update our ballotnum if priorityblock's is greater than ours? is this even possible?

    # Call propose to send the proposal
    for c in connections:
        thread = threading.Thread(name = "propose", target = propose, args = (c, proposal, ballot,))
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()
    threads = []
    #if len(accepts) < 3: # change 3 to quorum size later
        #return

    # Make the decision and send it to the quorum
    makeDecision(proposal, ballot)
    for c in connections:
        if c == connections[myID]:
            c.close()
        else:
            thread = threading.Thread(name="sendDecision", target=sendDecision, args=(c, ballot, depth, proposal,))
            threads.append(thread)
            thread.start()
    for t in threads:
        t.join()

    acceptLock.acquire()
    acceptBlock = None
    acceptNum = (0, 0)
    acceptLock.release()

    print(blockchain)

# Acceptor functions
def accept(connection):
    global ballotNum
    global depth
    global acceptBlock
    global acceptNum

    data = connection.recv(1024)
    if not data:
        return
    header, leaderNum, leaderDepth = pickle.loads(data)


    ballotLock.acquire()
    if leaderNum[0] > ballotNum[0] or (leaderNum[0] == ballotNum[0] and leaderNum[1] > ballotNum[1]):
        ballotNum = leaderNum
    elif leaderNum == ballotNum:
        pass
    else:
        return
    ballotLock.release()


    ack = ("ack", leaderNum, acceptNum, acceptBlock, depth)
    message = pickle.dumps(ack)
    connection.sendall(message)

    data = connection.recv(1024)
    if not data:
        return
    header, acceptBal, acceptDepth, proposal = pickle.loads(data)


    ballotLock.acquire()
    acceptLock.acquire()
    if acceptBal[0] > ballotNum[0] or (acceptBal[0] == ballotNum[0] and acceptBal[1] > ballotNum[1]):
        acceptBlock = proposal
        acceptNum = acceptBal
    elif acceptBal == ballotNum:
        pass
    else:
        return
    acceptLock.release()
    ballotLock.release()


    accept = ("accept", acceptBal, proposal, depth)
    message = pickle.dumps(message)
    connection.sendall(message)

    data = connection.recv(1024)
    if not data:
        return
    header, decicedBallot, decisionDepth, decision = pickle.loads(data)


    blockchain.append(decision)
    print(blockchain)

def acceptor(listener):
    while True:
        connection, address = listener.accept()
        start_new_thread(accept, (connection,))

def flushQueue():
    time.sleep(60)
    start_new_thread(leader, ())

def Main():
    global ballotNum
    global myID
    global nodeAddrMatrix

    myID = int(input("Enter process ID: "))
    ballotNum = (0, myID)

    configList  = open("config.txt").read().splitlines()

    row = []
    for c in configList:
        ip, ports = c.split(",")
        portList = ports.split(":")

        for p in portList:
            row.append((ip, int(p)))
        nodeAddrMatrix.append(row)
        row = []

    for (myIP, myPort) in nodeAddrMatrix[myID]:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind((myIP, myPort))
        listener.listen(1)

        start_new_thread(acceptor, (listener,))

    while True:
        print("Please select one of the following options:\n"
              "moneyTransfer\n")

        command = input("Enter selection: ")
        if command == "moneyTransfer":
            args = input("Please enter amount,debit_node,credit_node: ")
            amount = int(args.split(",")[0])
            debitNode = int(args.split(",")[1])
            creditNode = int(args.split(",")[2])

            if amount > balance:
                print("Error: Not enough money to transfer $" + str(amount) + "\n")
                continue

            if debitNode < 0 or debitNode > 2:
                print("Error: Node " + str(debitNode) + " does not exist\n")
                continue

            if creditNode < 0 or creditNode > 2:
                print("Error: Node " + str(creditNode) + " does not exist\n")
                continue

            transactionLock.acquire()
            if len(transactions) == 0:
                start_new_thread(flushQueue, ())

            if len(transactions) < 10:
                transactions.append((amount, creditNode))
            transactionLock.release()
            print("")

        else:
            print("Error: Invalid command\n")
            continue


if __name__ == "__main__":
    Main()