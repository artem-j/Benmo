import sys, os, socket, threading, time
import pickle
import json
from _thread import *

global myID
global nodeAddrMatrix
listeners = []
linkSevered = [False, False, False]

global balance
global transactions
global blockchain
global depth

global ballotNum
acceptNum = (0, 0)
acceptBlock = None

transactionLock = threading.Lock()
blockchainLock = threading.Lock()
ballotLock = threading.Lock()
acceptLock = threading.Lock()
freeLocks = [transactionLock, blockchainLock, ballotLock]

def saveState():
    global balance
    global transactions
    global blockchain
    global depth
    global ballotNum

    state = open("state" + str(myID) + ".txt", "w")

    for lock in freeLocks:
        lock.acquire()

    balanceStr = "balance:" + str(balance) + "\n"
    transactionStr = "transactions:" + json.dumps(transactions) + "\n"
    blockchainStr = "blockchain:" + json.dumps(blockchain) + "\n"
    depthStr = "depth:" + str(depth) + "\n"
    ballotNumStr = "ballotNum:" + json.dumps(ballotNum) + "\n"

    state.write(balanceStr)
    state.write(transactionStr)
    state.write(blockchainStr)
    state.write(depthStr)
    state.write(ballotNumStr)

    for lock in freeLocks:
        lock.release()


def loadState():
    global balance
    global transactions
    global blockchain
    global depth
    global ballotNum

    state = open("state" + str(myID) + ".txt", "r")

    for lock in freeLocks:
        lock.acquire()

    lines = state.readlines()
    balance = int(lines[0].split(":")[1])
    transactions = json.loads(lines[1].split(":")[1])
    blockchain = json.loads(lines[2].split(":")[1])
    depth = int(lines[3].split(":")[1])
    ballotNum  = json.loads(lines[4].split(":")[1])

    for lock in freeLocks:
        lock.release()


def isGreater(ballot1, ballot2):
    if ballot1[0] > ballot2[0] or (ballot1[0] == ballot2[0] and ballot1[1] > ballot2[1]):
        return True
    else:
        return False


def updateChain(update):
    #print("Entering updateChain")
    global depth
    global blockchain

    for block in update:
        blockchain.append(block)
        depth += 1
        saveState()
    #print("updateChain returned successfully")

# Leader functions

def prepare(connection, acks, crashedNodes):
    print("Entering prepare")
    global ballotNum
    global depth

    prepare = ("prepare", ballotNum, depth)
    message = pickle.dumps(prepare)

    try:
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        crashedNodes.append(connection)
        return

    try:
        data = connection.recv(1024)
    except (ConnectionError, socket.timeout) as e:
        crashedNodes.append(connection)
        return
    if not data:
        crashedNodes.append(connection)
        return

    ack = pickle.loads(data)
    acks.append(ack)
    print("Prepare returned successfully")

def getPriorityBlock(acks):
    #print("Entering getPriorityBlock")
    global ballotNum
    global depth

    priorityBlock = (None, (0, 0))
    for ack in acks:
        header, ackBallot, prevAcceptNum, prevAcceptBlock, ackDepth, update = ack

        blockchainLock.acquire()
        freeLocks.remove(blockchainLock)
        if ackDepth > depth and update != None:
            updateChain(update)
        blockchainLock.release()
        freeLocks.append(blockchainLock)

        if prevAcceptBlock is not None:
            if isGreater(prevAcceptNum, priorityBlock[1]):
                priorityBlock = (prevAcceptBlock, prevAcceptNum)

    #print("getPriorityBlock returned successfully")
    return transactions if priorityBlock[0] is None else priorityBlock[0]


def propose(id, connection, proposal, ballot, accepts, needsUpdate, crashedNodes):
    #print("Entering propose")
    global depth

    accept = ("accept", ballot, depth, proposal)  # Continue with proposal if chain updated to match depth?
    message = pickle.dumps(accept)

    try:
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        return

    try:
        data = connection.recv(1024)
    except (ConnectionError, socket.timeout) as e:
        crashedNodes.append(connection)
        return
    if not data:
        crashedNodes.append(connection)
        return

    accepted = pickle.loads(data)
    accepts.append(accepted)

    blockchainLock.acquire()
    freeLocks.remove(blockchainLock)

    recvDepth = accepted[3]
    if recvDepth < depth:
        needsUpdate.append((id, connection, recvDepth))

    blockchainLock.release()
    freeLocks.append(blockchainLock)
    #print("Propose returned successfully")

def sendDecision(connection, decidedNum, decidedDepth, decidedBlock, crashedNodes):
    #print("Entering sendDecision")

    decision = ("decision", decidedNum, decidedDepth, decidedBlock)
    message = pickle.dumps(decision)

    try:
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        crashedNodes.append(connection)
        return
    #print("sendDecision returned successfully")


def sendUpdate(connection, recvDepth, savedChain):
    #print("Entering sendUpdate")
    global depth

    update = savedChain[recvDepth:depth]
    message = pickle.dumps(update)
    try:
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        return
    #print("sendUpdate returned successfully")

def leader():
    print("Entering leader")
    global myID
    global nodeAddrMatrix
    global transactions
    global ballotNum
    global depth

    time.sleep(5)

    ballotNum = (ballotNum[0] + 1, myID)
    saveState()

    connections = []
    crashedNodes = []
    for id, row in enumerate(nodeAddrMatrix):
        if linkSevered[id]:
            continue
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.settimeout(3)
        try:
            connection.connect(row[myID])
        except (ConnectionError, socket.timeout) as e:
            print("Failed to connect")
            continue
        connections.append(connection)

    acks = []
    threads = []
    for id, c in enumerate(connections):
        if linkSevered[id]:
            continue
        thread = threading.Thread(name="prepare", target=prepare,
                                  args=(c, acks, crashedNodes,))
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()
    if (len(acks) < 2):  # change 3 to quorum size later
        start_new_thread(leader, ())
        return

    proposal = getPriorityBlock(acks)

    accepts = []
    needsUpdate = []
    threads = []
    for id, c in enumerate(connections):
        if linkSevered[id] or c in crashedNodes:
            continue
        thread = threading.Thread(name="propose", target=propose,
                                  args=(id, c, proposal, ballotNum, accepts, needsUpdate, crashedNodes))
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()
    if len(accepts) < 2:  # change 3 to quorum size later
        start_new_thread(leader, ())
        return

    savedChain = list(blockchain)
    savedDepth = depth
    threads = []
    for id, c in enumerate(connections):
        if linkSevered[id] or c in crashedNodes:
            continue
        thread = threading.Thread(name="sendDecision", target=sendDecision,
                                  args=(c, ballotNum, savedDepth, proposal, crashedNodes))
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()

    threads = []
    for id, c, d in needsUpdate:
        if linkSevered[id] or c in crashedNodes:
            continue
        thread = threading.Thread(name="sendUpdate", target=sendUpdate,
                                  args=(c, d, savedChain,))
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()

    transactionLock.acquire()
    freeLocks.remove(transactionLock)

    if proposal == transactions:
        transactions = []
        saveState()
    elif transactions != []:
        start_new_thread(leader, ())

    transactionLock.release()
    freeLocks.append(transactionLock)
    print("leader returned successfully")


# Acceptor functions
def sendAck(connection, leaderNum, leaderDepth):
    print("Entering sendAck")
    global ballotNum
    global depth
    global acceptNum
    global acceptBlock

    ballotLock.acquire()
    freeLocks.remove(ballotLock)

    if isGreater(leaderNum, ballotNum) or leaderNum == ballotNum:
        ballotNum = leaderNum
        saveState()
    else:
        ballotLock.release()
        freeLocks.append(ballotLock)
        return False

    ballotLock.release()
    freeLocks.append(ballotLock)

    if leaderDepth < depth:
        update = blockchain[leaderDepth:depth]
    else:
        update = None

    ack = ("ack", leaderNum, acceptNum, acceptBlock, depth, update)
    message = pickle.dumps(ack)
    try:
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        return False

    print("sendAck returned successfully")
    return True


def sendAccept(connection, acceptBal, proposal):
    #print("Entering sendAccept")
    global acceptNum
    global acceptBlock

    ballotLock.acquire()
    acceptLock.acquire()
    freeLocks.remove(ballotLock)

    if isGreater(acceptBal, acceptNum) or acceptBal == acceptNum:
        acceptBlock = proposal
        acceptNum = acceptBal
    else:
        acceptLock.release()
        ballotLock.release()
        freeLocks.append(ballotLock)
        return False

    acceptLock.release()
    ballotLock.release()
    freeLocks.append(ballotLock)

    accept = ("accept", acceptBal, proposal, depth)
    message = pickle.dumps(accept)
    try:
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        return False

    #print("sendAccept returned successfully")
    return True

def decide(connection, decisionDepth, decision):
    #print("Entering applyDecision")
    global depth
    global transactions
    global blockchain

    blockchainLock.acquire()
    freeLocks.remove(blockchainLock)

    if decisionDepth > depth:
        try:
            data = connection.recv(1024)
        except (ConnectionError, socket.timeout) as e:
            blockchainLock.release()
            freeLocks.append(blockchainLock)
            return False

        if not data:
            blockchainLock.release()
            freeLocks.append(blockchainLock)
            return False
        updateChain(pickle.loads(data))

    blockchain.append(decision)
    depth += 1
    if decision == transactions:  # REMEMBER TO FIX THIS
        transactions = []

    saveState()

    blockchainLock.release()
    freeLocks.append(blockchainLock)

    #print("applyDecision returned successfully")
    return True

def acceptor(listener, id):
    print("Entering acceptor for " + str(id))
    global linkSevered
    global acceptBlock
    global acceptNum

    connection, address = listener.accept()

    if linkSevered[id]:
        return
    try:
        data = connection.recv(1024)
    except (ConnectionError, socket.timeout) as e:
        start_new_thread(acceptor, (listener, id))
        return
    if not data:
        start_new_thread(acceptor, (listener, id))
        return

    header, leaderNum, leaderDepth = pickle.loads(data)
    if sendAck(connection, leaderNum, leaderDepth) == False:
        start_new_thread(acceptor, (listener, id))
        return


    if linkSevered[id]:
        return
    try:
        data = connection.recv(1024)
    except (ConnectionError, socket.timeout) as e:
        start_new_thread(acceptor, (listener, id))
        return
    if not data:
        start_new_thread(acceptor, (listener, id))
        return

    header, acceptBal, acceptDepth, proposal = pickle.loads(data)
    if sendAccept(connection, acceptBal, proposal) == False:
        start_new_thread(acceptor, (listener, id))
        return

    if linkSevered[id]:
        return
    try:
        data = connection.recv(1024)
    except (ConnectionError, socket.timeout) as e:
        start_new_thread(acceptor, (listener, id))
        return
    if not data:
        start_new_thread(acceptor, (listener, id))
        return

    header, decicedBallot, decisionDepth, decision = pickle.loads(data)
    madeDecision = decide(connection, decisionDepth, decision)
    if not madeDecision:
        start_new_thread(acceptor, (listener, id))
        return

    print(blockchain)

    acceptLock.acquire()
    acceptBlock = None
    acceptNum = (0, 0)
    acceptLock.release()

    start_new_thread(acceptor, (listener, id))
    print("acceptor returned successfully")


def parseAddrs(config):

    matrix = []
    row = []
    for c in config:
        ip, ports = c.split(",")
        portList = ports.split(":")

        for p in portList:
            row.append((ip, int(p)))
        matrix.append(row)
        row = []

    return matrix

def openListeners():
    global myID
    global nodeAddrMatrix
    global listeners

    listeners = []
    for (id, (myIP, myPort)) in enumerate(nodeAddrMatrix[myID]):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listeners.append(listener)
        listener.bind((myIP, myPort))
        listener.listen(1)

        start_new_thread(acceptor, (listener, id,))


def processTransaction(amount, debitNode, creditNode):
    global balance
    global transactions

    transactionLock.acquire()
    freeLocks.remove(transactionLock)
    if transactions == []: ## Fix later -- causes failure if starting from iniitalized with nonempty transacs
        start_new_thread(leader, ())

    if len(transactions) < 10:
        balance -= amount
        transactions.append((amount, debitNode, creditNode))
        saveState()
    else:
        print("Error: Transaction log is full. Please wait for consensus.")
    transactionLock.release()
    freeLocks.append(transactionLock)

def Main():
    global myID
    global nodeAddrMatrix
    global listeners
    global ballotNum

    global balance
    global transactions
    global blockchain
    global depth

    myID = int(input("Enter process ID: "))
    loadState()

    config = open("config.txt").read().splitlines()
    nodeAddrMatrix = parseAddrs(config)
    openListeners()

    while True:
        print("Please select one of the following options:\n"
              "moneyTransfer\n"
              "printBlockchain\n"
              "printBalance\n"
              "printQueue\n"
              "crashNode\n"
              "severLink\n"
              "restoreLink\n")

        command = input("Enter selection: ")
        if command == "moneyTransfer":
            transaction = input("Please enter amount,debit_node,credit_node: ")
            amount, debitNode, creditNode = map(int, (transaction.split(",")))

            if amount > balance:
                print("Error: Not enough money to transfer $" + str(amount) + "\n")
                continue

            if debitNode < 0 or debitNode > 2:
                print("Error: Node " + str(debitNode) + " does not exist\n")
                continue

            if creditNode < 0 or creditNode > 2:
                print("Error: Node " + str(creditNode) + " does not exist\n")
                continue

            processTransaction(amount, debitNode, creditNode)
            print("")

        elif command == "printBlockchain":
            print(blockchain)
        elif command == "printBalance":
            print(balance)
        elif command == "printQueue":
            print(transactions)
        elif command == "crashNode":
            sys.exit()
        elif command == "severLink":
            targetNode = int(input("Please enter node to sever link with: "))
            if linkSevered[targetNode]:
                print("Error: Link with node " + str(targetNode) + " already severed.")
            else:
                linkSevered[targetNode] = True
        elif command == "restoreLink":
            targetNode = int(input("Please enter node to restore link with: "))
            if not linkSevered[targetNode]:
                print("Error: Link with node " + str(targetNode) + " has not been severed.")
            else:
                linkSevered[targetNode] = False
                print(listeners)
                start_new_thread(acceptor, (listeners[targetNode], targetNode))
                '''listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                while True:
                    try:
                        print("Attempting to bind to " + str(nodeAddrMatrix[myID][targetNode]))
                        listener.bind(nodeAddrMatrix[myID][targetNode])
                        print("Successfully connected")
                        break
                    except OSError:
                        print("Stuck")
                        time.sleep(2)
                        continue
                print("About to start accept thread with " + str(listener))
                '''

        else:
            print("Error: Invalid command\n")
            continue


if __name__ == "__main__":
    Main()