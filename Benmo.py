import sys, os, socket, threading, time, random
import pickle
import json
from _thread import *


# Network state
global quorum_size
global node_addr_matrix
link_severed = [False, False, False]

# Process state
global my_id
global balance
global transactions
global blockchain
global depth

# Paxos state
in_paxos = False
global ballot_num
accept_num = (0, 0)
accept_block = None

# Locks
transaction_lock = threading.Lock()
blockchain_lock = threading.Lock()
ballot_lock = threading.Lock()
accept_lock = threading.Lock()
free_locks = [transaction_lock, blockchain_lock, ballot_lock]


'''
Global utility functions for saving/loading state, ballot comparisons, and state updates
'''

def save_state():
    global balance
    global transactions
    global blockchain
    global depth
    global ballot_num

    state = open("state" + str(my_id) + ".txt", "w")

    for lock in free_locks:
        lock.acquire()

    balance_str = "balance:" + str(balance) + "\n"
    transaction_str = "transactions:" + json.dumps(transactions) + "\n"
    blockchain_str = "blockchain:" + json.dumps(blockchain) + "\n"
    depth_str = "depth:" + str(depth) + "\n"
    ballot_num_str = "ballotNum:" + json.dumps(ballot_num) + "\n"

    state.write(balance_str)
    state.write(transaction_str)
    state.write(blockchain_str)
    state.write(depth_str)
    state.write(ballot_num_str)

    for lock in free_locks:
        lock.release()

def load_state():
    global balance
    global transactions
    global blockchain
    global depth
    global ballot_num

    state = open("state" + str(my_id) + ".txt", "r")

    for lock in free_locks:
        lock.acquire()

    lines = state.readlines()
    balance = int(lines[0].split(":")[1])
    transactions = json.loads(lines[1].split(":")[1])
    blockchain = json.loads(lines[2].split(":")[1])
    depth = int(lines[3].split(":")[1])
    ballot_num = json.loads(lines[4].split(":")[1])

    for lock in free_locks:
        lock.release()

def is_greater(ballot1, ballot2):
    if ballot1[0] > ballot2[0] or (ballot1[0] == ballot2[0] and ballot1[1] > ballot2[1]):
        return True
    else:
        return False

def update_balance(block):
    global my_id
    global balance

    for amount, debit_node, credit_node in block:
        if debit_node == my_id:
            balance -= amount
        if credit_node == my_id:
            balance += amount

def update_chain(update):
    global depth
    global blockchain

    for block in update:
        update_balance(block)
        blockchain.append(block)
        depth += 1
        save_state()


''' 
Leader functions for communicating with processes, getting the highest priority acceptVal,
determining if quorum has been reached, and executing Paxos as leader.
'''

def prepare(connection, acks, crashed_nodes):
    global ballot_num
    global depth

    prepare = ("prepare", ballot_num, depth)
    message = pickle.dumps(prepare)

    try:
        time.sleep(2)
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        crashed_nodes.append(connection)
        return

    try:
        data = connection.recv(1024)
    except (ConnectionError, socket.timeout) as e:
        crashed_nodes.append(connection)
        return
    if not data:
        crashed_nodes.append(connection)
        return

    ack = pickle.loads(data)
    acks.append(ack)

def get_priority_block(acks):
    global ballot_num
    global depth

    priority_block = (None, (0, 0))
    for ack in acks:
        header, ack_ballot, prev_accept_num, prev_accept_block, ack_depth, update = ack

        blockchain_lock.acquire()
        free_locks.remove(blockchain_lock)
        
        if ack_depth > depth and update != None:
            update_chain(update)
            
        blockchain_lock.release()
        free_locks.append(blockchain_lock)

        if prev_accept_block is not None:
            if is_greater(prev_accept_num, priority_block[1]):
                priority_block = (prev_accept_block, prev_accept_num)

    return transactions if priority_block[0] is None else priority_block[0]

def propose(id, connection, proposal, ballot, accepts, needs_update, crashed_nodes):
    global depth

    accept = ("accept", ballot, depth, proposal)
    message = pickle.dumps(accept)

    try:
        time.sleep(2)
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        return

    try:
        data = connection.recv(1024)
    except (ConnectionError, socket.timeout) as e:
        crashed_nodes.append(connection)
        return
    if not data:
        crashed_nodes.append(connection)
        return

    accepted = pickle.loads(data)
    accepts.append(accepted)

    blockchain_lock.acquire()
    free_locks.remove(blockchain_lock)

    recv_depth = accepted[3]
    if recv_depth < depth:
        needs_update.append((id, connection, recv_depth))

    blockchain_lock.release()
    free_locks.append(blockchain_lock)

def send_decision(connection, decided_num, decided_depth, decided_block, crashed_nodes):

    decision = ("decision", decided_num, decided_depth, decided_block)
    message = pickle.dumps(decision)

    try:
        time.sleep(2)
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        crashed_nodes.append(connection)
        return

def send_update(connection, recv_depth, saved_chain):
    global depth

    update = saved_chain[recv_depth:depth]
    message = pickle.dumps(update)
    try:
        time.sleep(2)
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        return

def quorum(responses):
    global quorum_size

    if len(responses) >= quorum_size:
        return True
    else:
        return False

def leader():
    global node_addr_matrix
    
    global my_id
    global transactions
    global depth

    global in_paxos
    global ballot_num

    timer = random.randint(20, 60)
    time.sleep(timer)
    in_paxos = True

    ballot_num = [ballot_num[0] + 1, my_id]
    save_state()

    connections = []
    crashed_nodes = []
    for id, row in enumerate(node_addr_matrix):
        if link_severed[id]:
            continue
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            connection.connect(row[my_id])
        except (ConnectionError, socket.timeout) as e:
            continue
        connections.append(connection)

    acks = []
    threads = []
    for id, c in enumerate(connections):
        if link_severed[id]:
            continue
        thread = threading.Thread(name="prepare", target=prepare, args=(c, acks, crashed_nodes,))
        threads.append(thread)
        thread.start()
    for t in threads:
        if quorum(acks):
            break
        t.join()

    if not quorum(acks):
        start_new_thread(leader, ())
        return


    proposal = get_priority_block(acks)


    accepts = []
    needs_update = []
    threads = []
    for id, c in enumerate(connections):
        if link_severed[id] or c in crashed_nodes:
            continue
        thread = threading.Thread(name="propose", target=propose, args=(id, c, proposal, ballot_num, accepts, needs_update, crashed_nodes))
        threads.append(thread)
        thread.start()
    for t in threads:
        if quorum(accepts):
            break
        t.join()

    if not quorum(accepts):
        start_new_thread(leader, ())
        return

    saved_chain = list(blockchain)
    saved_depth = depth

    threads = []
    for id, c in enumerate(connections):
        if link_severed[id] or c in crashed_nodes:
            continue
        thread = threading.Thread(name="sendDecision", target=send_decision, args=(c, ballot_num, saved_depth, proposal, crashed_nodes))
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()

    threads = []
    for id, c, d in needs_update:
        if link_severed[id] or c in crashed_nodes:
            continue
        thread = threading.Thread(name="sendUpdate", target=send_update, args=(c, d, saved_chain,))
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()


    transaction_lock.acquire()
    free_locks.remove(transaction_lock)

    if proposal == transactions:
        transactions = []
        save_state()
    elif transactions != []:
        start_new_thread(leader, ())

    transaction_lock.release()
    free_locks.append(transaction_lock)
    in_paxos = False


''' 
Acceptor functions for communicating with leader, applying the decision to the blockchain, 
receiving data from leader, and acting as an acceptor in an instance of Paxos.
'''

def send_ack(connection, leader_num, leader_depth):
    global ballot_num
    global depth
    global accept_num
    global accept_block

    ballot_lock.acquire()
    free_locks.remove(ballot_lock)

    if is_greater(leader_num, ballot_num) or leader_num == ballot_num:
        ballot_num = leader_num
        save_state()
    else:
        ballot_lock.release()
        free_locks.append(ballot_lock)
        return False

    ballot_lock.release()
    free_locks.append(ballot_lock)

    if leader_depth < depth:
        update = blockchain[leader_depth:depth]
    else:
        update = None

    ack = ("ack", leader_num, accept_num, accept_block, depth, update)
    message = pickle.dumps(ack)
    try:
        time.sleep(2)
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        return False

    return True

def send_accept(connection, accept_bal, proposal):
    global accept_num
    global accept_block

    ballot_lock.acquire()
    accept_lock.acquire()
    free_locks.remove(ballot_lock)

    if is_greater(accept_bal, accept_num) or accept_bal == accept_num:
        accept_block = proposal
        accept_num = accept_bal
    else:
        accept_lock.release()
        ballot_lock.release()
        free_locks.append(ballot_lock)
        return False

    accept_lock.release()
    ballot_lock.release()
    free_locks.append(ballot_lock)

    accept = ("accept", accept_bal, proposal, depth)
    message = pickle.dumps(accept)
    try:
        time.sleep(2)
        connection.sendall(message)
    except (ConnectionError, socket.timeout) as e:
        return False

    return True

def decide(connection, decision_depth, decision):
    global depth
    global transactions
    global blockchain

    blockchain_lock.acquire()
    free_locks.remove(blockchain_lock)

    if decision_depth > depth:
        try:
            data = connection.recv(1024)
        except (ConnectionError, socket.timeout) as e:
            blockchain_lock.release()
            free_locks.append(blockchain_lock)
            return False

        if not data:
            blockchain_lock.release()
            free_locks.append(blockchain_lock)
            return False
        
        update_chain(pickle.loads(data))

    update_balance(decision)
    blockchain.append(decision)
    depth += 1
    if decision == transactions:
        transactions = []
    save_state()

    blockchain_lock.release()
    free_locks.append(blockchain_lock)

    return True

def get_data(connection, listener, id):
    if link_severed[id]:
        return None
    try:
        data = connection.recv(1024)
    except (ConnectionError, socket.timeout) as e:
        start_new_thread(acceptor, (listener, id))
        return None
    if not data:
        start_new_thread(acceptor, (listener, id))
        return None

    return data

def acceptor(listener, id):
    global link_severed
    global accept_block
    global accept_num

    connection, address = listener.accept()

    data = get_data(connection, listener, id)
    if data == None:
        return

    header, leader_num, leader_depth = pickle.loads(data)
    if send_ack(connection, leader_num, leader_depth) == False:
        start_new_thread(acceptor, (listener, id))
        return

    data = get_data(connection, listener, id)
    if data == None:
        return

    header, accept_bal, accept_depth, proposal = pickle.loads(data)
    if send_accept(connection, accept_bal, proposal) == False:
        start_new_thread(acceptor, (listener, id))
        return

    data = get_data(connection, listener, id)
    if data == None:
        return

    header, deciced_ballot, decision_depth, decision = pickle.loads(data)
    made_decision = decide(connection, decision_depth, decision)
    if not made_decision:
        start_new_thread(acceptor, (listener, id))
        return

    accept_lock.acquire()
    accept_block = None
    accept_num = (0, 0)
    accept_lock.release()

    start_new_thread(acceptor, (listener, id))


''' 
Utility functions for parsing the network addresses from the config file, opening listeners
and starting the corresponding acceptor threads, and processing a transaction and starting an
instance of Paxos as leader.
'''

def parse_addrs(config):

    matrix = []
    row = []
    for c in config:
        ip, ports = c.split(",")
        port_list = ports.split(":")

        for p in port_list:
            row.append((ip, int(p)))
        matrix.append(row)
        row = []

    return matrix

def open_listeners():
    global my_id
    global node_addr_matrix

    listeners = []
    for (id, (myIP, myPort)) in enumerate(node_addr_matrix[my_id]):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listeners.append(listener)
        listener.bind((myIP, myPort))
        listener.listen(1)

        start_new_thread(acceptor, (listener, id,))

    return listeners

def process_transaction(amount, debit_node, credit_node):
    global transactions

    if not in_paxos:
        start_new_thread(leader, ())

    transaction_lock.acquire()
    free_locks.remove(transaction_lock)

    if len(transactions) < 10:
        transactions.append([amount, debit_node, credit_node])
        save_state()
    else:
        print("Error: Transaction log is full. Please wait for consensus.")

    transaction_lock.release()
    free_locks.append(transaction_lock)

def Main():
    global my_id
    global quorum_size
    global node_addr_matrix

    global balance
    global transactions
    global blockchain

    my_id = int(input("Enter process ID: "))
    while my_id > 4:
        my_id = int(input("Error: Cannot have more than 5 nodes. Please input a valid process ID (0 - 4): "))
    load_state()

    if not in_paxos and transactions != []:
        start_new_thread(leader, ())

    config = open("openstack_config.txt").read().splitlines()
    node_addr_matrix = parse_addrs(config)
    quorum_size = int(len(node_addr_matrix) / 2) + 1
    listeners = open_listeners()

    while True:
        print("\nPlease select one of the following options:\n"
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
            amount, debit_node, credit_node = map(int, (transaction.split(",")))

            if amount > balance:
                print("Error: Not enough money to transfer $" + str(amount) + "\n")
                continue

            if debit_node < 0 or debit_node > 4:
                print("Error: Node " + str(debit_node) + " does not exist\n")
                continue

            if credit_node < 0 or credit_node > 4:
                print("Error: Node " + str(credit_node) + " does not exist\n")
                continue

            if debit_node != my_id:
                print("Error: Cannot transfer another nodes funds\n")
                continue

            process_transaction(amount, debit_node, credit_node)

        elif command == "printBlockchain":
            print(blockchain)
            
        elif command == "printBalance":
            print(balance)
            
        elif command == "printQueue":
            print(transactions)
            
        elif command == "crashNode":
            sys.exit()
            
        elif command == "severLink":
            target_node = int(input("Please enter node to sever link with: "))
            if link_severed[target_node]:
                print("Error: Link with node " + str(target_node) + " already severed.\n")
            else:
                link_severed[target_node] = True
                
        elif command == "restoreLink":
            target_node = int(input("Please enter node to restore link with: "))
            if not link_severed[target_node]:
                print("Error: Link with node " + str(target_node) + " has not been severed.\n")
            else:
                link_severed[target_node] = False
                start_new_thread(acceptor, (listeners[target_node], target_node))

        else:
            print("Error: Invalid command\n")
            continue


if __name__ == "__main__":
    Main()
