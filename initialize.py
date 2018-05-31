import json

balance = 100
transactions = []
blockchain = []
depth = 0


for i in range(5):
    ballotNum = (0, i)
    state = open("state" + str(i) + ".txt", "w")

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