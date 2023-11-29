from DatabaseAdaptor import DatabaseAdaptor
from RedisAdaptor import RedisAdaptor
from models import *
from time import time_ns
import uuid
import binascii

class DataSquidClient:
    def __init__(self, adaptor:DatabaseAdaptor):
        self.adaptor = adaptor
        self.adaptor.flush()
        self.users = []
        self.datasets = []
    def createNewUser(self, user:User):
        self.users.append(user)
        self.adaptor.writeNewUser(self.users[-1])
    def loginExistingUser(self, userId):
        self.adaptor.writeNewAction(Action(userId, ActionType.LOGIN, 'Performance user login'))
    def logoutExistingUser(self, userId):
        self.adaptor.writeNewAction(Action(userId, ActionType.LOGOUT, 'Performance user logout'))
    def createNewDataset(self):
        self.datasets.append(f'PerformanceDataset{len(self.datasets)}')
        self.adaptor.writeNewDataset(self.datasets[-1])

    def downloadDatasetForUser(self, userId, datasetId):
        self.adaptor.writeNewAction(Action(userId, ActionType.DOWNLOAD_DATASET, f'Downloaded dataset #{datasetId}'))

    def updateStats(self):
        currentTime = int(time_ns()/1_000_000)
        users = self.adaptor.queryActiveUsers()
        for userId in users:
            latestStats = self.adaptor.queryLatestUserStatistics(userId)
            if latestStats == None:
                latestStats = Stats(userId)
                latestStats.timestamp = 0
            actions = self.adaptor.queryUserActions(userId, latestStats.timestamp, currentTime)
            latestStats.mergeWith(actions)
            self.adaptor.writeNewStats(latestStats)
        

class DataSquidClientEx:
    def __init__(self, adaptor:DatabaseAdaptor, redis:RedisAdaptor):
        self.adaptor = adaptor
        self.redis = redis
        self.redis.flush()
        self.adaptor.flush()
        self.users = {}
        self.datasets = []

    def createNewUser(self, user:User):
        self.users[user.userId] = user
        self.adaptor.writeNewUser(user)
    

    def loginExistingUser(self, userId):
        action = Action(userId, ActionType.LOGIN, 'Performance user login')
        self.adaptor.writeNewAction(action)
        self.redis.addUserToActiveSet(self.users[userId])
        self.redis.addActionEntry(action)

    def logoutExistingUser(self, userId):
        action = Action(userId, ActionType.LOGOUT, 'Performance user logout')
        self.adaptor.writeNewAction(action)
        self.redis.removeUserFromActiveSet(self.users[userId])
        self.redis.addActionEntry(action)

    def createNewDataset(self):
        self.datasets.append(f'PerformanceDataset{len(self.datasets)}')
        self.adaptor.writeNewDataset(self.datasets[-1])

    def downloadDatasetForUser(self, userId, datasetId):
        action = Action(userId, ActionType.DOWNLOAD_DATASET, f'Downloaded dataset #{datasetId}')
        self.adaptor.writeNewAction(action)
        self.redis.addActionEntry(action)

    def updateStats(self):
        currentTime = int(time_ns() / 1_000_000)
        users = self.redis.queryAllActiveUsers()
        for userId in users:
            latestStats = self.redis.queryLatestUserStats(userId)
            if latestStats == None:
                latestStats = Stats(userId)
                latestStats.timestamp = 0
            actions = self.redis.queryUserActions(userId, latestStats.timestamp, currentTime)
            latestStats.mergeWith(actions)
            self.adaptor.writeNewStats(latestStats)
            self.redis.addStatEntry(latestStats)