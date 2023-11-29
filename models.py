import enum
from typing import List
from time import time_ns
import json

class ActionType(enum.Enum):

    DOWNLOAD_DATASET = 1
    LOGIN = 2
    LOGOUT = 3

class Action:
    def __init__(self, userId, actionType:ActionType, actionContent:ActionType, timestamp = None):
        self.userId = str(userId)
        self.actionType = actionType
        self.actionContent = str(actionContent)
        if timestamp == None: timestamp = int(time_ns() / 1_000_000)
        self.actionTime = timestamp

    def fromSql(id, userId, type, content, time):
        return Action(userId.hex(), ActionType._value2member_map_[type], content, int(time.timestamp()))
    def fromRedis(userId, content, time):
        content = json.loads(content)
        return Action(userId, ActionType._member_map_[content['type']], content['content'], time)
        


class Stats:
    def __init__(self, userId, content = '{"loginCnt":0,"logoutCnt":0,"downloadCnt":0}', timestamp = 0):
        self.userId = str(userId)
        self.content = str(content)
        self.timestamp = timestamp
    def mergeWith(self, actions:List[Action]):
        if actions == None:
            return
        contentDict = json.loads(self.content)
        for act in actions:
            if act.actionType == ActionType.LOGIN:
                contentDict['loginCnt'] += 1
            elif act.actionContent == ActionType.LOGOUT:
                contentDict['logoutCnt'] += 1
            else:
                contentDict['downloadCnt'] += 1
            self.timestamp = max(self.timestamp, act.actionTime)
        self.content = json.dumps(contentDict)
    def fromSql(id, userId, content, time):
        return Stats(userId.hex(), content, int(time.timestamp()))
    def fromRedis(userId, contentDict, timestamp):
        return Stats(userId, contentDict, timestamp)

class User:
    def __init__(self, userId, userName):
        self.userId = str(userId)
        self.username = userName