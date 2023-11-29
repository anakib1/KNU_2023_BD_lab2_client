import redis
import models

class RedisAdaptor:
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, decode_responses=True, password='password')

    def flush(self):
        self.r.flushdb()

    def addUserToActiveSet(self, user:models.User):
        active_users_key = 'activeUsers'
        self.r.hset(active_users_key, user.userId.upper(), user.username)

    def removeUserFromActiveSet(self, user:models.User):
        active_users_key = 'activeUsers'
        self.r.hdel(active_users_key, user.userId.upper())
    
    def addStatEntry(self, stats:models.Stats):
        statistics_key = f'statistics:{stats.userId.upper()}'
        
        self.r.zadd(statistics_key, {stats.content: stats.timestamp})

    def addActionEntry(self, action:models.Action):
        actions_key = f'actions:{action.userId.upper()}'
        self.r.zadd(actions_key, {'{' + f'"content" : "{action.actionContent}", "type" : "{action.actionType.name}"' + '}': action.actionTime})

    def queryAllActiveUsers(self):
        active_users_key = 'activeUsers'
        
        active_users = self.r.hgetall(active_users_key)
        return active_users

    def queryLatestUserStats(self, user_id:str):
        statistics_key = f'statistics:{user_id.upper()}'
        
        latest_statistics = self.r.zrange(statistics_key, -1, -1, withscores=True)
        if latest_statistics:
            return models.Stats.fromRedis(user_id, *latest_statistics[0])
        else:
            return None

    def queryUserActions(self, user_id:str, start_timestamp, end_timestamp):
        actions_key = f'actions:{user_id.upper()}'
        
        user_actions = self.r.zrangebyscore(actions_key, start_timestamp, end_timestamp, withscores=True)
        return [] if user_actions is None else [models.Action.fromRedis(user_id, *x) for x in user_actions]