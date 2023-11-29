import mariadb
from typing import List
import models
from datetime import datetime


class DatabaseOperator:
    def __init__(self, user = 'root', password = 'root', host = '127.0.0.1', port = 3306, db_name = 'datasquid'):
        try:
            self.conn = mariadb.connect(
                user=user,
                password=password,
                host=host,
                port=port,
                database=db_name
            )
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
        self.cur = self.conn.cursor()

    def executeQuery(self, query:str, args, needFetch = False):
        ret = None 
        try:
            self.cur.execute(query, args)
        except Exception as ex:
            print(f"Error executing a query: {ex}")


        if needFetch:
            try:
                ret = self.cur.fetchall()
            except Exception as ex:
                print(f"Error fetching a query: {ex}")


        if ret is None:
            try:
                self.conn.commit()
            except Exception as ex:
                print(f"Error commiting a query: {ex}")

        return ret

FOREIGN_ID = '0693ead4-8d6a-11ee-a12b-e2cad36a8b44'.replace('-', '')
ORG_ID = '0684c02c-8d6a-11ee-a12b-e2cad36a8b44'.replace('-', '')

class DatabaseAdaptor:
    def __init__(self, databaseOperator : DatabaseOperator):
        self.operator = databaseOperator

    def queryActiveUsers(self) -> List:
        ret = self.operator.executeQuery("SELECT userId FROM Users WHERE isUserLoggedIn(userId) = TRUE;", None, needFetch=True)

        return [] if ret is None else [x[0].hex() for x in ret]
    
    def flush(self):
        self.operator.executeQuery("SET FOREIGN_KEY_CHECKS = 0;", None)
        self.operator.executeQuery("DELETE FROM users;", None)
        self.operator.executeQuery("DELETE FROM UserActions;", None)
        self.operator.executeQuery("DELETE FROM UserStatistics;", None)
        self.operator.executeQuery("SET FOREIGN_KEY_CHECKS = 1;", None)

    def queryLatestUserStatistics(self, userId):
        ret = self.operator.executeQuery("CALL getLastUserStatistic(?);", (bytes.fromhex(userId.replace('-', '')), ), needFetch=True)
        if ret is None or len(ret) < 1: return None
        return models.Stats.fromSql(*ret[0])
    def queryActiveDatasets(self) -> List:
        pass 
    def queryUserActions(self, userId, fromTime, toTime):
        ret = self.operator.executeQuery('SELECT * FROM UserActions WHERE userId = ? AND actionTime BETWEEN ? AND ?',
                                          (bytes.fromhex(userId.replace('-', '')), 
                                           datetime.fromtimestamp(fromTime/1_000).strftime('%Y-%m-%d %H:%M:%S'),
                                           datetime.fromtimestamp(toTime/1_000).strftime('%Y-%m-%d %H:%M:%S')),
                                           needFetch=True)
        
        return [] if ret is None else [models.Action.fromSql(*x) for x in ret]
    def writeNewStats(self, stats:models.Stats):
        self.operator.executeQuery("INSERT INTO UserStatistics (statisticsId, userId, content, statisticsTime) VALUES (UNHEX(REPLACE(UUID(), '-', '')), ?, ?, NOW())", 
                                          (bytes.fromhex(stats.userId.replace('-', '')), stats.content))
    def writeNewAction(self, action:models.Action):
        self.operator.executeQuery("INSERT INTO UserActions (actionId, userId, actionType, actionContent, actionTime) VALUE (UNHEX(REPLACE(UUID(), '-', '')), ?, ?, ?, NOW())", 
                                   (bytes.fromhex(action.userId.replace('-', '')), action.actionType.value, action.actionContent))
    def writeNewDataset(self, content):
        self.operator.executeQuery("INSERT INTO DataDumpEntries (entryId, content, entryTime, dataDumpId) VALUE (UNHEX(REPLACE(UUID(), '-', '')), ?, NOW(), ?)", (content, bytes.fromhex(FOREIGN_ID))) 
    def writeNewUser(self, user : models.User):
        self.operator.executeQuery("INSERT INTO `users` (userId, userName, organisationId, creationTime) value (?, ?, ?, NOW())", 
                                          (bytes.fromhex(user.userId.replace('-', '')), user.username, bytes.fromhex(ORG_ID)))


class SqlOnlyDatabaseAdaptor(DatabaseAdaptor):
    def __init__(self, databaseOperator:DatabaseOperator):
        super().__init__(databaseOperator)
    def queryActiveUsers(self) -> List:
        return self.operator.executeQuery("SELECT * FROM `ActiveUsers`")