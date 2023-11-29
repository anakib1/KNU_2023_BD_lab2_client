from DataSquidClient import DataSquidClient, DataSquidClientEx
from DatabaseAdaptor import DatabaseAdaptor, DatabaseOperator
from RedisAdaptor import RedisAdaptor
import random
import models
from time import time
import uuid

class Stress:
    def __init__(self, client : DataSquidClient | DataSquidClientEx, total_users:int, logged_in_users:int, total_datasets:int, total_iterations:int, seed = 0):
        random.seed(seed)
        self.users = []
        self.datasets = []
        for i in range(total_users):
            user = models.User(uuid.uuid4(), f'PerformanceUser{i}')
            client.createNewUser(user)
            self.users.append(user)
        
        for i in range(total_datasets):
            self.datasets.append(i)
            client.createNewDataset()
        
        self.client = client

        self.total_datasets = total_datasets
        self.logged_in_users = logged_in_users
        self.total_iterations = total_iterations


    def run_testing(self):
        start_time = time()

        for user in self.users[:self.logged_in_users]:
            self.client.loginExistingUser(userId=user.userId)

        for iteration in range(self.total_iterations):
            for user in self.users[:self.logged_in_users]:
                chosen_dataset = random.randint(0, self.total_datasets - 1)
                self.client.downloadDatasetForUser(user.userId, chosen_dataset)
            
            self.client.updateStats()

        for user in self.users[:self.logged_in_users]:
            self.client.logoutExistingUser(userId=user.userId)

        end_time = time()

        return (end_time - start_time)

    def initRunTesting(useEx:bool, total:int, loggedIn:int, datasets:int, iterations:int):
        client = DataSquidClientEx(DatabaseAdaptor(DatabaseOperator()), RedisAdaptor()) if useEx else DataSquidClient(DatabaseAdaptor(DatabaseOperator()))
    
        stress = Stress(
            client=client,
            total_users=total,
            logged_in_users=loggedIn,
            total_datasets=datasets,
            total_iterations=iterations
        )

        return stress.run_testing()
    
    
if __name__ == '__main__':
    print(f'Elapsed: {Stress.initRunTesting(False, 10000, 1000, 1000, 100)}')

