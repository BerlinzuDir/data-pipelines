import random
import csv
import os

file_name = os.path.dirname(os.path.realpath(__file__)) + '/user_agents.csv'
user_agent_csv = open(file_name, 'r')
user_agents = csv.reader(user_agent_csv)
user_agents = [row for row in user_agents]
user_agents = [user_agent[0] for user_agent in user_agents]


def generate_header(seed=None):
    if seed:
        random.seed(seed)
    return {'User-Agent': random.choice(user_agents)}
