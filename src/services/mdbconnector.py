"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
Note:       This file contains a modified version of the database connector
            previously written by Paul Barry and redistributed as DBcm package.
            The major change in this file was introduced to support the MongoDB,
            rather than the initial MySQL.
"""

import pymongo


class MongoConnector:
    def __init__(self, config):
        """
        This class expects a single dictionary argument which needs to assign the
        appropriate values to (at least) the following keys:
            host - the IP address of the host running mongodb.
            user - the mongodb username to use.
            password - the user's password.
            database - the name of the database to use.
        """
        self.config = config

    def __enter__(self):
        """Connect to database and create a DB cursor.
        Return the database cursor to the context manager.
        """
        self.client = pymongo.MongoClient(self.config['host'])
        if 'user' in self.config and 'password' in self.config:
            self.client[self.config['database']].authenticate(self.config['user'],
                                                              self.config['password'])
        return self.client[self.config['database']]

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """close the  connection. """
        self.client.close()

def to_list(iterator):
    """
    A helper function to turn iterator returned by query into a list
    :param iterator: iterator returned by the mongoDB.
    :return: list of elements returned by the iterator.
    """
    items = []
    for item in iterator:
        items.append(item)
    return items

