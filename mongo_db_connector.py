import os
from urllib.parse import quote_plus
from pymongo import MongoClient, errors
import constants as constants
import logging


class MongoDBConnector:
    def __init__(self):
        self.client = None

        # maxPoolSize sets the maximum number of connections in the connection pool
        # To support extremely high numbers of concurrent MongoDB operations within one process, increase maxPoolSize
        self.maxPoolSize = os.environ.get("MAX_POOL_SIZE", 50)

        # The maximum number of milliseconds that a connection can remain idle in the pool
        # before being removed and closed. (1000 MS = 1 second)
        self.maxIdleTimeMS = os.environ.get("MAX_IDLE_TIME_MS", 2000)

        self.logger = logging.getLogger("report-generator")
        logging.basicConfig(level=logging.INFO)

    @staticmethod
    def get_connection_string():
        if constants.MONGO_DB_CLUSTER in os.environ and constants.MONGO_DB_USER in os.environ and \
                constants.MONGO_DB_NAME in os.environ and constants.MONGO_DB_PASSWORD in os.environ:
            cluster = os.environ[constants.MONGO_DB_CLUSTER]
            user = quote_plus(os.environ[constants.MONGO_DB_USER])
            name = os.environ[constants.MONGO_DB_NAME]
            password = quote_plus(os.environ[constants.MONGO_DB_PASSWORD])
            connection_string = constants.MONGO_PREFIX + user + ":" + password + "@" + cluster + constants.MONGO_SUFFIX
            return connection_string
        else:
            raise Exception("Not able to fetch or wrong db credentials from env variables")

    @staticmethod
    def get_db_name():
        if constants.MONGO_DB_NAME in os.environ:
            return os.environ[constants.MONGO_DB_NAME]
        else:
            return "lineaje_data"
            # raise Exception("Not able to fetch or wrong db credentials from env variables")

    def connect(self, use_pool=True):
        try:
            if self.client is None:
                # Use MongoClient with a connection pool if specified
                if use_pool:
                    self.client = MongoClient(self.get_connection_string(), maxPoolSize=self.maxPoolSize,
                                              maxIdleTimeMS=self.maxIdleTimeMS)
                else:
                    self.client = MongoClient(self.get_connection_string())
        except errors.ConnectionFailure:
            raise Exception("Failed to connect to MongoDB")

    def disconnect(self):
        if self.client:
            self.client.close()
            self.client = None

    def get_collection(self, collection_name, db_name=None, use_pool=True):
        try:
            db_name = db_name if db_name is not None else self.get_db_name()
            self.connect(use_pool)
            return self.client[db_name][collection_name]
        except Exception as e:
            self.logger.error(e)
