import MySQLdb


class DatabaseConfig():
    """Manages database configuration data and provides
       methods to expose that data to different systems (spark, mysqldb, ...)
    """

    def __init__(self, host=None, user=None, password=None):
        self._host = host
        self._user = user
        self._password = password

    def jdbc_url(self, db_name):
        return "jdbc:mysql://%s/%s" % (self._host, db_name)

    def jdbc_properties(self):
        return {'user': self._user, 'password': self._password}

    def mysqldb_connection(self):
        return MySQLdb.connect(host=self._host,  # your host
                               user=self._user,       # username
                               passwd=self._password)   # name of the database

    def write_df(self, df, db_name, table_name, mode='error'):
        df.write.jdbc(
            self.jdbc_url(db_name),
            table_name,
            mode=mode,
            properties=self.jdbc_properties())
