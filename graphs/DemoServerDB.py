import psycopg2

class ConnectDatabase:
    def __init__(self):
        self.conn = None
        self.cur = None
        try:
            self.conn = psycopg2.connect(
                database='postgres_db',
                user='postgres_user',
                password='admin',
                host='developer.tuple-mia.com',
                port='5432',
                sslmode='require'
            )
            self.cur = self.conn.cursor()
        except Exception as e:
            print e
            return None

    def isConnected(self):
        if self.conn:
            return True
        else:
            return False

    def conn(self):
        return self.conn

    def cur(self):
        return self.cur