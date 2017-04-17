import psycopg2

class ConnectDatabase:
    def __init__(self):
        self.conn = None
        self.cur = None
        try:
            self.conn = psycopg2.connect(
                database='postgres_trans',
                user='client3',
                password='client333',
                host='postgres-trans.c3yxphqgag3s.ap-southeast-1.rds.amazonaws.com',
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
