import pymysql

def get_connection():
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='1234',
    )

    return conn
