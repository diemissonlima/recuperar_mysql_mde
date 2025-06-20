import pymysql

def get_connection_origin_db():
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='1234',
    )

    return conn


def get_connection_destiny_db():
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='1234',
        database='smc_manifesto_temp'
    )

    return conn
