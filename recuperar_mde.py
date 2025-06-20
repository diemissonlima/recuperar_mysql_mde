from time import sleep
import pymysql
import shutil
import os
import subprocess
from models import database
import scripts_sql


conn = database.get_connection()


def create_database():
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE IF NOT EXISTS smc_manifesto_temp")
            cursor.execute("USE smc_manifesto_temp")

            cursor.execute(scripts_sql.table_empresa)
            cursor.execute(scripts_sql.table_nfe_destinadas)
            cursor.execute(scripts_sql.table_tabela_pais)
            cursor.execute(scripts_sql.table_tabela_estado)
            cursor.execute(scripts_sql.table_tabela_ibge)

        insert_database()

    except pymysql.MySQLError as error:
        print(f'erro {error}')


def insert_database():
    try:
        with conn.cursor() as cursor:
            cursor.execute(scripts_sql.insert_table_pais)
            sleep(0.5)

            cursor.execute(scripts_sql.insert_table_estado)
            sleep(0.5)

            cursor.execute(scripts_sql.insert_table_ibge)
            conn.commit()

            sleep(0.5)
            alter_table()

    except pymysql.MySQLError as error:
        print(f'erro {error}')


def alter_table():
    try:
        with conn.cursor() as cursor:
            cursor.execute('ALTER TABLE empresa DISCARD TABLESPACE')
            cursor.execute('ALTER TABLE nfe_destinadas DISCARD TABLESPACE')

            copy_files()

            cursor.execute('ALTER TABLE empresa IMPORT TABLESPACE')
            cursor.execute('ALTER TABLE nfe_destinadas IMPORT TABLESPACE')

            conn.close()
            save_database()

    except pymysql.MySQLError as error:
        print(f'erro {error}')


def copy_files():
    lista_arquivos = os.listdir('tables')

    for arquivo in lista_arquivos:
        origem = f'tables/{arquivo}'
        destino_pasta = 'C:\ProgramData\MySQL\MySQL Server 8.0\Data\smc_manifesto_temp'
        destino = os.path.join(destino_pasta, arquivo)

        shutil.copy2(origem, destino)


def save_database():
    usuario = 'root'
    senha = '1234'
    banco = 'smc_manifesto_temp'

    mysql_bin_path = r"C:\Program Files\MySQL\MySQL Server 8.0\bin"

    env = os.environ.copy()
    env["PATH"] = mysql_bin_path + os.pathsep + env["PATH"]

    comando = f"mysqldump -u {usuario} -p{senha} --databases {banco}"

    with open(f'{banco}.sql', 'w') as output:
        subprocess.run(comando, shell=True, stdout=output, env=env)

    input("Backup concluido! pressione ENTER pra encerrar!!!")

print("-=" * 30)
print("Realizando BACKUP! Aguarde...".center(60))
print("-=" * 30)


create_database()
