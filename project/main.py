#!/usr/bin/python3

import psycopg2
from py_scripts import config
import pandas as pd
import os
from datetime import datetime as dt

# 1. Подключение
conn_dwh = psycopg2.connect(
    host=config.HOST,
    port=config.PORT,
    database=config.DWH_DB,
    user=config.DWH_USER,
    password=config.DWH_PASSWORD
)
conn_src = psycopg2.connect(
    host=config.HOST,
    port=config.PORT,
    database=config.SRC_DB,
    user=config.SRC_USER,
    password=config.SRC_PASSWORD
)

conn_dwh.autocommit = False
conn_src.autocommit = False

cursor_dwh = conn_dwh.cursor()
cursor_src = conn_src.cursor()

# 2. Очистка stage слоя
#cursor_dwh.execute('DELETE FROM deaise.mivi_stg_clients')
#cursor_dwh.execute('DELETE FROM deaise.mivi_stg_del_clients')
#cursor_dwh.execute('DELETE FROM deaise.mivi_stg_accounts')
#cursor_dwh.execute('DELETE FROM deaise.mivi_stg_del_accounts')
#cursor_dwh.execute('DELETE FROM deaise.mivi_stg_cards')
#cursor_dwh.execute('DELETE FROM deaise.mivi_stg_del_cards')
#cursor_dwh.execute('DELETE FROM deaise.mivi_stg_terminals')
#cursor_dwh.execute('DELETE FROM deaise.mivi_stg_transactions')
#cursor_dwh.execute('DELETE FROM deaise.mivi_stg_blacklist')

# Инкрементальная загрузка SCD1

# 3. Загрузка данных в stage слой
# 3.1.1 Полный срез из info.clients в deaise.mivi_stg_del_clients
cursor_src.execute("""SELECT 
                        last_name,
                        first_name,
                        patronymic,
                        to_char(date_of_birth, 'YYYYMMDD') date_of_birth
                    FROM info.clients""")

df = pd.DataFrame(cursor_src.fetchall())

cursor_dwh.executemany("""INSERT INTO deaise.mivi_stg_del_clients(
                                    last_name,
                                    first_name,
                                    patronymic,
                                    date_of_birth) 
                            VALUES(
                                cast(%s as varchar(20)),
                                cast(%s as varchar(20)),
                                cast(%s as varchar(20)),
                                to_date(%s, 'YYYYMMDD')
                            )""", df.values.tolist())

# 3.1.2 Срез по дате из info.clients в deaise.mivi_stg_clients
cursor_dwh.execute("""  SELECT to_char(max_update_dt , 'YYYYMMDDHH24MISSMS') max_update_dt
                        FROM deaise.mivi_meta 
                        WHERE schema_name = 'deaise' AND table_name = 'stg_clients'""")
last_download_clients = cursor_dwh.fetchall()[0][0]
cursor_src.execute(f"SELECT "
                       f"client_id,"
                       f"last_name,"
                       f"first_name,"
                       f"patronymic,"
                       f"to_char(date_of_birth, 'YYYYMMDD') date_of_birth,"
                       f"passport_num,"
                       f"coalesce(to_char(passport_valid_to, 'YYYYMMDD'), null) passport_valid_to,"
                       f"phone,"
                       f"to_char(create_dt, 'YYYYMMDDHH24MISSMS') create_dt,"
                       f"coalesce(to_char(update_dt, 'YYYYMMDDHH24MISSMS'), null) update_dt "
                   f"FROM info.clients "
                   f"WHERE create_dt > to_timestamp('{last_download_clients}', 'YYYYMMDDHH24MISSMS') "
                   f"OR update_dt > to_timestamp('{last_download_clients}', 'YYYYMMDDHH24MISSMS')")

df = pd.DataFrame(cursor_src.fetchall())

cursor_dwh.executemany("""INSERT INTO deaise.mivi_stg_clients(
                                client_id,
                                last_name,
                                first_name,
                                patronymic,
                                date_of_birth,
                                passport_num,
                                passport_valid_to,
                                phone,
                                create_dt,
                                update_dt) 
                            VALUES(
                                cast(%s as varchar(10)),
                                cast(%s as varchar(20)),
                                cast(%s as varchar(20)),
                                cast(%s as varchar(20)),
                                to_date(%s, 'YYYYMMDD'),
                                cast(trim(%s) as char(11)),
                                coalesce(to_date(%s, 'YYYYMMDD'), null),
                                cast(trim(%s) as char(16)),
                                to_timestamp(%s, 'YYYYMMDDHH24MISSMS'),
                                coalesce(to_timestamp(%s, 'YYYYMMDDHH24MISSMS'), null)
                            )""", df.values.tolist())

# 3.2.1 Полный срез из info.accounts
cursor_src.execute("""SELECT 
                        account,
                        client
                    FROM info.accounts""")

df = pd.DataFrame(cursor_src.fetchall())

cursor_dwh.executemany("""INSERT INTO deaise.mivi_stg_del_accounts(
                                    account_num,
                                    client) 
                            VALUES(
                                cast(trim(%s) as char(20)),
                                cast(%s as varchar(10))
                            )""", df.values.tolist())

# 3.2.2 Срез по дате
cursor_dwh.execute("""  SELECT to_char(max_update_dt , 'YYYYMMDDHH24MISSMS') max_update_dt
                        FROM deaise.mivi_meta 
                        WHERE schema_name = 'deaise' AND table_name = 'stg_accounts'""")
last_download_accounts = cursor_dwh.fetchall()[0][0]
cursor_src.execute(f"SELECT "
                       f"account,"
                       f"to_char(valid_to, 'YYYYMMDD') valid_to,"
                       f"client,"
                       f"to_char(create_dt, 'YYYYMMDDHH24MISSMS') create_dt,"
                       f"coalesce(to_char(update_dt, 'YYYYMMDDHH24MISSMS'), null) update_dt "
                   f"FROM info.accounts "
                   f"WHERE create_dt > to_timestamp('{last_download_accounts}', 'YYYYMMDDHH24MISSMS') "
                   f"OR update_dt > to_timestamp('{last_download_accounts}', 'YYYYMMDDHH24MISSMS')")

df = pd.DataFrame(cursor_src.fetchall())

cursor_dwh.executemany("""INSERT INTO deaise.mivi_stg_accounts(
                                account_num,
                                valid_to,
                                client,
                                create_dt,
                                update_dt)
                        VALUES(
                            cast(trim(%s) as char(20)),
                            to_date(%s, 'YYYYMMDD'),
                            cast(%s as varchar(10)),
                            to_timestamp(%s, 'YYYYMMDDHH24MISSMS'),
                            coalesce(to_timestamp(%s, 'YYYYMMDDHH24MISSMS'), null)
                        )""", df.values.tolist())

# 3.3.1 Срез из info.cards в deaise.mivi_stg_del_cards
cursor_src.execute("""SELECT 
                        card_num,
                        account
                    FROM info.cards""")

df = pd.DataFrame(cursor_src.fetchall())

cursor_dwh.executemany("""INSERT INTO deaise.mivi_stg_del_cards(
                                    card_num,
                                    account_num) 
                            VALUES(
                                cast(trim(%s) as char(19)),
                                cast(trim(%s) as char(20))
                            )""", df.values.tolist())

# 3.3.2 Срез по дате из info.cards в deaise.mivi_stg_cards
cursor_dwh.execute("""  SELECT to_char(max_update_dt , 'YYYYMMDDHH24MISSMS') max_update_dt
                        FROM deaise.mivi_meta 
                        WHERE schema_name = 'deaise' AND table_name = 'stg_cards'""")
last_download_cards = cursor_dwh.fetchall()[0][0]
cursor_src.execute(f"SELECT "
                       f"card_num,"
                       f"account,"
                       f"to_char(create_dt, 'YYYYMMDDHH24MISSMS') create_dt,"
                       f"coalesce(to_char(update_dt, 'YYYYMMDDHH24MISSMS'), null) update_dt "
                   f"FROM info.cards "
                   f"WHERE create_dt > to_timestamp('{last_download_cards}', 'YYYYMMDDHH24MISSMS') "
                   f"OR update_dt > to_timestamp('{last_download_cards}', 'YYYYMMDDHH24MISSMS')")

df = pd.DataFrame(cursor_src.fetchall())

cursor_dwh.executemany("""INSERT INTO deaise.mivi_stg_cards (
                                card_num,
                                account_num,
                                create_dt,
                                update_dt) 
                            VALUES( 
                                cast(trim(%s) as char(19)),
                                cast(trim(%s) as char(20)),
                                to_timestamp(%s, 'YYYYMMDDHH24MISSMS'),
                                coalesce(to_timestamp(%s, 'YYYYMMDDHH24MISSMS'), null)
                            )""", df.values.tolist())

# 3.4 Загрузка из файлов в:deaise.mivi_stg_blacklist, deaise.mivi_stg_terminals, deaise.mivi_stg_transactions
create_report, change_terminals = False, False
worked_files = []
for file in os.listdir(config.FILE_PATH):

    # 3.4.1 Загрузка из файлов в deaise.mivi_stg_blacklist
    if 'passport_blacklist' in file:
        df = pd.read_excel(config.FILE_PATH + file, sheet_name='blacklist', header=0, index_col=None)
        df = df[['passport', 'date']]
        cursor_dwh.executemany("""INSERT INTO deaise.mivi_stg_blacklist (
                                        passport_num,
                                        entry_dt)
                                    VALUES(
                                        cast(trim(%s) as char(11)),
                                        cast(%s as date)
                                    )""", df.values.tolist())
        worked_files.append(file)

    # 3.4.2 Загрузка из файлов в deaise.mivi_stg_terminals
    elif 'terminals' in file:
        df = pd.read_excel(config.FILE_PATH + file, sheet_name='terminals', header=0, index_col=None)
        cursor_dwh.executemany(f"INSERT INTO deaise.mivi_stg_terminals ("
                                       f"terminal_id,"
                                       f"terminal_type,"
                                       f"terminal_city,"
                                       f"terminal_address,"
                                       f"entry_dt) "
                               f"VALUES("
                                   f"cast(%s as varchar(10)),"
                                   f"cast(%s as varchar(10)),"
                                   f"cast(%s as varchar(30)),"
                                   f"cast(%s as varchar(200)),"
                               f"to_date(substr('{file}', strpos('{file}','_') + 1, 8), 'DDMMYYYY'))"
                               , df.values.tolist())
        worked_files.append(file)
        change_terminals = True

    # 3.4.3 Загрузка из файлов в deaise.mivi_stg_transactions
    elif 'transactions' in file:
        df = pd.read_csv(config.FILE_PATH + file, sep=';', index_col=None)
        df = df[['transaction_id', 'transaction_date', 'card_num', 'oper_type', 'amount', 'oper_result', 'terminal']]
        cursor_dwh.executemany("""INSERT INTO deaise.mivi_stg_transactions (
                                        trans_id,
                                        trans_date,
                                        card_num,
                                        oper_type,
                                        amt,
                                        oper_result,
                                        terminal)
                                    VALUES( 
                                        cast(%s as varchar(15)),
                                        to_timestamp(%s, 'YYYY-MM-DD HH24:MI:SS'),
                                        cast(trim(%s) as char(19)),
                                        cast(%s as varchar(15)),
                                        cast(replace(%s,',','.') as decimal(10,2)),
                                        cast(%s as varchar(15)),
                                        cast(%s as varchar(10))
                                    )""", df.values.tolist())
        worked_files.append(file)
        create_report = True

# 4. Загрузка данных в Детальный слой
# 4.1.1 Insert into из deaise.mivi_stg_clients в deaise.mivi_dwh_dim_clients
cursor_dwh.execute("""INSERT INTO deaise.mivi_dwh_dim_clients (
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            create_dt,
                            update_dt)
                        SELECT
                            stg.client_id,
                            stg.last_name,
                            stg.first_name,
                            stg.patronymic,
                            stg.date_of_birth,
                            stg.passport_num,
                            stg.passport_valid_to,
                            stg.phone,
                            stg.create_dt,
                            stg.update_dt
                        FROM deaise.mivi_stg_clients stg
                        LEFT JOIN deaise.mivi_dwh_dim_clients tgt
                        ON 1=1
                            AND stg.last_name = tgt.last_name
                            AND stg.first_name = tgt.first_name
                            AND stg.patronymic = tgt.patronymic
                            AND stg.date_of_birth = tgt.date_of_birth
                        WHERE tgt.last_name is null""")

# 4.1.2 Update из deaise.mivi_stg_clients в deaise.mivi_dwh_dim_clients')
cursor_dwh.execute("""  UPDATE deaise.mivi_dwh_dim_clients tgt 
                        SET client_id = tmp.client_id,
                            last_name = tmp.last_name,
                            first_name = tmp.first_name,
                            patronymic = tmp.patronymic,
                            date_of_birth = tmp.date_of_birth,
                            passport_num = tmp.passport_num,
                            passport_valid_to = tmp.passport_valid_to,
                            phone = tmp.phone,
                            create_dt = tmp.create_dt,
                            update_dt = now()
                        FROM (SELECT
                                stg.client_id,
                                stg.last_name,
                                stg.first_name,
                                stg.patronymic,
                                stg.date_of_birth,
                                stg.passport_num,
                                stg.passport_valid_to,
                                stg.phone,
                                tgt.create_dt
                            FROM deaise.mivi_stg_clients stg
                            INNER JOIN deaise.mivi_dwh_dim_clients tgt
                            ON 1=1
                                AND stg.last_name = tgt.last_name
                                AND stg.first_name = tgt.first_name
                                AND stg.patronymic = tgt.patronymic
                                AND stg.date_of_birth = tgt.date_of_birth
                            WHERE ( 1=0
                                OR stg.passport_num <> tgt.passport_num 
                                OR (stg.passport_num is null and tgt.passport_num is not null) 
                                OR (stg.passport_num is not null and tgt.passport_num is null)
                                OR stg.passport_valid_to <> tgt.passport_valid_to 
                                OR (stg.passport_valid_to is null and tgt.passport_valid_to is not null) 
                                OR (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
                                OR stg.phone <> tgt.phone 
                                OR (stg.phone is null and tgt.phone is not null) 
                                OR (stg.phone is not null and tgt.phone is null))
                            ) tmp
                        WHERE tmp.client_id = tgt.client_id;""")

# 4.1.3 Delete в deaise.mivi_dwh_dim_clients
cursor_dwh.execute("""  DELETE FROM deaise.mivi_dwh_dim_clients 
                        WHERE client_id in (SELECT tgt.client_id
                                            FROM deaise.mivi_dwh_dim_clients tgt
                                            LEFT JOIN deaise.mivi_stg_del_clients stg
                                            ON 1=1
                                                AND stg.last_name = tgt.last_name
                                                AND stg.first_name = tgt.first_name
                                                AND stg.patronymic = tgt.patronymic
                                                AND stg.date_of_birth = tgt.date_of_birth
                                            WHERE stg.last_name is null)""")

# 4.2.1 Insert into из deaise.mivi_stg_accounts в deaise.mivi_dwh_dim_accounts
cursor_dwh.execute("""INSERT INTO deaise.mivi_dwh_dim_accounts (
                            account_num,
                            valid_to,
                            client,
                            create_dt,
                            update_dt)
                        SELECT
                            stg.account_num,
                            stg.valid_to,
                            stg.client,
                            stg.create_dt,
                            stg.update_dt
                        FROM deaise.mivi_stg_accounts stg
                        LEFT JOIN deaise.mivi_dwh_dim_accounts tgt
                        ON stg.account_num = tgt.account_num
                        AND stg.client = tgt.client
                        WHERE tgt.client is null""")

# 4.2.2 Update из deaise.mivi_stg_accounts в deaise.mivi_dwh_dim_accounts
cursor_dwh.execute("""  UPDATE deaise.mivi_dwh_dim_accounts tgt
                        SET account_num = tmp.account_num,
                            valid_to = tmp.valid_to,
                            client = tmp.client,
                            create_dt = tmp.create_dt,
                            update_dt = now()
                        FROM (SELECT
                                 tgt.account_num,
                                 stg.valid_to,
                                 tgt.client,
                                 tgt.create_dt
                              FROM deaise.mivi_stg_accounts stg
                              INNER JOIN deaise.mivi_dwh_dim_accounts tgt
                              ON stg.account_num = tgt.account_num
                              AND stg.client = tgt.client
                              WHERE stg.valid_to <> tgt.valid_to
                              OR (stg.valid_to is null and tgt.valid_to is not null)
                              OR (stg.valid_to is not null and tgt.valid_to is null)
                             ) tmp
                        WHERE tmp.account_num = tgt.account_num AND tmp.client = tgt.client""")

# 4.2.3 Delete в deaise.mivi_dwh_dim_accounts
cursor_dwh.execute("""  DELETE FROM deaise.mivi_dwh_dim_accounts
                        WHERE (client, account_num) in (SELECT tgt.client, tgt.account_num
                                                        FROM deaise.mivi_dwh_dim_accounts tgt
                                                        LEFT JOIN deaise.mivi_stg_del_accounts stg
                                                        ON stg.account_num = tgt.account_num
                                                        AND stg.client = tgt.client
                                                        WHERE stg.client is null)""")

# 4.3.1 Insert into из deaise.mivi_stg_cards в deaise.mivi_dwh_dim_cards
cursor_dwh.execute("""INSERT INTO deaise.mivi_dwh_dim_cards (
                            card_num,
                            account_num,
                            create_dt,
                            update_dt) 
                        SELECT
                            stg.card_num,
                            stg.account_num,
                            stg.create_dt,
                            stg.update_dt
                        FROM deaise.mivi_stg_cards stg
                        LEFT JOIN deaise.mivi_dwh_dim_cards tgt
                        ON stg.account_num = tgt.account_num
                            AND stg.card_num = tgt.card_num
                        WHERE tgt.card_num is null""")

# 4.3.2 Delete в deaise.mivi_dwh_dim_cards
cursor_dwh.execute("""  DELETE FROM deaise.mivi_dwh_dim_cards
                        WHERE (card_num, account_num) in  (SELECT tgt.card_num, tgt.account_num
                                                            FROM deaise.mivi_dwh_dim_cards tgt
                                                            LEFT JOIN deaise.mivi_stg_del_cards stg
                                                            ON stg.account_num = tgt.account_num
                                                            AND stg.card_num = tgt.card_num
                                                            WHERE stg.card_num is null)""")

if change_terminals:
    # 4.4.1 Insert into из deaise.mivi_stg_terminals в deaise.mivi_dwh_dim_terminals
    cursor_dwh.execute("""INSERT INTO deaise.mivi_dwh_dim_terminals (
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                create_dt,
                                update_dt)
                            SELECT
                                stg.terminal_id,
                                stg.terminal_type,
                                stg.terminal_city,
                                stg.terminal_address,
                                stg.entry_dt,
                                null
                            FROM deaise.mivi_stg_terminals stg
                            LEFT JOIN deaise.mivi_dwh_dim_terminals tgt
                            ON stg.terminal_id = tgt.terminal_id
                            WHERE tgt.terminal_id is null""")

    # 4.4.2 Update из deaise.mivi_stg_terminals в deaise.mivi_dwh_dim_terminals
    cursor_dwh.execute("""  UPDATE deaise.mivi_dwh_dim_terminals tgt
                            SET terminal_id = tmp.terminal_id,
                                terminal_type = tmp.terminal_type,
                                terminal_city = tmp.terminal_city,
                                terminal_address = tmp.terminal_address,
                                create_dt = tmp.create_dt,
                                update_dt = now()
                            FROM (SELECT
                                    tgt.terminal_id,
                                    stg.terminal_type,
                                    stg.terminal_city,
                                    stg.terminal_address,
                                    tgt.create_dt
                                FROM deaise.mivi_stg_terminals stg
                                INNER JOIN deaise.mivi_dwh_dim_terminals tgt
                                ON stg.terminal_id = tgt.terminal_id
                                WHERE stg.terminal_address <> tgt.terminal_address
                                OR (stg.terminal_address is null and tgt.terminal_address is not null)
                                OR (stg.terminal_address is not null and tgt.terminal_address is null)
                                OR stg.terminal_type <> tgt.terminal_type
                                OR (stg.terminal_type is null and tgt.terminal_type is not null)
                                OR (stg.terminal_type is not null and tgt.terminal_type is null)
                                OR stg.terminal_city <> tgt.terminal_city
                                OR (stg.terminal_city is null and tgt.terminal_city is not null)
                                OR (stg.terminal_city is not null and tgt.terminal_city is null)
                                ) tmp
                            WHERE tgt.terminal_id = tmp.terminal_id""")

    # 4.4.3 Delete в deaise.mivi_dwh_dim_terminals
    cursor_dwh.execute("""DELETE FROM deaise.mivi_dwh_dim_terminals
                            WHERE terminal_id IN (SELECT tgt.terminal_id
                                                FROM deaise.mivi_dwh_dim_terminals tgt
                                                LEFT JOIN deaise.mivi_stg_terminals stg
                                                ON stg.terminal_id = tgt.terminal_id
                                                WHERE stg.terminal_id is null)""")

# 4.5 Insert into из deaise.mivi_stg_blacklist в deaise.mivi_dwh_fact_passport_blacklist
cursor_dwh.execute("""INSERT INTO deaise.mivi_dwh_fact_passport_blacklist (
                            passport_num,
                            entry_dt)
                        SELECT
                            stg.passport_num,
                            MIN(stg.entry_dt) entry_dt
                        FROM deaise.mivi_stg_blacklist stg
                        LEFT JOIN deaise.mivi_dwh_fact_passport_blacklist tgt
                        ON stg.passport_num = tgt.passport_num
                        WHERE stg.entry_dt > (SELECT max_update_dt
                                               FROM deaise.mivi_meta 
                                               WHERE schema_name = 'deaise' 
                                               AND table_name = 'stg_blacklist')
                            AND tgt.passport_num is null
                        GROUP BY stg.passport_num""")

# 4.6 Insert into из deaise.mivi_stg_transactions в deaise.mivi_dwh_fact_transactions
cursor_dwh.execute("""INSERT INTO deaise.mivi_dwh_fact_transactions (
                            trans_id,
                            trans_date,
                            card_num,
                            oper_type,
                            amt,
                            oper_result,
                            terminal)
                        SELECT
                            stg.trans_id,
                            stg.trans_date,
                            stg.card_num,
                            stg.oper_type,
                            stg.amt,
                            stg.oper_result,
                            stg.terminal
                        FROM deaise.mivi_stg_transactions stg
                        LEFT JOIN deaise.mivi_dwh_fact_transactions tgt
                        ON stg.trans_id = tgt.trans_id
                            AND stg.trans_date = tgt.trans_date
                        WHERE stg.trans_date > (SELECT max_update_dt
                                               FROM deaise.mivi_meta 
                                               WHERE schema_name = 'deaise' 
                                               AND table_name = 'dwh_fact_transactions')
                            AND tgt.trans_id is null""")

# 4.7.1 Обновление Мета Обновление Мета для: clients, accounts, cards
meta_table = ('clients', 'accounts', 'cards')
for table in meta_table:
    cursor_dwh.execute(
        f"UPDATE deaise.mivi_meta "
        f"SET max_update_dt = coalesce("
                f"(SELECT "
                    f"CASE "
                        f"WHEN max(update_dt) is null THEN max(create_dt) "
                        f"WHEN max(update_dt) is not null and max(update_dt) < max(create_dt) THEN max(create_dt) "
                        f"WHEN max(update_dt) is not null and max(update_dt) >= max(create_dt) THEN max(update_dt) "
                    f"END as max_update_dt "
                f"FROM deaise.mivi_stg_{table}), max_update_dt) "
        f"WHERE schema_name = 'deaise' and table_name = 'stg_{table}'")

# 4.7.2 Обновление Мета для passport_blacklist
cursor_dwh.execute("""  UPDATE deaise.mivi_meta 
                        SET max_update_dt = coalesce((SELECT max(entry_dt) 
                                                      FROM deaise.mivi_stg_blacklist), max_update_dt)
                        WHERE schema_name = 'deaise' and table_name = 'stg_blacklist'""")

# 4.7.3 Обновление Мета для dwh_fact_transactions
cursor_dwh.execute("""  UPDATE deaise.mivi_meta 
                        SET max_update_dt = coalesce((SELECT max(trans_date) 
                                                      FROM deaise.mivi_dwh_fact_transactions), max_update_dt) 
                        WHERE schema_name = 'deaise' and table_name = 'dwh_fact_transactions'""")

# 5. Создание отчета
if create_report:
    # 5.1 Совершение операции при просроченном или заблокированном паспорте.
    cursor_dwh.execute("""INSERT INTO deaise.mivi_rep_fraud (
                                event_dt,
                                passport,
                                fio,
                                phone,
                                event_type,
                                report_dt)
                            SELECT
                                tmp.event_dt,
                                tmp.passport,
                                tmp.fio,
                                tmp.phone,
                                tmp.event_type,
                                cast((SELECT max_update_dt
                                      FROM deaise.mivi_meta
                                      WHERE schema_name = 'deaise'
                                      AND table_name = 'dwh_fact_transactions') as date) report_dt                    
                            FROM (SELECT 
                                    t.event_dt,
                                    cl.passport_num passport,
                                    cl.last_name||' '||cl.first_name||' '||cl.patronymic fio,
                                    cl.phone phone,
                                    '1' event_type
                                    FROM deaise.mivi_dwh_dim_clients cl
                                    LEFT JOIN (SELECT
                                                    min(tr.trans_date) event_dt, 
                                                    cl.client_id
                                              FROM deaise.mivi_dwh_fact_transactions tr
                                              LEFT JOIN deaise.mivi_dwh_dim_cards c
                                              ON tr.card_num = c.card_num
                                              LEFT JOIN deaise.mivi_dwh_dim_accounts an
                                              ON an.account_num = c.account_num
                                              LEFT JOIN deaise.mivi_dwh_dim_clients cl
                                              ON an.client = cl.client_id
                                              WHERE cl.passport_valid_to + interval'1 day' < tr.trans_date
                                                OR (cl.passport_num in (SELECT passport_num 
                                                                        FROM deaise.mivi_dwh_fact_passport_blacklist) 
                                                   AND tr.trans_date > (SELECT entry_dt 
                                                                        FROM deaise.mivi_dwh_fact_passport_blacklist 
                                                                        WHERE passport_num = cl.passport_num))
                                              GROUP BY cl.client_id
                                              ) t
                                    ON t.client_id = cl.client_id
                                    WHERE t.client_id is not null
                                    ) tmp
                            LEFT JOIN deaise.mivi_rep_fraud rf
                            ON tmp.event_dt = rf.event_dt
                               AND tmp.passport = rf.passport
                               AND tmp.event_type = rf.event_type
                            WHERE rf.event_dt is null""")

    # 5.2 Совершение операции при недействующем договоре.
    cursor_dwh.execute("""INSERT INTO deaise.mivi_rep_fraud (
                                event_dt,
                                passport,
                                fio,
                                phone,
                                event_type,
                                report_dt)
                            SELECT
                                tmp.event_dt,
                                tmp.passport,
                                tmp.fio,
                                tmp.phone,
                                tmp.event_type,
                                cast((SELECT max_update_dt
                                      FROM deaise.mivi_meta
                                      WHERE schema_name = 'deaise'
                                      AND table_name = 'dwh_fact_transactions') as date) report_dt                    
                            FROM (SELECT 
                                    t.event_dt,
                                    cl.passport_num passport,
                                    cl.last_name||' '||cl.first_name||' '||cl.patronymic fio,
                                    cl.phone phone,
                                    '2' event_type
                                  FROM deaise.mivi_dwh_dim_clients cl
                                  LEFT JOIN (SELECT 
                                                an.client, 
                                                min(tr.trans_date) event_dt
                                           FROM deaise.mivi_dwh_fact_transactions tr
                                           LEFT JOIN deaise.mivi_dwh_dim_cards c
                                           ON tr.card_num = c.card_num
                                           LEFT JOIN deaise.mivi_dwh_dim_accounts an
                                           ON an.account_num = c.account_num
                                           WHERE an.valid_to + interval'1 day' < tr.trans_date
                                           GROUP BY an.client
                                           ) t
                                  ON t.client = cl.client_id
                                  WHERE t.client is not null
                                  ) tmp
                            LEFT JOIN deaise.mivi_rep_fraud rf
                            ON tmp.event_dt = rf.event_dt
                            AND tmp.passport = rf.passport
                            AND tmp.event_type = rf.event_type
                            WHERE rf.event_dt is null""")

    # 5.3 Совершение операций в разных городах в течение одного часа.
    cursor_dwh.execute("""INSERT INTO deaise.mivi_rep_fraud (
                                event_dt,
                                passport,
                                fio,
                                phone,
                                event_type,
                                report_dt)
                            SELECT
                                tmp.event_dt,
                                tmp.passport,
                                tmp.fio,
                                tmp.phone,
                                tmp.event_type,
                                cast((SELECT max_update_dt
                                      FROM deaise.mivi_meta
                                      WHERE schema_name = 'deaise'
                                      AND table_name = 'dwh_fact_transactions') as date) report_dt                    
                            FROM (SELECT
                                    t.event_dt,
                                    cl.passport_num passport,
                                    cl.last_name||' '||cl.first_name||' '||cl.patronymic fio,
                                    cl.phone phone,
                                    '3' event_type
                                  FROM deaise.mivi_dwh_dim_clients cl
                                  LEFT JOIN deaise.mivi_dwh_dim_accounts an
                                  ON cl.client_id = an.client
                                  LEFT JOIN deaise.mivi_dwh_dim_cards ca
                                  ON an.account_num = ca.account_num
                                  LEFT JOIN (SELECT 
                                                min(trans_date) event_dt, 
                                                card_num
                                             FROM (SELECT
                                                        tr.card_num,
                                                        tr.trans_date,
                                                        lag(tr.trans_date) OVER (PARTITION BY tr.card_num ORDER BY tr.trans_date) lg_date,
                                                        te.terminal_city,
                                                        lag(te.terminal_city) OVER (PARTITION BY tr.card_num ORDER BY tr.trans_date) lg_city
                                                   FROM deaise.mivi_dwh_fact_transactions tr
                                                   LEFT JOIN deaise.mivi_dwh_dim_terminals te
                                                   ON tr.terminal = te.terminal_id
                                                   ) m
                                             WHERE terminal_city <> lg_city
                                                AND lg_date between (trans_date - interval '1 hour') and trans_date
                                             GROUP BY card_num
                                            ) t
                                  ON t.card_num = ca.card_num
                                  WHERE t.card_num is not null
                                  ) tmp
                            LEFT JOIN deaise.mivi_rep_fraud rf
                            ON tmp.event_dt = rf.event_dt
                               AND tmp.passport = rf.passport
                               AND tmp.event_type = rf.event_type
                            WHERE rf.event_dt is null""")

    # 5.4 Попытка подбора суммы.
    cursor_dwh.execute("""INSERT INTO deaise.mivi_rep_fraud (
                                event_dt,
                                passport,
                                fio,
                                phone,
                                event_type,
                                report_dt)
                            SELECT
                                tmp.event_dt,
                                tmp.passport,
                                tmp.fio,
                                tmp.phone,
                                tmp.event_type,
                                cast((SELECT max_update_dt
                                      FROM deaise.mivi_meta
                                      WHERE schema_name = 'deaise'
                                      AND table_name = 'dwh_fact_transactions') as date) report_dt                    
                            FROM (SELECT
                                    t.event_dt,
                                    cl.passport_num passport,
                                    cl.last_name||' '||cl.first_name||' '||cl.patronymic fio,
                                    cl.phone phone,
                                    '4' event_type
                                  FROM deaise.mivi_dwh_dim_clients cl
                                  LEFT JOIN deaise.mivi_dwh_dim_accounts an
                                  ON cl.client_id = an.client
                                  LEFT JOIN deaise.mivi_dwh_dim_cards ca
                                  ON an.account_num = ca.account_num
                                  LEFT JOIN (SELECT 
                                                event_dt, 
                                                card_num
                                            FROM (SELECT
                                                    trans_date event_dt,
                                                    lag(trans_date, 3) over (partition by card_num order by trans_date) lg_date,
                                                    card_num, 
                                                    amt current_amt,
                                                    lag(amt) over (partition by card_num order by trans_date) third_amt,
                                                    lag(amt, 2) over (partition by card_num order by trans_date) second_amt,
                                                    lag(amt, 3) over (partition by card_num order by trans_date) first_amt,
                                                    oper_result current_result, 
                                                    lag(oper_result) over (partition by card_num order by trans_date) third_result,
                                                    lag(oper_result, 2) over (partition by card_num order by trans_date) second_result,
                                                    lag(oper_result, 3) over (partition by card_num order by trans_date) first_result
                                                 FROM deaise.mivi_dwh_fact_transactions) m
                                            WHERE first_amt > second_amt and second_amt > third_amt and third_amt > current_amt
                                              AND first_result = 'REJECT' and second_result = 'REJECT' and third_result = 'REJECT' 
                                              AND current_result = 'SUCCESS' 
                                              AND event_dt between lg_date and lg_date + interval '20 minutes'
                                            ) t
                                  ON t.card_num = ca.card_num
                                  WHERE t.card_num is not null
                                  ) tmp
                            LEFT JOIN deaise.mivi_rep_fraud rf
                            ON tmp.event_dt = rf.event_dt
                               AND tmp.passport = rf.passport
                               AND tmp.event_type = rf.event_type
                            WHERE rf.event_dt is null""")

# 6.1 Коммит
conn_dwh.commit()

# 6.2 Закрытие соединения
cursor_dwh.close()
cursor_src.close()
conn_dwh.close()
conn_src.close()

# 6.3 Переименование и перенос использованных файлов
if not os.path.isdir(config.FILE_PATH + config.ARCHIVE):
    os.mkdir(config.FILE_PATH + config.ARCHIVE)

if len(worked_files) > 0:
    for file in worked_files:
        os.replace(config.FILE_PATH + file, config.FILE_PATH + config.ARCHIVE + file + config.BACKUP)
