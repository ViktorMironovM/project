create table deaise.mivi_stg_clients (
    client_id         varchar(10),
    last_name         varchar(20),
    first_name        varchar(20),
    patronymic        varchar(20),
    date_of_birth     date,
    passport_num      char(11),
    passport_valid_to date,
    phone             char(16),
    create_dt         timestamp(0),
    update_dt         timestamp(0)
);

create table deaise.mivi_stg_del_clients (
    last_name     varchar(20),
    first_name    varchar(20),
    patronymic    varchar(20),
    date_of_birth date
);

create table deaise.mivi_stg_accounts (
    account_num char(20),
    valid_to    date,
    client      varchar(10),
    create_dt   timestamp(0),
    update_dt   timestamp(0)
);

create table deaise.mivi_stg_del_accounts (
    account_num char(20),
    client      varchar(10)
);

create table deaise.mivi_stg_cards (
    card_num    char(19),
    account_num char(20),
    create_dt   timestamp(0),
    update_dt   timestamp(0)
);

create table deaise.mivi_stg_del_cards (
    card_num    char(19),
    account_num char(20)
);

create table deaise.mivi_stg_blacklist (
    passport_num char(11),
    entry_dt     date
);

create table deaise.mivi_stg_terminals (
    terminal_id      varchar(10),
    terminal_type    varchar(10),
    terminal_city    varchar(30),
    terminal_address varchar(200),
    entry_dt         date
);

create table deaise.mivi_stg_transactions (
    trans_id    varchar(15),
    trans_date  timestamp(0),
    card_num    char(19),
    oper_type   varchar(15),
    amt         decimal(10, 2),
    oper_result varchar(15),
    terminal    varchar(10)
);


create table deaise.mivi_meta (
    schema_name   varchar(30),
    table_name    varchar(30),
    max_update_dt timestamp(0)
);

insert into deaise.mivi_meta(schema_name, table_name, max_update_dt)
values
    ('deaise', 'stg_clients', to_timestamp('1890-01-01', 'YYYY-MM-DD')),
    ('deaise', 'stg_accounts', to_timestamp('1890-01-01', 'YYYY-MM-DD')),
    ('deaise', 'stg_cards', to_timestamp('1890-01-01', 'YYYY-MM-DD')),
    ('deaise', 'stg_blacklist', to_date('1890-01-01', 'YYYY-MM-DD')),
    ('deaise', 'dwh_fact_transactions', to_timestamp('1890-01-01', 'YYYY-MM-DD'))
;


create table deaise.mivi_dwh_dim_clients (
    client_id         varchar(10),
    last_name         varchar(20),
    first_name        varchar(20),
    patronymic        varchar(20),
    date_of_birth     date,
    passport_num      char(11),
    passport_valid_to date,
    phone             char(16),
    create_dt         timestamp(0),
    update_dt         timestamp(0)
);

create table deaise.mivi_dwh_dim_accounts (
    account_num char(20),
    valid_to    date,
    client      varchar(10),
    create_dt   timestamp(0),
    update_dt   timestamp(0)
);

create table deaise.mivi_dwh_dim_cards (
    card_num    char(19),
    account_num char(20),
    create_dt   timestamp(0),
    update_dt   timestamp(0)
);

create table deaise.mivi_dwh_dim_terminals (
    terminal_id      varchar(10),
    terminal_type    varchar(10),
    terminal_city    varchar(30),
    terminal_address varchar(200),
    create_dt        date,
    update_dt        timestamp(0)
);

create table deaise.mivi_dwh_fact_passport_blacklist (
    passport_num char(11),
    entry_dt     date
);

create table deaise.mivi_dwh_fact_transactions (
    trans_id    varchar(15),
    trans_date  timestamp(0),
    card_num    char(19),
    oper_type   varchar(15),
    amt         decimal(10, 2),
    oper_result varchar(15),
    terminal    varchar(10)
);


create table deaise.mivi_rep_fraud (
    event_dt   timestamp(0),
    passport   char(11),
    fio        varchar(62),
    phone      char(16),
    event_type char(1),
    report_dt  date
);