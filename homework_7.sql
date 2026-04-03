--Проверка названия кластера
SELECT * FROM system.clusters;

--Создание базы данных в Click House
CREATE DATABASE std12_50 ON CLUSTER 'default_cluster';

--Проверка, что все создалось
SELECT name, engine FROM system.databases WHERE name = 'std12_50';

--Создание таблицы с внешним источником Greenplum
CREATE TABLE std12_50.ch_plan_fact_ext
(
    region        String,
    matdirec      String,
    distr_chan    String,
    plan_quantity Int32,
    fact_quantity Int32,
    percent       DECIMAL(18, 2),
    material      String
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'plan_fact_202107', 'std12_50', '3e6WXMQ6zl5uZA');

--Создание словарей с внешним источником

-- Словарь цен
CREATE DICTIONARY std12_50.ch_price_dict
(
    material   String,
    region     String,
    distr_chan String,
    price      Int32
)
PRIMARY KEY material, region, distr_chan
SOURCE(POSTGRESQL(
    host '192.168.214.203'
    port 5432
    user 'std12_50'
    password '3e6WXMQ6zl5uZA'
    db 'adb'
    table 'price'
    invalidate_query 'SELECT MAX(updated_at) FROM price'
))
LIFETIME(300)
LAYOUT(COMPLEX_KEY_HASHED());

-- Словарь каналов
CREATE DICTIONARY std12_50.ch_chanel_dict
(
    distr_chan String,
    txtsh      String
)
PRIMARY KEY distr_chan
SOURCE(POSTGRESQL(
    host '192.168.214.203'
    port 5432
    user 'std12_50'
    password '3e6WXMQ6zl5uZA'
    db 'adb'
    table 'chanel'
    invalidate_query 'SELECT MAX(updated_at) FROM chanel'
))
LIFETIME(300)
LAYOUT(HASHED());

-- Словарь продуктов
CREATE DICTIONARY std12_50.ch_product_dict
(
    material String,
    asgrp    Int32,
    brand    Int32,
    matcateg String,
    matdirec String,
    txt      String
)
PRIMARY KEY material
SOURCE(POSTGRESQL(
    host '192.168.214.203'
    port 5432
    user 'std12_50'
    password '3e6WXMQ6zl5uZA'
    db 'adb'
    table 'product'
    invalidate_query 'SELECT MAX(updated_at) FROM product'
))
LIFETIME(300)
LAYOUT(HASHED());

-- Словарь регионов
CREATE DICTIONARY std12_50.ch_region_dict
(
    region String,
    txt    String
)
PRIMARY KEY region, txt
SOURCE(POSTGRESQL(
    host '192.168.214.203'
    port 5432
    user 'std12_50'
    password '3e6WXMQ6zl5uZA'
    db 'adb'
    table 'region'
))
LIFETIME(300)
LAYOUT(COMPLEX_KEY_HASHED());

DROP DICTIONARY std12_50.ch_region_dict;

-- Реплицированная таблица на всех хостах
CREATE TABLE std12_50.ch_plan_fact ON CLUSTER 'default_cluster'
(
    region        String,
    matdirec      String,
    distr_chan    String,
    plan_quantity Int32,
    fact_quantity Int32,
    percent       Decimal(18,2),
    material      String
)
ENGINE = ReplicatedMergeTree('/click/ch_plan_fact_std12_50/{shard}', '{replica}')
    ORDER BY (region, matdirec, distr_chan)
    SETTINGS index_granularity = 8192;

-- Распределенная таблица
CREATE TABLE std12_50.ch_plan_fact_distr ON CLUSTER 'default_cluster'
AS std12_50.ch_plan_fact
ENGINE = Distributed('default_cluster', 'std12_50', 'ch_plan_fact', cityHash64(matdirec));

-- Вставка данных из интеграционной таблицы
INSERT INTO std12_50.ch_plan_fact_distr
SELECT * FROM std12_50.ch_plan_fact_ext;