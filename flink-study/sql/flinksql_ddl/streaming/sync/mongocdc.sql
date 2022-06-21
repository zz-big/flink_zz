-- 连接mongo source
CREATE TABLE source_mongo_flinkcdc_test (
  _id STRING,
  gameid INT,
  title STRING,
  description STRING,
  `by` STRING,
  url STRING,
  tags ARRAY<STRING>,
  likes INT,
  plan ROW<t1 DECIMAL(10,2),t2 INT,t3 STRING>,
  PRIMARY KEY(_id) NOT ENFORCED
) WITH (
  'connector' = 'mongodb-cdc',
  'hosts' = '10.20.0.2:27017',
  'username' = 'root',
  'password' = '12345678',
  'database' = 'admin',
  'collection' = 'game'
);

CREATE TABLE sink_print_flinkcdc_test (
  _id STRING,
  gameid INT,
  title STRING,
  description STRING,
  `by` STRING,
  url STRING,
  tags ARRAY<STRING>,
  likes INT,
  plan ROW<t1 DECIMAL(10,2),t2 INT,t3 STRING>
) WITH (
  'connector' = 'print'
);

insert into sink_print_flinkcdc_test select * from source_mongo_flinkcdc_test;




