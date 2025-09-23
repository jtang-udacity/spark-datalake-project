CREATE EXTERNAL TABLE `steptrainer_landing`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://jtang97-udacity-project-bucket/step_trainer/landing/'
TBLPROPERTIES (
  'CreatedByJob'='Accelerometer Landing', 
  'CreatedByJobRun'='jr_bc39be2ba0d5937bd6b96f605cd49641dadbe321a69f3962097027ff607b34d6', 
  'classification'='json')