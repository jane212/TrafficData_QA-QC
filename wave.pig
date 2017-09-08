data = LOAD 'Tingting/SensorData/wave_raw.txt' Using PigStorage(',') AS (name:chararray, date:chararray, time:chararray, drop:chararray, status:chararray);

data2 = FOREACH data GENERATE name, date, SUBSTRING(time, 0, 2) as hour, (status=='operational'?1:0) as operational, (status=='off'?1:0) as off, (status=='failed'?1:0) as failed;

data3 = GROUP data2 BY (name, date, hour);

data4 = FOREACH data3 GENERATE group.name as name, group.date as date, group.hour as hour, SUM(data2.operational) AS op, SUM(data2.off) AS off, SUM(data2.failed) AS failed;

data5 = FOREACH data4 GENERATE name, date, hour, op, off, failed, op+off+failed as total;

data6 = GROUP data5 BY (name, date);

data7 = FOREACH data6 GENERATE group.name as name, group.date as date, SUM(data5.total) AS dtotal;

data8 = JOIN data5 BY (name, date) LEFT, data7 BY (name, date);

final = FOREACH data8 GENERATE data5::name, data5::date, data5::hour, data5::op, data5::off, data5::failed, data5::total, data7::dtotal;


STORE final INTO 'Tingting/SensorData/wave' Using PigStorage(',');