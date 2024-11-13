Create 5 ev folders for external table due to data format.

1. California data
California EV Dataset Issue
When loading the California EV dataset in Beeline, the Vehicle ID field caused issues due to unintentional line breaks, leading to row splitting and NULL values. 
This Python script removes line breaks within Vehicle ID to ensure each record stays on a single line for correct parsing in Hive.

#csv file cleasning with python.
import csv
import re

# Define file paths
input_file = r'F:\CSULA\CIS 5200\Project\EV\ca_ev_registrations_public.csv'
output_file = r'F:\CSULA\CIS 5200\Project\EV\cleaned_ca_ev_registrations_public.csv'

# Open input and output files
with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    # Initialize CSV writer
    writer = csv.writer(outfile)
    
    # Buffer to accumulate lines for each record
    buffer = []
    
    for line in infile:
        # If line starts with "CA-", it is a new record, so process the buffer
        if re.match(r'^"CA-\d{3}-\d{5}', line):
            if buffer:
                # Write the previous buffered line as a single record
                writer.writerow(csv.reader([''.join(buffer)]).__next__())
            # Start new buffer
            buffer = [line.strip()]
        else:
            # Continuation of the previous line; add to buffer
            buffer.append(line.strip())
    
    # Write the last buffered record
    if buffer:
        writer.writerow(csv.reader([''.join(buffer)]).__next__())

print(f"Cleaned file saved to {output_file}")

beeline

use jlee464;

DROP TABLE IF EXISTS ev1;

CREATE EXTERNAL TABLE IF NOT EXISTS ev1 (
    vehicle_id STRING,
    county_geoid STRING,
    registration_valid_date STRING,
    dmv_id INT,
    dmv_snapshot STRING,
    registration_expiration_date STRING,
    state_abbreviation STRING,
    geography STRING,
    vehicle_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/jlee464/ev1'
TBLPROPERTIES ("skip.header.line.count"="1");

2. fl External Table

CREATE EXTERNAL TABLE IF NOT EXISTS fl (
   	dmv_id INT,
    dmv_snapshot STRING,
    county string,
    vehicle_name STRING,
    vin_model_year STRING,
    registration_valid_date STRING,
    registration_expiration_date STRING,
    technology string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION '/user/jlee464/fl'
TBLPROPERTIES ("skip.header.line.count"="1");

3. wa External Table

Drop table if exists wa;

CREATE EXTERNAL TABLE IF NOT EXISTS wa (
    dmv_id INT,
    dmv_snapshot STRING,
    zipcode STRING,
    state STRING,
    registration_valid_date STRING,
    dmv_is_complete STRING,
    registration_expiration_date STRING,
    vin_prefix STRING,
    vin_model_year STRING,
    vehicle_name STRING,
    technology STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION '/user/jlee464/wa'
TBLPROPERTIES ("skip.header.line.count"="1");


4. wi EXTERNAL TABLE

drop table if exitsts wi;
CREATE EXTERNAL TABLE IF NOT EXISTS wi (
    dmv_id INT,
    dmv_snapshot STRING,
    zipcode STRING,
    vin_prefix STRING,
    vin_model_year STRING,
    registration_valid_date STRING,
    registration_expiration_date STRING,
    make STRING,
    model STRING,
    model_year STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION '/user/jlee464/wi'
TBLPROPERTIES ("skip.header.line.count"="1");


5. All other State External Table

CREATE EXTERNAL TABLE IF NOT EXISTS ev3 (
    state STRING,
    county STRING,
    registration_date STRING,
    vehicle_make STRING,
    vehicle_model STRING,
    vehicle_model_year INT,
    drivetrain_type STRING,
    vehicle_gvwr_class STRING,
    vehicle_gvwr_category STRING,
    vehicle_count INT,
    dmv_snapshot_id INT,
    dmv_snapshot STRING,
    latest_dmv_snapshot_flag BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION '/user/jlee464/ev3'
TBLPROPERTIES ("skip.header.line.count"="1");

create ev2 folder in your local computer
move file in to ev2 folder with wi_ev_registrations_public, wa_ev_registrations_public and fl_ev_registrations_public.
create ev3 folder in your local computer
move file in to ev3 folder with all register files except ca, wi, wa, fl data files.
Create a zip file with wi_ev_registrations_public, wa_ev_registrations_public and fl_ev_registrations_public.
Chagne Zip file name as ev2
Create a zip file with all other state csv file as ev3

scp "F:\CSULA\CIS 5200\Project\EV\cleaned_ca_ev_registrations_public.csv" jlee464@129.146.230.230:/home/jlee464/
scp "F:\CSULA\CIS 5200\Project\EV\ev2\fl_ev_registrations_public.csv" jlee464@129.146.230.230:/home/jlee464/
scp "F:\CSULA\CIS 5200\Project\EV\ev2\wa_ev_registrations_public.csv" jlee464@129.146.230.230:/home/jlee464/
scp "F:\CSULA\CIS 5200\Project\EV\ev2\wi_ev_registrations_public.csv" jlee464@129.146.230.230:/home/jlee464/
scp "F:\CSULA\CIS 5200\Project\EV\ev3.zip" jlee464@129.146.230.230:/home/jlee464/

mkdir ev1
mv cleaned_ca_ev_registrations_public.csv/
unzip ev3.zip

hdfs dfs -mkdir ev1
hdfs dfs -mkdir fl
hdfs dfs -mkdir wa
hdfs dfs -mkdir wi
hdfs dfs -mkdir ev3

hdfs dfs -put ev1/*.* ev1/
hdfs dfs -ls ev1/
hdfs dfs -put fl_ev_registrations_public.csv fl/
hdfs dfs -ls fl/
hdfs dfs -put wa_ev_registrations_public.csv wa/
hdfs dfs -ls wa/
hdfs dfs -put wi_ev_registrations_public.csv wi/
hdfs dfs -ls wi/
hdfs dfs -put ev3/*.* ev3/ 
hdfs dfs -ls ev3/

*/


--create ev1_sum table
DROP table if exists ev1_sum;

CREATE TABLE IF NOT EXISTS ev1_sum AS
SELECT 
    r_year,
    state,
    make,
    model,
    COUNT(*) AS vehicle_count
FROM (
    SELECT 
        SUBSTRING(registration_valid_date, 1, 4) AS r_year,
        state_abbreviation AS state,
        REGEXP_EXTRACT(vehicle_name, '^([^ ]+)', 1) AS make,
        REGEXP_EXTRACT(vehicle_name, '^[^ ]+ (.+)$', 1) AS model
    FROM ev1
) AS extracted_data
GROUP BY 
    r_year,
    state,
    make,
    model
ORDER BY 
    r_year, state, make, model;

select * from ev1_sum limit 5;

--test external tables;
select * from ev1 limit 5;
select * from fl limit 5;
select * from wa limit 5;
select * from wi limit 5;
select * from ev3 limit 5;

--create tabel wi_sum
DROP table	if exists wi_sum;

CREATE TABLE IF NOT EXISTS wi_sum AS
SELECT
    CASE 
        WHEN registration_valid_date IS NOT NULL AND registration_valid_date != ''
        THEN SUBSTRING(registration_valid_date, -4)
        ELSE NULL
    END AS r_year,
    'WI' AS state,
    make,
    model,
    COUNT(dmv_id) AS vehicle_count
FROM wi
GROUP BY
    CASE 
        WHEN registration_valid_date IS NOT NULL AND registration_valid_date != ''
        THEN SUBSTRING(registration_valid_date, -4)
        ELSE NULL
    END,
    make,
    model;

select * from wi_sum limit 5;

--create fl_sum table
DROP table	if exists fl_sum;

CREATE TABLE IF NOT EXISTS fl_sum AS
SELECT 
    CASE 
        WHEN registration_valid_date IS NOT NULL AND registration_valid_date != ''
        THEN SUBSTRING(registration_valid_date, -4)
        ELSE NULL
    END AS r_year,  -- Extracts the year from MM/DD/YYYY format
    'FL' AS state,  -- Sets state as FL
    REGEXP_EXTRACT(vehicle_name, '^([^ ]+)', 1) AS make,  -- Extracts make from Vehicle Name
    REGEXP_EXTRACT(vehicle_name, '^[^ ]+ (.+)$', 1) AS model,  -- Extracts model from Vehicle Name
    COUNT(*) AS vehicle_count
FROM fl
GROUP BY 
    CASE 
        WHEN registration_valid_date IS NOT NULL AND registration_valid_date != ''
        THEN SUBSTRING(registration_valid_date, -4)
        ELSE NULL
    END,
    REGEXP_EXTRACT(vehicle_name, '^([^ ]+)', 1),
    REGEXP_EXTRACT(vehicle_name, '^[^ ]+ (.+)$', 1)
ORDER BY 
    r_year, state, make, model;

select * from fl_sum limit 5;

--create wa_sum table
DROP TABLE IF EXISTS wa_sum;

CREATE TABLE IF NOT EXISTS wa_sum AS
SELECT 
    CASE 
        WHEN registration_valid_date IS NOT NULL AND registration_valid_date != ''
        THEN SUBSTRING(registration_valid_date, -4)
        ELSE NULL
    END AS r_year,  -- Extracts the year from MM/DD/YYYY format
    state,
    REGEXP_EXTRACT(vehicle_name, '^([^ ]+)', 1) AS make,  -- Extracts make from Vehicle Name
    REGEXP_EXTRACT(vehicle_name, '^[^ ]+ (.+)$', 1) AS model,  -- Extracts model from Vehicle Name
    COUNT(*) AS vehicle_count
FROM wa
GROUP BY 
    CASE 
        WHEN registration_valid_date IS NOT NULL AND registration_valid_date != ''
        THEN SUBSTRING(registration_valid_date, -4)
        ELSE NULL
    END,
    state,
    REGEXP_EXTRACT(vehicle_name, '^([^ ]+)', 1),
    REGEXP_EXTRACT(vehicle_name, '^[^ ]+ (.+)$', 1)
ORDER BY 
    r_year, state, make, model;


select * from wa_sum limit 5;

-- ev3_sum table
DROP TABLE IF EXISTS ev3_sum;

CREATE TABLE IF NOT EXISTS ev3_sum AS
SELECT 
    CASE 
        WHEN registration_date IS NOT NULL AND registration_date != ''
        THEN SUBSTRING(registration_date, -4)
        ELSE NULL
    END AS r_year,  -- Extracts the year from MM/DD/YYYY format
    state,
    vehicle_make AS make,
    vehicle_model AS model,
    SUM(vehicle_count) AS vehicle_count
FROM ev3
GROUP BY 
    CASE 
        WHEN registration_date IS NOT NULL AND registration_date != ''
        THEN SUBSTRING(registration_date, -4)
        ELSE NULL
    END,
    state,
    vehicle_make,
    vehicle_model
ORDER BY 
    r_year, state, make, model;

SELECT * FROM ev3_sum limit 5;

-- union tables;
DROP TABLE IF EXISTS all_vehicles_sum;

CREATE TABLE all_vehicles_sum AS
SELECT * FROM ev1_sum
UNION ALL
SELECT * FROM wa_sum
UNION ALL
SELECT * FROM wi_sum
UNION ALL
SELECT * FROM fl_sum
UNION ALL
SELECT * FROM ev3_sum;

SELECT * FROM all_vehicles_sum LIMIT 5;

DROP TABLE IF EXISTS all_veh;

CREATE TABLE all_veh AS
SELECT r_year, state, UPPER(make), UPPER(model), vehicle_count
FROM all_vehicles_sum;

--export
INSERT OVERWRITE DIRECTORY '/user/jlee464/all_veh'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','  -- Use comma as the delimiter
SELECT * FROM all_veh;



hdfs dfs -ls /user/jlee464/all_veh

hdfs dfs -get /user/jlee464/all_veh/*

ls

scp jlee464@129.146.230.230:/home/jlee464/000000_0 "F:\CSULA\CIS 5200\Project/EV/all_veh.csv"




