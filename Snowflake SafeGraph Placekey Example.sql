/*
This worksheet walks through the process of accessing the SafeGraph data hosted in the 
Snowflake Data Marketplace (https://www.snowflake.com/datasets/safegraph-foot-traffic-patterns-starbucks/)
and joining it with some "internal" data using the Placekey External Function
*/

// setup
// note: you may have to change the name of the file to reflect the database name in your instance
use role accountadmin;
use demo_db;

// get SafeGraph patterns data for NY Starbucks locations
// note: you may have to change the name of the file to reflect the filename in your instance
select * 
from safegraph_aws_us_east_2_starbucks_patterns_sample.public.patterns
where region = 'NY'
limit 10;

// pull "internal" data from an s3 bucket

CREATE OR REPLACE TABLE starbucks_origin (
  brand VARCHAR(20),
  store_number VARCHAR(20),
  name VARCHAR(256),
  ownership_type VARCHAR(256),
  facility_id VARCHAR(128),
  products VARCHAR(128),
  service VARCHAR(128),
  stations VARCHAR(128),
  food_region VARCHAR(128),
  venue_type VARCHAR(128),
  phone_number VARCHAR(128),
  location VARCHAR(512),
  street_address VARCHAR(128),
  street_line1 VARCHAR(128),
  street_line2 VARCHAR(128),
  city VARCHAR(128),
  state VARCHAR(2),
  zip VARCHAR(20),
  country VARCHAR(2),
  coordinates VARCHAR(40),
  latitude VARCHAR(20),
  longitude VARCHAR(20),
  insert_date VARCHAR(30)
);

// copy the data from s3 into Snowflake
copy into starbucks_origin
  from @aws_s3_starbucks_origin_stage
  pattern = '.*.csv'
  on_error = continue
;

// inspect the "internal" data
select * 
from starbucks_origin
where state = 'NY';

delete from starbucks_origin
where state != 'NY';


// set up the Placekey External Function

// api integration object to communicate with lambda function
create or replace api integration placekey_api_integration
  api_provider = aws_api_gateway
  api_aws_role_arn = 'arn:aws:iam::886725170148:role/placekey-lambda-production'
  enabled = true
  api_allowed_prefixes = ('https://lbdl9njufi.execute-api.us-east-1.amazonaws.com/api/')
;

// create the external function
create or replace external function get_placekeys_variant(
  input variant
)
  returns variant
  api_integration = placekey_api_integration
  headers = ('api-key' = 'lxsFTUkHSyT8mcQ4g4hn2UuoJqqSQJrH')
  max_batch_rows = 1000
  as 'https://lbdl9njufi.execute-api.us-east-1.amazonaws.com/api/placekeys'
;

// query the Placekey API to get Placekeys to conduct the join
create or replace temporary table placekeyed_data as (
  select
    cast(api_result[0] as integer) as id,
    cast(api_result[1] as varchar) as placekey,
    cast(api_result[2] as varchar) as error
  from (
    select get_placekeys_variant(object_construct(joined.*)) as api_result
    from (
      select
        facility_id as primary_key,
        brand as location_name,
        street_address as street_address,
        city as city,
        state as region,
        zip as postal_code,
        latitude as latitude,
        longitude as longitude
      from starbucks_origin
    ) as joined
  ) as result
);

// visualize the merged datasets
select * 
from placekeyed_data d
join starbucks_origin s on d.id = s.facility_id
join safegraph_aws_us_east_2_starbucks_patterns_sample.public.patterns p on p.placekey = d.placekey
limit 10;

// select certain columns for inspection
select d.placekey, s.service, p.visits_by_day
from placekeyed_data d
join starbucks_origin s on d.id = s.facility_id
join safegraph_aws_us_east_2_starbucks_patterns_sample.public.patterns p on p.placekey = d.placekey;



