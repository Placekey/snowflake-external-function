/**

Snowflake External Function

The Snowflake External Function allows you to append Placekeys to your address and POI data stored in Snowflake. 

The function can be used similarly to a user-defined function in a query. 

Sign in with an ACCOUNTADMIN role to your Snowflake instance. Alternatively, sign in with a role with the global CREATE INTEGRATION privilege.

**/


// Create the API Integration.

USE ROLE ACCOUNTADMIN;
USE <PASTE_YOUR_DB_HERE>;


CREATE OR REPLACE API INTEGRATION placekey_api_integration
  API_PROVIDER = aws_api_gateway
  API_AWS_ROLE_ARN = 'arn:aws:iam::886725170148:role/placekey-lambda-production'
  ENABLED = true
  API_ALLOWED_PREFIXES = ('https://lbdl9njufi.execute-api.us-east-1.amazonaws.com/api/')
;


// Create the External function.

CREATE OR REPLACE EXTERNAL FUNCTION get_placekeys(
  mapping variant,
  input variant
)
  RETURNS variant
  API_INTEGRATION = placekey_api_integration
  HEADERS = ('api-key' = '<PASTE_YOUR_KEY_HERE>')
  MAX_BATCH_ROWS = 1000
  AS 'https://lbdl9njufi.execute-api.us-east-1.amazonaws.com/api/placekeys'
;


// Make a table test_addresses to hold sample address and point-of-interest information.

CREATE OR REPLACE TABLE test_addresses (
  ID VARCHAR(16777216),
  NAME VARCHAR(16777216),
  STREETADDRESS VARCHAR(16777216),
  CITY VARCHAR(16777216),
  STATE VARCHAR(16777216),
  ZIPCODE VARCHAR(16777216),
  LATITUDE VARCHAR(16777216),
  LONGITUDE VARCHAR(16777216),
  COUNTRY VARCHAR(16777216),
  OTHER_COLUMN VARCHAR(16777216)
);


// Insert some data into test_addresses.

INSERT INTO test_addresses
    VALUES 
    ('0', 'Twin Peaks Petroleum', '598 Portola Dr', 'San Francisco', 'CA', '94131', '37.7371', '-122.44283', 'US', 'other_value_1'),
    ('1', null, null, null, null, null, '37.7371', '-122.44283', 'US', 'other_value_2'),
    ('2', 'Beretta', '1199 Valencia St', 'San Francisco', 'CA', '94110', null, null, 'US', 'other_value_3'),
    ('3', 'Tasty Hand Pulled Noodle', '1 Doyers St', 'New York', 'ny', '10013', null, null, 'US', 'other_value_4'),
    ('4', null, '1 Doyers St', 'New York', 'NY', '10013', null, null, null, null);


// Get Placekeys for the data in test_addresses directly.

SELECT get_placekeys(
  (SELECT object_construct(*) FROM test_lookup), object_construct(joined.*)
) AS RESULT
FROM (
  SELECT ID, NAME, STREETADDRESS, CITY, STATE, ZIPCODE, LATITUDE, LONGITUDE, COUNTRY
  FROM test_addresses
) AS joined;


// Get Placekeys for the data in test_addresses, but only query (id, street_address, city, and region). 
// Note that a null iso_country_code defaults to 'US'.

SELECT get_placekeys(
  (SELECT object_construct(*) FROM test_lookup), object_construct(joined.*))
FROM (
  SELECT ID, null AS missing_name, STREETADDRESS, CITY, STATE, null AS missing_zip, null AS no_lat, null AS no_lon, null AS defaults_to_us
  FROM test_addresses
) AS joined;

/**
Using the External Function Within a Procedure

To query Placekeys for more than 1,000 rows, use a procedure. 
Create a table to map the column names in your table to the Placekey API fields:
**/

CREATE OR REPLACE TABLE test_lookup (
  PRIMARY_KEY VARCHAR(100),
  STREET_ADDRESS VARCHAR(100),
  CITY VARCHAR(100),
  REGION VARCHAR(100),
  POSTAL_CODE VARCHAR(100),
  LOCATION_NAME VARCHAR(100),
  LATITUDE VARCHAR(100),
  LONGITUDE VARCHAR(100),
  ISO_COUNTRY_CODE VARCHAR(100)
);


insert into test_lookup
  values
  ('ID', 'STREETADDRESS', 'CITY', 'STATE', 'ZIPCODE', 'NAME', 'LATITUDE', 'LONGITUDE', 'COUNTRY')
;


// Use the following procedure to perform bulk queries.

CREATE OR REPLACE PROCEDURE APPEND_PLACEKEYS(
  TBL_QUERY VARCHAR(100), --Input table
  TBL_MAPPING VARCHAR(100), --Column mapping table
  
  --The mapping table allows you to create a mapping between your input table and the column names expected by the Placekey API.
  --For the mapping, the COLUMN NAMES correspond to the PLACEKEY COLUMN NAMES, and the values of ROW 1 correspond to your input table's column names.
  --You may indicate any of the columns as NULL if you don't have them in your table
  
  TBL_OUT VARCHAR(100), --This is the name of your OUTPUT table.
  TBL_TEMP VARCHAR(100), --This is a TEMP table used to query the API and get the placekeys.
  API_FUNCTION VARCHAR(100), --The function to call. For this example, the function was named get_placekeys. Include only the name, not parentheses.
  BATCH_SIZE FLOAT --Size of the batch per operation. Can't be greater than 1000.

)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$

    try{
      // Column mapping

      var cmd_map = `SELECT * FROM ${TBL_MAPPING};`
      var stmt_map = snowflake.createStatement( {sqlText: cmd_map} );
      var result_map = stmt_map.execute();
      result_map.next();
      c_primary_key = result_map.getColumnValue("PRIMARY_KEY");
      c_location_name = result_map.getColumnValue("LOCATION_NAME");
      c_street_address = result_map.getColumnValue("STREET_ADDRESS");
      c_city = result_map.getColumnValue("CITY");
      c_region = result_map.getColumnValue("REGION");
      c_postal_code = result_map.getColumnValue("POSTAL_CODE");
      c_latitude = result_map.getColumnValue("LATITUDE");
      c_longitude = result_map.getColumnValue("LONGITUDE");
      c_country_code = result_map.getColumnValue("ISO_COUNTRY_CODE");


      // The RECID table must go from 0 to maxRecords - 1 for the JOIN operation to be successful.
      // Validate that the RECID column starts with 0

      var cmd_validateRecId = `SELECT CAST(${c_primary_key} as FLOAT) as RECID FROM ${TBL_QUERY} LIMIT 1`;

      var stmt_validateRecId = snowflake.createStatement( {sqlText: cmd_validateRecId});
      var result_validateRecId = stmt_validateRecId.execute();
      result_validateRecId.next();
      var firstKey = result_validateRecId.getColumnValue("RECID");

      if(firstKey != 0) {throw "The Record Id from the Input table must start with 0 and end with maxRecords - 1";}

      // Create a temporary table to store the results of the query

      var cmd_payload = `CREATE OR REPLACE TEMPORARY TABLE ${TBL_TEMP} (RESULT ARRAY);`
      var stmt_payload = snowflake.createStatement( {sqlText: cmd_payload} );
      var result_payload = stmt_payload.execute();
      result_payload.next();

      // Query the API

      var cmd_count = `SELECT COUNT(*) FROM ${TBL_QUERY};`   
      var stmt_count = snowflake.createStatement( {sqlText: cmd_count} );
      var result_count = stmt_count.execute();
      result_count.next()
      var num_rows = result_count.getColumnValue(1)
      var num_batches = Math.ceil(num_rows / BATCH_SIZE)

      for (var i = 0; i < num_batches; i++) {
        var cmd_api = `
          INSERT INTO ${TBL_TEMP}(RESULT)
            SELECT ${API_FUNCTION}(
                (SELECT object_construct(*) FROM test_lookup),
                object_construct(a.*)
              ) AS RESULT
            FROM (
              SELECT ${c_primary_key}, 
              ${c_location_name}, 
              ${c_street_address}, 
              ${c_city}, 
              ${c_region}, 
              ${c_postal_code}, 
              ${c_latitude},
              ${c_longitude}, 
              ${c_country_code} 
              FROM ${TBL_QUERY} 
              LIMIT ${BATCH_SIZE}
              OFFSET ${BATCH_SIZE * i} ) AS a;
        `;

        var statementLoop = snowflake.createStatement( {sqlText: cmd_api} );
        var result_setLoop = statementLoop.execute();
        result_setLoop.next();
      }

      var cmd_join = `CREATE OR REPLACE TABLE ${TBL_OUT} AS (
        SELECT p.*, CAST(B.RESULT[1] AS VARCHAR(100)) AS PLACEKEY, B.RESULT[2] AS error
        FROM ${TBL_QUERY} p
        INNER JOIN ${TBL_TEMP} B
        ON p.${c_primary_key} = B.RESULT[0]
        ORDER BY ${c_primary_key} ASC
      )`;

      var stmt_join = snowflake.createStatement( {sqlText: cmd_join} );
      var result_join = stmt_join.execute();

      var cmd_cleanPlacekeys = `UPDATE ${TBL_OUT} SET PLACEKEY = REPLACE(PLACEKEY,'"', '')`;
      var stmt_cleanPlacekeys = snowflake.createStatement( {sqlText: cmd_cleanPlacekeys} );
      var result_cleanPlacekeys = stmt_cleanPlacekeys.execute();

      return `Done! Data stored in table: ${TBL_OUT}`;

    } catch (err) {
        return `ERROR: ${err} - ${err.stackTraceTxt}`
    }
$$
;


// Call the procedure.

CALL APPEND_PLACEKEYS('test_addresses', 'test_lookup', 'payload', 'temp', 'get_placekeys', 2);


// Check the results.

SELECT * FROM payload;
