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
    ('5', 'Twin Peaks Petroleum', '598 Portola Dr', 'San Francisco', 'CA', '94131', '37.7371', '-122.44283', 'US', 'other_value_1'),
    ('1', null, null, null, null, null, '37.7371', '-122.44283', 'US', 'other_value_2'),
    ('8', 'Beretta', '1199 Valencia St', 'San Francisco', 'CA', '94110', null, null, 'US', 'other_value_3'),
    ('3', 'Tasty Hand Pulled Noodle', '1 Doyers St', 'New York', 'ny', '10013', null, null, 'US', 'other_value_4'),
    ('7', null, '1 Doyers St', 'New York', 'NY', '10013', null, null, null, null);


// Get Placekeys for the data in test_addresses directly.

SELECT
  CAST(API_RESULT[0] AS INTEGER) AS ID,
  CAST(API_RESULT[1] AS VARCHAR) AS PLACEKEY,
  CAST(API_RESULT[2] AS VARCHAR) AS ERROR
FROM (
  SELECT get_placekeys(object_construct(joined.*)) AS API_RESULT
  FROM (
    SELECT
      ID as PRIMARY_KEY,
      NAME as LOCATION_NAME,
      STREETADDRESS as STREET_ADDRESS,
      CITY,
      STATE AS REGION,
      ZIPCODE AS POSTAL_CODE,
      LATITUDE, LONGITUDE,
      COUNTRY AS ISO_COUNTRY_CODE
    FROM test_addresses
  ) AS joined
) AS RESULT
ORDER BY ID;


// Get Placekeys for the data in test_addresses, but only query (id, street_address, city, and region). 
// Note that a null iso_country_code defaults to 'US'.

SELECT
  CAST(API_RESULT[0] AS INTEGER) AS ID,
  CAST(API_RESULT[1] AS VARCHAR) AS PLACEKEY,
  CAST(API_RESULT[2] AS VARCHAR) AS ERROR
FROM (
  SELECT get_placekeys(object_construct(joined.*)) AS API_RESULT
  FROM (
    SELECT
      ID AS PRIMARY_KEY,
      STREETADDRESS AS STREET_ADDRESS,
      CITY,
      STATE AS REGION
    FROM test_addresses
  ) AS joined
) AS RESULT
ORDER BY ID
;


// Use the following procedure to perform bulk queries.

CREATE OR REPLACE PROCEDURE APPEND_PLACEKEYS(
  TBL_QUERY VARCHAR(100), --Input table
  TBL_OUT VARCHAR(100), --This is the name of your OUTPUT table.
  TBL_TEMP VARCHAR(100), --This is a TEMP table used to query the API and get the placekeys.
  API_FUNCTION VARCHAR(100), --The function to call. For this example, the function was named get_placekeys. Include only the name, not parentheses.
  BATCH_SIZE FLOAT --Size of the batch per operation. Can't be greater than 1000.
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$

    try{
      // Column mapping SELECT * FROM TEST_ADDRESSES;
      c_primary_key = "ID";
      c_location_name = null;
      c_street_address = "STREETADDRESS";
      c_city = null; //"CITY";
      c_region = null; //"STATE";
      c_postal_code = "ZIPCODE";
      c_latitude = "LATITUDE";
      c_longitude = "LONGITUDE";
      c_country_code = null; //"COUNTRY";

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
                object_construct(a.*)
              ) AS RESULT
            FROM (
              SELECT
                ${c_primary_key} AS PRIMARY_KEY, 
                ${c_location_name} AS LOCATION_NAME, 
                ${c_street_address} AS STREET_ADDRESS, 
                ${c_city} AS CITY, 
                ${c_region} AS REGION, 
                ${c_postal_code} AS POSTAL_CODE, 
                ${c_latitude} AS LATITUDE,
                ${c_longitude} AS LONGITUDE, 
                ${c_country_code} AS ISO_COUNTRY_CODE 
              FROM ${TBL_QUERY}
              ORDER BY PRIMARY_KEY
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

CALL APPEND_PLACEKEYS('test_addresses', 'payload', 'temp', 'get_placekeys', 2);


// Check the results.

SELECT * FROM payload;
