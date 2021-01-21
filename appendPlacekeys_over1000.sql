CREATE OR REPLACE PROCEDURE APPEND_PLACEKEYS_ALT(
  TBL_QUERY VARCHAR(100), --Input table
  TBL_MAPPING VARCHAR(100), 
  
  --The mapping table allows you to create a mapping between your input table and the column names expected by the Placekey API.
  --For the mapping, the COLUMN NAMES correspond to the PLACEKEY COLUMN NAMES, and the values of ROW 1 correspond to your input table's column names.
  --You may indicate any of the columns as NULL if you don't have them in your table
  
  TBL_OUT VARCHAR(100), --This is the name of your OUTPUT table.
  TBL_TEMP VARCHAR(100), --This is a TEMP table used to query the API and get the placekeys.
  COL_RECID VARCHAR(100), --This is the COLUMN NAME for the column that acts as <RECORD ID> in your INPUT TABLE. 
  API_FUNCTION VARCHAR(100), --The function to call. For this example, the function was named get_placekeys. Include only the name, not parentheses.
  BATCH_SIZE FLOAT --Size of the batch per operation. Can't be greater than 1000.

)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$

    try{
      // The RECID table must go from 0 to maxRecords - 1 for the JOIN operation to be successful.
      // Validate that the RECID column starts with 0

      var cmd_validateRecId = `SELECT CAST(${COL_RECID} as FLOAT) as RECID FROM ${TBL_QUERY} LIMIT 1`;

      var stmt_validateRecId = snowflake.createStatement( {sqlText: cmd_validateRecId});
      var result_validateRecId = stmt_validateRecId.execute();
      result_validateRecId.next();
      var firstKey = result_validateRecId.getColumnValue("RECID");

      if(firstKey != 0) {throw "The Record Id from the Input table must start with 0 and end with maxRecords - 1";}


      // Column mapping

      var cmd_map = `SELECT * FROM ${TBL_MAPPING};`
      var stmt_map = snowflake.createStatement( {sqlText: cmd_map} );
      var result_map = stmt_map.execute();
      result_map.next();
      c_location_name = result_map.getColumnValue("LOCATION_NAME");
      c_street_address = result_map.getColumnValue("STREET_ADDRESS");
      c_city = result_map.getColumnValue("CITY");
      c_region = result_map.getColumnValue("REGION");
      c_postal_code = result_map.getColumnValue("POSTAL_CODE");
      c_latitude = result_map.getColumnValue("LATITUDE");
      c_longitude = result_map.getColumnValue("LONGITUDE");
      c_country_code = result_map.getColumnValue("ISO_COUNTRY_CODE");

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
            SELECT ${API_FUNCTION}(a.*) AS RESULT FROM (
              SELECT ${COL_RECID}, 
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
        SELECT p.*, CAST(B.RESULT[1] AS VARCHAR(100)) AS PLACEKEY_RESULT, B.RESULT[2] AS error
        FROM ${TBL_QUERY} p
        INNER JOIN ${TBL_TEMP} B
        ON p.${COL_RECID} = B.RESULT[0]
        ORDER BY ${COL_RECID} ASC
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