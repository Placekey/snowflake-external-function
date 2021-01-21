CREATE OR REPLACE PROCEDURE APPEND_PLACEKEYS_SUB1000(
    TBL_INPUT VARCHAR(100),
    TBL_MAPPING VARCHAR(100),
    TBL_OUTPUT VARCHAR(100),
    API_FUNCTION VARCHAR(100)
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
    try{
    
      //Counting the amount of rows to ensure it's less than a 1000.
      
      var cmd_countInput = `SELECT CAST(COUNT(*) AS FLOAT) AS INPUT_SIZE FROM ${TBL_INPUT}`;
      var stmt_countInput = snowflake.createStatement( {sqlText: cmd_countInput} );
      var result_countInput = stmt_countInput.execute();
      result_countInput.next();
      var tableSize = result_countInput.getColumnValue("INPUT_SIZE");
      if(tableSize > 1000) {throw "Input table has more than 1000 rows";}

      //Assigning the values from the MAPPING table

      var cmd_map = `SELECT * FROM ${TBL_MAPPING};`;
      var stmt_map = snowflake.createStatement( {sqlText: cmd_map} );
      var result_map = stmt_map.execute();
      result_map.next();
      
      // PRIMARY_KEY column should exist in the mapping table already.
      c_id = result_map.getColumnValue("PRIMARY_KEY");
      c_location_name = result_map.getColumnValue("LOCATION_NAME");
      c_street_address = result_map.getColumnValue("STREET_ADDRESS");
      c_city = result_map.getColumnValue("CITY");
      c_region = result_map.getColumnValue("REGION");
      c_postal_code = result_map.getColumnValue("POSTAL_CODE");
      c_latitude = result_map.getColumnValue("LATITUDE");
      c_longitude = result_map.getColumnValue("LONGITUDE");
      c_country_code = result_map.getColumnValue("ISO_COUNTRY_CODE");
      
      //Querying the API.
      
      var cmd_outputCreation = `CREATE OR REPLACE TABLE ${TBL_OUTPUT} AS(
                                SELECT A.*, B.RESULT[0] AS PLACEKEY_ID, CAST(B.RESULT[1] AS VARCHAR(100)) AS PLACEKEY_RESULT
                                FROM(
                                  SELECT ${API_FUNCTION}(joined.*) AS result
                                  FROM (
                                      SELECT ${c_id}, ${c_location_name}, 
                                      ${c_street_address}, ${c_city}, ${c_region}, 
                                      ${c_postal_code}, ${c_latitude}, ${c_longitude},
                                      ${c_country_code}
                                      FROM ${TBL_INPUT} 
                                  ) AS joined
                                ) AS B
                                INNER JOIN ${TBL_INPUT} AS A
                                ON A.${c_id} = B.RESULT[0]
                              )`;
       var stmt_outputCreation = snowflake.createStatement( {sqlText: cmd_outputCreation} );
       var result_outputCreation = stmt_outputCreation.execute();
       
       var cmd_cleanPlacekeys = `UPDATE ${TBL_OUTPUT} SET PLACEKEY = REPLACE(PLACEKEY,'"', '')`;
       var stmt_cleanPlacekeys = snowflake.createStatement( {sqlText: cmd_cleanPlacekeys} );
       var result_cleanPlacekeys = stmt_cleanPlacekeys.execute();
       
       return `Placekeys appended to table ${TBL_OUTPUT}`;
    } catch (err) {
        return `PROCEDURE FAILED: ${err}. Stack trace: ${err.stackTraceTxt}`;
    }
$$;
