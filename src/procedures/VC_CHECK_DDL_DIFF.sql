CREATE OR REPLACE PROCEDURE VC.VC_CHECK_DDL_DIFF("OBJECT_TYPE" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216), "OBJECT_NAME" VARCHAR(16777216))
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '/* Create function to execute SQLs */
    function executeSQL(SQL ) {
         // Prepare statement.
         var stmt = snowflake.createStatement(
                {
                sqlText: SQL
                }
             );
         // Execute Statement
         var res = stmt.execute();
         res.next();
         return res.getColumnValue(1);
        }
        function escapeQuote(INPUT_STRING ) {
    return INPUT_STRING =  INPUT_STRING.replace(/''/g, "\\\\''");}

       var OBJECT_TYPE = executeSQL(`SELECT VC.UDF_Handle_Object_Names(''${OBJECT_TYPE}'')`);
       var SCHEMA_NAME = executeSQL(`SELECT VC.UDF_Handle_Object_Names(''${SCHEMA_NAME}'')`);
       var OBJECT_NAME = executeSQL(`SELECT VC.UDF_Handle_Object_Names(''${OBJECT_NAME}'')`);

	var DDL_SQL = executeSQL(`select get_ddl(''${OBJECT_TYPE}'' , ''${SCHEMA_NAME}.${OBJECT_NAME}'', true )`);
    var Commit_ID = executeSQL(`SELECT VC.UDF_MAX_COMMIT_ID(''${OBJECT_TYPE}'',''${SCHEMA_NAME}'',''${OBJECT_NAME}'')`);
    var DDL_SQL_BACKUP = executeSQL (`SELECT DDL_SQL FROM "VC"."_OBJECT_DDL"
                                      WHERE OBJECT_TYPE = ''${OBJECT_TYPE}''
                                      AND SCHEMA = ''${SCHEMA_NAME}''
                                      AND OBJECT_NAME = ''${OBJECT_NAME}''
                                      AND COMMIT_ID = ${Commit_ID}`);

if (DDL_SQL == DDL_SQL_BACKUP)
    {return false;
    }
    else {
    return true;
     }
';