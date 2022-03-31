CREATE OR REPLACE PROCEDURE VC.VC_CHECK_CLONE_DIFF("OBJECT_TYPE" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216), "OBJECT_NAME" VARCHAR(16777216))
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


       var OBJECT_TYPE = executeSQL(`SELECT VC.UDF_Handle_Object_Names(''${OBJECT_TYPE}'')`);
       var SCHEMA_NAME = executeSQL(`SELECT VC.UDF_Handle_Object_Names(''${SCHEMA_NAME}'')`);
       var OBJECT_NAME = executeSQL(`SELECT VC.UDF_Handle_Object_Names(''${OBJECT_NAME}'')`);
       var flag_difference = false;

    var Commit_ID = executeSQL(`SELECT VC.UDF_MAX_COMMIT_ID(''${OBJECT_TYPE}'',''${SCHEMA_NAME}'',''${OBJECT_NAME}'')`);
    var Clone_Name = executeSQL(`SELECT VC_CLONE_NAME FROM "VC"."_OBJECT_DDL"
                                  WHERE OBJECT_TYPE = ''${OBJECT_TYPE}''
                                  AND SCHEMA = ''${SCHEMA_NAME}''
                                  AND OBJECT_NAME =''${OBJECT_NAME}''
                                  AND COMMIT_ID = ${Commit_ID}`);

    var columns_object = executeSQL(`SELECT COUNT(COLUMN_NAME)
                                    FROM INFORMATION_SCHEMA.COLUMNS
                                    WHERE TABLE_SCHEMA = ''${SCHEMA_NAME}''
                                    AND TABLE_NAME = ''${OBJECT_NAME}''`)

    var columns_clone = executeSQL(`    SELECT COUNT(COLUMN_NAME)
                                        FROM INFORMATION_SCHEMA.COLUMNS
                                        WHERE TABLE_SCHEMA = ''VC''
                                        AND TABLE_NAME = ''${Clone_Name}''`)

    if (columns_object != columns_clone) {
    flag_difference = true;
    }
    else
    {
    var Clone_Diff = executeSQL(`SELECT IFF(Difference>0, TRUE, FALSE)
                        FROM (
                                 select count(*) as Difference
                                 FROM (
                                          select *
                                          from (
                                                   select *
                                                   from ${SCHEMA_NAME}.${OBJECT_NAME}
                                                   minus
                                                   select *
                                                   from VC.${Clone_Name}
                                               )
                                          union all
                                          select *
                                          from (
                                                   select *
                                                   from VC.${Clone_Name}
                                                   minus
                                                   select *
                                                   from ${SCHEMA_NAME}.${OBJECT_NAME}
                                               )
                                      )
                             )`)
    flag_difference = Clone_Diff;
}
    return flag_difference
';