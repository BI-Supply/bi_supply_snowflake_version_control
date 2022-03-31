CREATE OR REPLACE PROCEDURE VC.VC("OBJECT_TYPE" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216), "OBJECT_NAME" VARCHAR(16777216), "COMMENT" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
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

        /* Function that escapes single quotes with a Backslash */
        function escapeQuote(INPUT_STRING ) {
        INPUT_STRING = INPUT_STRING.replace(/\\\\/g, "\\\\\\\\");
        return INPUT_STRING =  INPUT_STRING.replace(/''/g, "\\\\''");}

        /* Function that adds the DDL with the Commit_ID, Comment etc. to the table _OBJECT_DDL */
        function writeDDLtoTable() { executeSQL(`INSERT INTO VC._OBJECT_DDL
                                                (OBJECT_TYPE,
                                                SCHEMA,
                                                OBJECT_NAME,
                                                COMMIT_ID,
                                                DDL_SQL,
                                                VC_CLONE_NAME,
                                                COMMIT_TS,
                                                COMMIT_COMMENT,
                                                COMMIT_USER)
                                            VALUES
                                            (
                                                ''${OBJECT_TYPE}''
                                                ,''${SCHEMA_NAME}''
                                                ,''${OBJECT_NAME}''
                                                ,${New_Commit_ID}
                                                ,''${DDL_SQL}''
                                                ,''${VC_Clone_Name}''
                                                ,current_timestamp()
                                                ,''${COMMENT}''
                                                ,current_user())`)
        };
        /* Function that adds the Object to _AUTO_BACKUP_CONTROL so backups are created automatically */
        function addToAutoBackupControl() { executeSQL(`INSERT INTO VC._AUTO_BACKUP_CONTROL
                                                        (OBJECT_TYPE,
                                                        SCHEMA,
                                                        OBJECT_NAME,
                                                        LAST_COMMIT_ID,
                                                        LAST_COMMIT_TS,
                                                        LAST_COMMIT_COMMENT,
                                                        LAST_COMMIT_USER)
                                                    VALUES
                                                    (
                                                        ''${OBJECT_TYPE}''
                                                        ,''${SCHEMA_NAME}''
                                                        ,''${OBJECT_NAME}''
                                                        ,${New_Commit_ID}
                                                        ,current_timestamp()
                                                        ,''${COMMENT}''
                                                        ,current_user())`)
        };
        /* Function that updates _AUTO_BACKUP_CONTROL with the last Commit_ID, TS, Comment und user */
        function UpdateAutoBackupControl() { executeSQL(`UPDATE VC._AUTO_BACKUP_CONTROL
                                                                SET LAST_COMMIT_ID = ${New_Commit_ID}
                                                                    ,LAST_COMMIT_TS = current_timestamp()
                                                                    ,LAST_COMMIT_COMMENT = ''${COMMENT}''
                                                                    , LAST_COMMIT_USER = CURRENT_USER()
                                                                WHERE
                                                                      OBJECT_TYPE = ''${OBJECT_TYPE}''
                                                                      AND SCHEMA = ''${SCHEMA_NAME}''
                                                                      AND OBJECT_NAME = ''${OBJECT_NAME}''
                                                                `)
        };

        /* function that gets the DDL of object and saves it in a variable */
        function getDDL() {
        DDL_SQL = executeSQL(`select get_ddl(''${OBJECT_TYPE}'' , ''${SCHEMA_NAME}.${OBJECT_NAME}'', true )`);
        DDL_SQL = escapeQuote(DDL_SQL);
        return DDL_SQL;
        ;}

        /* Creates a clone and adds the comment on the clone */
        function createClone() {
                /* CREATE CLONE */
                executeSQL(`CREATE OR REPLACE ${OBJECT_TYPE} VC.${VC_Clone_Name} CLONE ${SCHEMA_NAME}.${OBJECT_NAME}`);
                /* add comment to clone */
                executeSQL(`COMMENT ON ${OBJECT_TYPE} VC.${VC_Clone_Name} is ''${COMMENT}''`);
        }


       /* DECLARE Variables and format the input strings for consitency */
       var OBJECT_TYPE = executeSQL(`SELECT VC.UDF_Handle_Object_Names(''${OBJECT_TYPE}'')`);
       var SCHEMA_NAME = executeSQL(`SELECT VC.UDF_Handle_Object_Names(''${SCHEMA_NAME}'')`);
       var OBJECT_NAME = executeSQL(`SELECT VC.UDF_Handle_Object_Names(''${OBJECT_NAME}'')`);
       var COMMENT = COMMENT.trim();
       var DDL_SQL = '''';
       var VC_Clone_Name = ''N/A'';
       var return_value = '''';
       /* get the Commit_ID of the object and calculate the New_Commit_ID */
       // var Commit_ID =  executeSQL(`SELECT IFNULL((SELECT MAX(COMMIT_ID) FROM VC._OBJECT_DDL WHERE OBJECT_TYPE = ''${OBJECT_TYPE}'' AND SCHEMA= ''${SCHEMA_NAME}'' AND OBJECT_NAME=''${OBJECT_NAME}''),0) as MAX_COMMIT_ID`);
       var Commit_ID = executeSQL(`SELECT VC.UDF_MAX_COMMIT_ID(''${OBJECT_TYPE}'',''${SCHEMA_NAME}'',''${OBJECT_NAME}'')`);
       var New_Commit_ID = Commit_ID+1;


    if (Commit_ID == 0)
        {
        getDDL();
        if (OBJECT_TYPE == ''TABLE'') {
                VC_Clone_Name =    executeSQL(`SELECT VC.UDF_CLONE_NAME( ''${SCHEMA_NAME}'',''${OBJECT_NAME}'')`);
                createClone();
                                    }
        writeDDLtoTable();
        addToAutoBackupControl();
        return_value = `DDL was was written into _OBJECT_DDL:
 Object type: ${OBJECT_TYPE}
 Schema name: ${SCHEMA_NAME}
 Object name: ${OBJECT_NAME}
 Commit ID:   ${Commit_ID+1}`;


        if (OBJECT_TYPE == ''TABLE'') { 
            return_value = return_value + `
The clone ${VC_Clone_Name} was created.`;
                                    }
    
        }

        else if (Commit_ID >= 1)
        {
        var DDL_Diff = executeSQL(`call VC.VC_CHECK_DDL_DIFF(''${OBJECT_TYPE}'',''${SCHEMA_NAME}'',''${OBJECT_NAME}'')`);

        if (OBJECT_TYPE == ''TABLE'') {
                var Clone_Diff = executeSQL(`call VC.VC_CHECK_CLONE_DIFF(''${OBJECT_TYPE}'',''${SCHEMA_NAME}'',''${OBJECT_NAME}'')`);
        if (DDL_Diff == true || Clone_Diff == true) {
                VC_Clone_Name =    executeSQL(`SELECT VC.UDF_CLONE_NAME( ''${SCHEMA_NAME}'',''${OBJECT_NAME}'')`);
                createClone();
                getDDL();
                writeDDLtoTable();
                UpdateAutoBackupControl();
        return_value =  `DDL was was written into _OBJECT_DDL:
Object type: ${OBJECT_TYPE}
Schema name: ${SCHEMA_NAME}
Object name: ${OBJECT_NAME}
Commit ID:   ${Commit_ID+1}`;
        if (OBJECT_TYPE == ''TABLE'') { 
            return_value = return_value + `
The clone ${VC_Clone_Name} was created.`;
                                    }
    }
        else {return_value = `There is no difference between the current DDL and clone and the last DDL and clone recorded in _OBJECT_DDL with the Commit_ID ${Commit_ID}.`;}

        }
        else {
        if (DDL_Diff == true) {
                getDDL();
                writeDDLtoTable();
                UpdateAutoBackupControl();
                return_value =  `DDL was was written into _OBJECT_DDL:
Object type: ${OBJECT_TYPE}
Schema name: ${SCHEMA_NAME}
Object name: ${OBJECT_NAME}
Commit ID:   ${Commit_ID+1}`;
                }
        else {return_value = `There is no difference between the current DDL and the last DDL recorded in _OBJECT_DDL with the Commit_ID ${Commit_ID}.`;}
           }
    }

        /*
        return_value = `DDL_Diff is ${DDL_Diff}
                        Clone_Diff is ${Clone_Diff}`;
        */
            return return_value;
';