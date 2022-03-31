CREATE SCHEMA IF NOT EXISTS VC; 
USE SCHEMA VC; 

/* ===========TABLES=========== */
-- DWH.VC.AAA_AUTO_BACKUP_CONTROL definition
create or replace TABLE _AUTO_BACKUP_CONTROL (
	OBJECT_TYPE VARCHAR(255) NOT NULL,
	SCHEMA VARCHAR(25) NOT NULL,
	OBJECT_NAME VARCHAR(255) NOT NULL,
	AUTOBACKUP_ACTIVE BOOLEAN NOT NULL DEFAULT TRUE,
	LAST_COMMIT_ID NUMBER(38,0),
	LAST_COMMIT_TS TIMESTAMP_NTZ(9),
	LAST_COMMIT_COMMENT VARCHAR(16777216),
	LAST_COMMIT_USER VARCHAR(16777216),
	primary key (OBJECT_TYPE, SCHEMA, OBJECT_NAME)
);




-- DWH.VC.AAA_OBJECT_DDL definition
create or replace TABLE _OBJECT_DDL (
	OBJECT_TYPE VARCHAR(255) NOT NULL,
	SCHEMA VARCHAR(25) NOT NULL,
	OBJECT_NAME VARCHAR(255) NOT NULL,
	COMMIT_ID NUMBER(38,0) NOT NULL,
	DDL_SQL VARCHAR(16777216),
	VC_CLONE_NAME VARCHAR(16777216),
	COMMIT_TS TIMESTAMP_NTZ(9),
	COMMIT_COMMENT VARCHAR(16777216),
	COMMIT_USER VARCHAR(16777216),
	primary key (OBJECT_TYPE, SCHEMA, OBJECT_NAME, COMMIT_ID)
);
/* ===========VIEWS=========== */
/* ===========PROCEDURES=========== */
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
CREATE OR REPLACE PROCEDURE VC.VC_AUTO()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var comment = ''Automatischer Eintrag durch die Prozedur VC.VC_AUTO()''
    var return_text = '''';
	var Get_Objects =  `SELECT
                    OBJECT_TYPE
                    ,SCHEMA
                    ,OBJECT_NAME

                    FROM VC._AUTO_BACKUP_CONTROL
                    WHERE
                    AUTOBACKUP_ACTIVE = TRUE `;


    var stmt = snowflake.createStatement(
           {
           sqlText: Get_Objects
           }
        );


	/* Creates result set */
	var res = stmt.execute();
    while (res.next())  {
        var OBJECT_TYPE = res.getColumnValue(1);
        var SCHEMA_NAME = res.getColumnValue(2);
        var OBJECT_NAME = res.getColumnValue(3);
        var call_vc = `call VC.VC (''${OBJECT_TYPE}'', ''${SCHEMA_NAME}'', ''${OBJECT_NAME}'', ''${comment}'')`
           var statement = snowflake.createStatement(
           {
           sqlText: call_vc
           });
        var result = statement.execute();

    return_text = return_text +
    `OBJECT_TYPE: ${OBJECT_TYPE}
     SCHEMA_NAME: ${SCHEMA_NAME}
     OBJECT_NAME: ${OBJECT_NAME}
    `;

        }
return `SP VC.VC ausgef�hrt f�r:
        ` + return_text;
   ';
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



    //var Commit_ID =  executeSQL(`SELECT IFNULL((SELECT MAX(COMMIT_ID) FROM VC._OBJECT_DDL WHERE OBJECT_TYPE = ''${OBJECT_TYPE}'' AND SCHEMA= ''${SCHEMA_NAME}'' AND OBJECT_NAME=''${OBJECT_NAME}''),0) as MAX_COMMIT_ID`);
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
CREATE OR REPLACE PROCEDURE VC."VC_CHECK_DDL_DIFF"("OBJECT_TYPE" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216), "OBJECT_NAME" VARCHAR(16777216))
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


    //   var Commit_ID =  executeSQL(`SELECT IFNULL((SELECT MAX(COMMIT_ID) FROM VC._OBJECT_DDL WHERE OBJECT_TYPE = ''${OBJECT_TYPE}'' AND SCHEMA= ''${SCHEMA_NAME}'' AND OBJECT_NAME=''${OBJECT_NAME}''),0) as MAX_COMMIT_ID`);
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


/*
        return `OBJECT_TYPE: ${OBJECT_TYPE}
            SCHEMA_NAME: ${SCHEMA_NAME}
            OBJECT_NAME: ${OBJECT_NAME}
            DDL_SQL: ${DDL_SQL}
           Commit_ID: ${Commit_ID}
            DDL_SQL_BACKUP: ${DDL_SQL_BACKUP}
    `
*/
';
/* ===========FUNCTIONS=========== */
CREATE OR REPLACE FUNCTION VC."UDF_CLONE_NAME"("SCHEMA_NAME" VARCHAR(16777216), "OBJECT_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS '

         SELECT SCHEMA_NAME || ''_'' || IFF(RIGHT(REGEXP_REPLACE(OBJECT_NAME,''[,()]'',''_''),1)=''_'', SUBSTRING(REGEXP_REPLACE(OBJECT_NAME,''[,()]'',''_''),0,LENGTH(REGEXP_REPLACE(OBJECT_NAME,''[,()]'',''_''))-1), REGEXP_REPLACE(OBJECT_NAME,''[,()]'',''_''))    || ''_'' ||
                REPLACE(current_date()::STRING, ''-'', '''') || ''_'' || replace(current_time(), '':'', '''')

';
CREATE OR REPLACE FUNCTION VC."UDF_CLONE_NAME_VERSION_OLD"("SCHEMA_NAME" VARCHAR(16777216), "OBJECT_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS 'SELECT
  (SELECT VC.UDF_CLONE_NAME(SCHEMA_NAME, OBJECT_NAME))|| ''_V''||
    IFNULL((
        SELECT TOP 1
         (SUBSTR(TABLE_NAME,LEN(SELECT VC.UDF_CLONE_NAME(SCHEMA_NAME, OBJECT_NAME))+3)+1)::INT as Clone_ID FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ''VC'' and table_name like (SELECT VC.UDF_CLONE_NAME(SCHEMA_NAME, OBJECT_NAME))||''%''
        ORDER BY CREATED DESC),1)
';

CREATE OR REPLACE FUNCTION VC.UDF_HANDLE_OBJECT_NAMES("STRING" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS '


STRING = STRING.trim();
var SEARCHSTRING = ''\\"''
var indices = [];

for(var i=0; i<STRING.length;i++) {
    if (STRING[i] === SEARCHSTRING) {indices.push(i)};
   
}

 
  if (STRING.trim().charAt(0) == ''"'' && indices.length >= 2) {
    var last_quote = indices[indices.length -1];
    var first_part = STRING.substring(0,last_quote+1);
    var second_part = STRING.substr(last_quote+1);
    var second_part = second_part.replace(/\\s+/g, '''').toUpperCase();
    return first_part + second_part

    }
    else
    {
    return STRING = STRING.trim().toUpperCase();
    }
    
';

CREATE OR REPLACE FUNCTION VC."UDF_MAX_COMMIT_ID"("OBJECT_TYPE1" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216), "OBJECT_NAME1" VARCHAR(16777216))
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS 'SELECT IFNULL((SELECT MAX(COMMIT_ID) FROM VC._OBJECT_DDL WHERE OBJECT_TYPE = OBJECT_TYPE1 AND SCHEMA= SCHEMA_NAME AND OBJECT_NAME=OBJECT_NAME1),0) as MAX_COMMIT_ID


';
