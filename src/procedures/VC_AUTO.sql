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