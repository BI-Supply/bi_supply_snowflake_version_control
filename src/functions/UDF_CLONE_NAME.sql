CREATE OR REPLACE FUNCTION VC."UDF_CLONE_NAME"("SCHEMA_NAME" VARCHAR(16777216), "OBJECT_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS '

         SELECT SCHEMA_NAME || ''_'' || REPLACE(IFF(RIGHT(REGEXP_REPLACE(OBJECT_NAME,''[,()]'',''_''),1)=''_'', SUBSTRING(REGEXP_REPLACE(OBJECT_NAME,''[,()]'',''_''),0,LENGTH(REGEXP_REPLACE(OBJECT_NAME,''[,()]'',''_''))-1), REGEXP_REPLACE(OBJECT_NAME,''[,()]'',''_''))  ,''"'','''')  || ''_'' ||
                REPLACE(current_date()::STRING, ''-'', '''') || ''_'' || replace(current_time(), '':'', '''')

';