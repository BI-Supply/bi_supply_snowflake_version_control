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
