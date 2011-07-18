define input parameter filename as character.
define output parameter DATASET-HANDLE DatasetData.

DEFINE VARIABLE cSourceType             AS CHARACTER NO-UNDO.
DEFINE VARIABLE cReadMode               AS CHARACTER NO-UNDO.
DEFINE VARIABLE lOverrideDefaultMapping AS LOGICAL   NO-UNDO.
DEFINE VARIABLE cFile                   AS CHARACTER NO-UNDO.
DEFINE VARIABLE cEncoding               AS CHARACTER NO-UNDO.
DEFINE VARIABLE cSchemaLocation         AS CHARACTER NO-UNDO.
DEFINE VARIABLE cFieldTypeMapping       AS CHARACTER NO-UNDO.
DEFINE VARIABLE cVerifySchemaMode       AS CHARACTER NO-UNDO.
DEFINE VARIABLE retOK                   AS LOGICAL   NO-UNDO.
DEFINE VARIABLE httCust                 AS HANDLE    NO-UNDO. 

CREATE DATASET DatasetData.
ASSIGN  cSourceType     = "file"  
cFile                   = filename   
cReadMode               = "empty"  
cSchemaLocation         = ?  
lOverrideDefaultMapping = ?  
cFieldTypeMapping       = ?  
cVerifySchemaMode       = ?. 
retOK = DatasetData:READ-XML(cSourceType, cFile, cReadMode, cSchemaLocation,  lOverrideDefaultMapping, cFieldTypeMapping, cVerifySchemaMode).
