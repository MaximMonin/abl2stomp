define input parameter filename as character.
define input parameter TABLE-HANDLE hTableData.

DEFINE VARIABLE cTargetType     AS CHARACTER NO-UNDO.
DEFINE VARIABLE cFile           AS CHARACTER NO-UNDO.
DEFINE VARIABLE lFormatted      AS LOGICAL   NO-UNDO.
DEFINE VARIABLE cEncoding       AS CHARACTER NO-UNDO.
DEFINE VARIABLE cSchemaLocation AS CHARACTER NO-UNDO.
DEFINE VARIABLE lWriteSchema    AS LOGICAL   NO-UNDO.
DEFINE VARIABLE lMinSchema      AS LOGICAL   NO-UNDO.
DEFINE VARIABLE retOK           AS LOGICAL   NO-UNDO.

ASSIGN  cTargetType     = "file"  
cFile           = filename   
lFormatted      = true 
cEncoding       = ?  
cSchemaLocation = ?  
lWriteSchema    = true
lMinSchema      = false. 
retOK = hTableData:WRITE-XML ( cTargetType, cFile, lFormatted, cEncoding,  cSchemaLocation, lWriteSchema, lMinSchema).

return STRING(retOK).
