USING Progress.Lang.*.
USING Stomp.*.

ROUTINE-LEVEL ON ERROR UNDO, THROW.

define input parameter FileName as character.
define input parameter HostName as character.
define input parameter PortNumb as integer.
define input parameter UserName as character.
define input parameter UserPass as character.
define input parameter TopicName as character.
define input parameter HeaderData as character.

DEFINE TEMP-TABLE ttHeaders NO-UNDO
  FIELD cHdrData AS CHARACTER EXTENT 2.

DEFINE VARIABLE bigMessage   AS LONGCHAR  NO-UNDO.
define variable Filesize as integer.
define variable okflag as logical.
define var i as integer.

define variable IsBinary as logical.
define variable DoZip as logical.
define variable addMd5 as logical.
define variable md5value as character.
define variable RealFileName as character.
define variable RealFileSize as integer.
define variable UseTransaction as logical.

/* binary|true = binary file, will be encoded by uuencode to transfer though STOMP/MQ 
   zip|true = gzip source file before sending to MQ 
   md5|true = add md5 value to file packet
   transaction|true = use transaction (default)
   transaction|false = dont use transaction control.
*/

UseTransaction = true.
do i = 1 to NUM-ENTRIES (HeaderData, "|") by 2:
  if ENTRY(i,HeaderData, "|") = "binary" and 
     (ENTRY(i + 1,HeaderData, "|") = "yes" or ENTRY(i + 1,HeaderData, "|") = "true") then
    isBinary = yes.
  if ENTRY(i,HeaderData, "|") = "zip" and 
     (ENTRY(i + 1,HeaderData, "|") = "yes" or ENTRY(i + 1,HeaderData, "|") = "true") then
  do:
    DoZip = yes.
    isBinary = yes.
  end.
  if ENTRY(i,HeaderData, "|") = "md5" and 
     (ENTRY(i + 1,HeaderData, "|") = "yes" or ENTRY(i + 1,HeaderData, "|") = "true") then
    addMd5 = yes.
  if ENTRY(i,HeaderData, "|") = "transaction" and 
     (ENTRY(i + 1,HeaderData, "|") = "no" or ENTRY(i + 1,HeaderData, "|") = "false") then
    UseTransaction = false.
end.

if search(FileName) = ? then FileSize = 0.
else do:
  FILE-INFORMATION:FILE-NAME = FileName.
  FileSize = FILE-INFORMATION:FILE-SIZE.
end.
if FileSize = 0 then 
do:
  DoZip = False.
  isBinary = false.
  addMd5 = false.
end.

DEFINE VARIABLE objProducer   AS Stomp.Producer NO-UNDO.
DEFINE VARIABLE objLogger     AS Stomp.Logger   NO-UNDO.

objLogger   = NEW Stomp.Logger("log/producer.log", /* Log file name */
                                                2, /* Max logging entry level */
                                                2  /* Max logging error level */)
.
IF not VALID-OBJECT(objLogger) THEN RETURN "ERROR".
ASSIGN objProducer = NEW Stomp.Producer(TopicName, objLogger, "ErrorHandler", THIS-PROCEDURE).

/* Connect to ActiveMQ server */
IF NOT objProducer:connect(HostName, PortNumb, UserName, UserPass) THEN 
DO:
  RUN CleanUp.
  RETURN "ERROR".
END.

do i = 1 to NUM-ENTRIES (HeaderData, "|") by 2:
  if ENTRY(i,HeaderData, "|") = "binary" then NEXT.
  if ENTRY(i,HeaderData, "|") = "zip" then NEXT.
  if ENTRY(i,HeaderData, "|") = "md5" then NEXT.
  if ENTRY(i,HeaderData, "|") = "transaction" then NEXT.
  CREATE ttHeaders.
  ASSIGN ttHeaders.cHdrData[1] = ENTRY(i,HeaderData, "|").
  ASSIGN ttHeaders.cHdrData[2] = ENTRY(i + 1,HeaderData, "|").
end.


CREATE ttHeaders.
ASSIGN ttHeaders.cHdrData[1] = "persistent"
       ttHeaders.cHdrData[2] = "true".
CREATE ttHeaders.
ASSIGN ttHeaders.cHdrData[1] = "FileName"
       ttHeaders.cHdrData[2] = FileName.
CREATE ttHeaders.
ASSIGN ttHeaders.cHdrData[1] = "FileSize"
       ttHeaders.cHdrData[2] = STRING(FileSize).

if addMd5 then
do:
  input through value ("md5sum " + filename).
  import md5value.
  input close.

  CREATE ttHeaders.
  ASSIGN ttHeaders.cHdrData[1] = "MD5"
         ttHeaders.cHdrData[2] = md5value.
end.

RealFileName = FileName.
if DoZip then
do:
  realfilename = filename + ".gz".
  os-command silent value("gzip -c " + FileName + " > " + realfilename ).

  CREATE ttHeaders.
  ASSIGN ttHeaders.cHdrData[1] = "Zip"
         ttHeaders.cHdrData[2] = "yes".
end.
if isBinary then
do:
  os-command silent value("uuencode " + RealFileName + " " + RealFileName + " > " + realfilename + ".uu" ).
  if DoZip then OS-DELETE VALUE(RealFileName).
  realfilename = realfilename + ".uu".

  CREATE ttHeaders.
  ASSIGN ttHeaders.cHdrData[1] = "Binary"
         ttHeaders.cHdrData[2] = "yes".
end.
if search(realFileName) = ? then realFileSize = 0.
else do:
  FILE-INFORMATION:FILE-NAME = realFileName.
  realFileSize = FILE-INFORMATION:FILE-SIZE.
end.
if filesize <> 0 and realfilesize = 0 then
do:
  realFileName = FileName.
  realFileSize = FileSize.
end.

CREATE ttHeaders.
ASSIGN ttHeaders.cHdrData[1] = "RealFileSize"
       ttHeaders.cHdrData[2] = STRING(realFileSize).

okflag = false.
okflag = objProducer:sendFile(realFileName, UseTransaction, TABLE ttHeaders, "cHdrData").
if FileName <> realFileName then
  OS-DELETE VALUE(RealFileName).

FINALLY:
  RUN CleanUp.
  if okflag then RETURN "OK".
  else RETURN "ERROR".
END.

/*------------------------------------------------------------------------------------------------------------------*/
/*                                                    PROCEDURES                                                    */
/*------------------------------------------------------------------------------------------------------------------*/

/* Clean up heap memory */
PROCEDURE CleanUp:
    IF VALID-OBJECT(objProducer) THEN
      DELETE OBJECT objProducer.
    IF VALID-OBJECT(objLogger) THEN
      DELETE OBJECT objLogger.
    DELETE WIDGET-POOL.
END PROCEDURE.


/* Procedure for handling any errors received from the queue  */
/* (This gets called by the Stomp framework in case of error) */
PROCEDURE ErrorHandler:
  DEFINE INPUT PARAMETER ipiErrorLevel AS INTEGER        NO-UNDO.
  DEFINE INPUT PARAMETER ipcError      AS CHARACTER      NO-UNDO.
  DEFINE INPUT PARAMETER ipobjFrame    AS Stomp.Frame    NO-UNDO.
  
  /* Level 1:    Error:   System shutdown probably best course of action         */
  /* Level 2:  Warning:   System stability may be threatened; should advise user */
  /* Level 3+: Verbose:   Most probably safe to ignore                           */
  
  if valid-object (objLogger) then
    objLogger:writeError(ipiErrorLevel, ipcError).

  /* Dump raw Frame data to log, if available */
  IF ipobjFrame NE ? THEN
    if valid-object (objLogger) then
      objLogger:dumpErrorFrame(ipobjFrame, ipiErrorLevel). 

  IF ipiErrorLevel LE 2 THEN DO:
    /* Quit program on severe error */
    IF ipiErrorLevel LE 1 THEN DO:
      /* Garbage collection */
      RUN CleanUp.
    END. /* IF ipiErrorLevel LE 1 */
  END. /* IF ipiErrorLevel LE 2 */
  ELSE DO: /* ErrorLvl GE 3 */
      /* Do nothing - we will read log file if we are interested */
  END.

END PROCEDURE.

