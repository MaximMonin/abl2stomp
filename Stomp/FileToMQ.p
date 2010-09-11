USING Progress.Lang.*.
USING Stomp.*.

ROUTINE-LEVEL ON ERROR UNDO, THROW.

define input parameter FileName as character.
define input parameter HostName as character.
define input parameter PortNumb as integer.
define input parameter UserName as character.
define input parameter UserPass as character.
define input parameter TopicName as character.

DEFINE TEMP-TABLE ttHeaders NO-UNDO
  FIELD cHdrData AS CHARACTER EXTENT 2.

DEFINE VARIABLE bigMessage   AS LONGCHAR  NO-UNDO.
define variable Filesize as integer.

DEFINE VARIABLE objProducer   AS Stomp.Producer NO-UNDO.
DEFINE VARIABLE objLogger     AS Stomp.Logger   NO-UNDO.

if search(FileName) = ? then RETURN.

FILE-INFORMATION:FILE-NAME = FileName.
FileSize = FILE-INFORMATION:FILE-SIZE.
if FileSize = 0 then RETURN.

COPY-LOB FROM FILE FileName to bigMessage NO-CONVERT.


objLogger   = NEW Stomp.Logger("log/producer.log", /* Log file name */
                                                3, /* Max logging entry level */
                                                5  /* Max logging error level */)
.
IF not VALID-OBJECT(objLogger) THEN RETURN "ERROR".
ASSIGN objProducer = NEW Stomp.Producer(TopicName, objLogger, "ErrorHandler", THIS-PROCEDURE).

/* Connect to ActiveMQ server */
IF NOT objProducer:connect(HostName, PortNumb, UserName, UserPass) THEN 
DO:
  RUN CleanUp.
  RETURN.
END.


CREATE ttHeaders.
ASSIGN ttHeaders.cHdrData[1] = "persistent"
       ttHeaders.cHdrData[2] = "true".
CREATE ttHeaders.
ASSIGN ttHeaders.cHdrData[1] = "FileName"
       ttHeaders.cHdrData[2] = FileName.
CREATE ttHeaders.
ASSIGN ttHeaders.cHdrData[1] = "FileSize"
       ttHeaders.cHdrData[2] = STRING(FileSize).

objProducer:sendFile(FileName, TABLE ttHeaders, "cHdrData").

FINALLY:
  RUN CleanUp.
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
  
  /* Always log the error info */
  objLogger:writeError(ipiErrorLevel, ipcError).

  /* Dump raw Frame data to log, if available */
  IF ipobjFrame NE ? THEN
    objLogger:dumpFrame(ipobjFrame).
  
  IF ipiErrorLevel LE 2 THEN DO:
    /* Quit program on severe error */
    IF ipiErrorLevel LE 1 THEN DO:
      /* Garbage collection */
      IF VALID-OBJECT(objProducer) THEN
        DELETE OBJECT objProducer.
      IF VALID-OBJECT(objLogger) THEN
        DELETE OBJECT objLogger.
    END. /* IF ipiErrorLevel LE 1 */
  END. /* IF ipiErrorLevel LE 2 */
  ELSE DO: /* ErrorLvl GE 3 */
      /* Do nothing - we will read log file if we are interested */
  END.
END PROCEDURE.

