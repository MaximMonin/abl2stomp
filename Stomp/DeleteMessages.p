USING Progress.Lang.*.
USING Stomp.*.

ROUTINE-LEVEL ON ERROR UNDO, THROW.

define input parameter HostName as character.
define input parameter PortNumb as integer.
define input parameter UserName as character.
define input parameter UserPass as character.
define input parameter QueueName as character.

define variable okflag as logical.
DEFINE VARIABLE objConsumer   AS Stomp.Consumer NO-UNDO.
DEFINE VARIABLE objLogger     AS Stomp.Logger   NO-UNDO.
define variable ReadMessage   as logical.

ReadMessage = False.
objLogger   = NEW Stomp.Logger("log/consumer.log", /* Log file name */
                                                2, /* Max logging entry level */
                                                2  /* Max logging error level */)
.
IF not VALID-OBJECT(objLogger) THEN RETURN "ERROR".
ASSIGN objConsumer = NEW Stomp.Consumer(QueueName, objLogger, "MessageHandler", THIS-PROCEDURE, "ErrorHandler", THIS-PROCEDURE).

IF NOT objConsumer:connect(HostName, PortNumb, UserName, UserPass) THEN 
DO:
  RUN CleanUp.
  RETURN "ERROR".
END.
IF objConsumer:subscribe(TRUE) THEN DO ON ERROR UNDO, LEAVE ON STOP UNDO, LEAVE ON QUIT UNDO, LEAVE:
   if ReadMessage = false then
     objConsumer:waitForSocket(2). /* Wait for message during 2 seconds, if available */
   if ReadMessage = true then
   do:
     repeat:
       ReadMessage = false.
       objConsumer:waitForSocket(2). /* Wait for message during 2 seconds, if available */
       if ReadMessage = false then leave.
     end.
   end.
   RUN CleanUp.
   RETURN "OK".
END.

RUN CleanUp.
RETURN "ERROR".


PROCEDURE CleanUp:
    IF VALID-OBJECT(objConsumer) THEN
      DELETE OBJECT objConsumer.
    IF VALID-OBJECT(objLogger) THEN
      DELETE OBJECT objLogger.
    DELETE WIDGET-POOL.
END PROCEDURE.

PROCEDURE MessageHandler:
  DEFINE INPUT PARAMETER ipobjFrame AS Stomp.Frame NO-UNDO.

  ReadMessage = True.

  IF NOT objConsumer:ackFrame(ipobjFrame) THEN DO:
      RUN ErrorHandler(INPUT 2,
                       INPUT "Could not acknowledge Frame consumption! This Frame will reappear next connection!",
                       INPUT ipobjFrame).
  END.
END PROCEDURE.


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
      IF VALID-OBJECT(objConsumer) THEN
        DELETE OBJECT objConsumer.
      IF VALID-OBJECT(objLogger) THEN
        DELETE OBJECT objLogger.
    END. /* IF ipiErrorLevel LE 1 */
  END. /* IF ipiErrorLevel LE 2 */
  ELSE DO: /* ErrorLvl GE 3 */
      /* Do nothing - we will read log file if we are interested */
  END.
END PROCEDURE.
