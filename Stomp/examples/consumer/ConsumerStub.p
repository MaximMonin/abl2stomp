
/*------------------------------------------------------------------------
    File        : ConsumerStub.p
    Purpose     : Example stub file for an ActiveMQ consumer using the OO
                  Stomp framework.

    Author(s)   : Abe Voelker (http://abevoelker.com)
    Created     : Sat May 15 13:02:01 CDT 2010
    Notes       : 
  ----------------------------------------------------------------------*/

USING Progress.Lang.*.

DEFINE VARIABLE cAMQHost    AS CHARACTER      NO-UNDO.
DEFINE VARIABLE iAMQPort    AS INTEGER        NO-UNDO.
DEFINE VARIABLE cAMQLogin   AS CHARACTER      NO-UNDO.
DEFINE VARIABLE cAMQPass    AS CHARACTER      NO-UNDO.
DEFINE VARIABLE cQueue      AS CHARACTER      NO-UNDO.
DEFINE VARIABLE cSelector   AS CHARACTER      NO-UNDO.
DEFINE VARIABLE objConsumer AS Stomp.Consumer NO-UNDO.
DEFINE VARIABLE objLogger   AS Stomp.Logger   NO-UNDO.

/*------------------------------------------------------------------------------------------------------------------*/
/*                                                       MAIN                                                       */
/*------------------------------------------------------------------------------------------------------------------*/

CREATE WIDGET-POOL. /* So we can clean up dynamic memory easier at end of program */

/* Set up ActiveMQ variables: */
ASSIGN cAMQHost    = "10.10.4.9"
       iAMQPort    = 61613
       cAMQLogin   = "progress"
       cAMQPass    = "progress"
       cQueue      = ("/topic/" + "cwh.data") /* JMS will only see queue name (/queue/ is removed) */
       objLogger   = Stomp.LoggerMultiton:getLogger("/usr/pro/cwh/log/consumer.log", /* Log file name */
                                                                                3, /* Max logging entry level */
                                                                                5  /* Max logging error level */)
       NO-ERROR.

IF ERROR-STATUS:ERROR THEN DO:
    objLogger:writeError(1, "Error during setup variable assignment!  Error info: " + ERROR-STATUS:GET-MESSAGE(1)).
    RUN CleanUp.
    RETURN.
END.

/* Create the Consumer object */
ASSIGN objConsumer = NEW Stomp.Consumer(cQueue, objLogger, "MessageHandler", THIS-PROCEDURE, "ErrorHandler", THIS-PROCEDURE).

/* Connect to ActiveMQ server */
IF objConsumer:connect(cAMQHost, iAMQPort, cAMQLogin, cAMQPass, "cwh") THEN DO:
  /* Subscribe to queue */
  IF objConsumer:subscribe("activemq.subscriptionName|cwh", TRUE) THEN DO ON ERROR UNDO, LEAVE ON STOP UNDO, LEAVE ON QUIT UNDO, LEAVE:
    /* Wait forever at this point (Unix SIGTERM signal allows safe exit) */
    WAIT-FOR CLOSE OF THIS-PROCEDURE.
  END. /* IF objConsumer:subscribe(...) */
  ELSE DO:
    RUN CleanUp.
    RETURN.
  END.
  /* If we get here, then we were killed by a Unix signal */
  IF VALID-OBJECT(objLogger) THEN
    objLogger:writeEntry(1, "Received shutdown signal. Exiting gracefully...").
END. /* IF objConsumer:connect(...) */
ELSE DO:
  RUN CleanUp.
  RETURN.
END.

FINALLY:
  RUN CleanUp.
END.


/*------------------------------------------------------------------------------------------------------------------*/
/*                                                    PROCEDURES                                                    */
/*------------------------------------------------------------------------------------------------------------------*/

/* Clean up heap memory */
PROCEDURE CleanUp:
    IF VALID-OBJECT(objConsumer) THEN
      DELETE OBJECT objConsumer.
    IF VALID-OBJECT(objLogger) THEN
      DELETE OBJECT objLogger.
    DELETE WIDGET-POOL.
END PROCEDURE.


/* This procedure gets called by the Consumer each time a message is received from the queue */
PROCEDURE MessageHandler:
  DEFINE INPUT PARAMETER ipobjFrame AS Stomp.Frame NO-UNDO.

  DEFINE VARIABLE lcMessage AS LONGCHAR NO-UNDO.
  ASSIGN lcMessage = ipobjFrame:getMessageData(). /* Read in message data.  Typically, this is XML data */

  define variable filename as character.
  define variable filesize as integer.
  define variable filepart as integer.
  filename = ipobjFrame:getHeaderValue ("FileName").
  filesize = INTEGER(ipobjFrame:getHeaderValue ("FileSize")).
  filepart = INTEGER(ipobjFrame:getHeaderValue ("FilePart")).
  if filename = ? or filename = "" then
    filename = "log/" + ipobjFrame:getHeaderValue ("message-id").
  else
    filename = "log/" + filename.
  if FilePart = 0 then
  do:
    copy-lob lcMessage to file filename.
    if filesize <> 0 and filesize <> length (lcMessage) then
    do:
      objLogger:writeError(1, "FileSize = " + STRING(length (lcMessage)) + ", Expected = " + STRING(FileSize) ).
    end.
    objLogger:writeEntry(1, "Received file: " + FileName + ", Size (" + string(length(lcMessage)) + ")").
  end.
  else do:
    copy-lob lcMessage to file filename append.
    objLogger:writeEntry(1, "Received file: " + FileName + ", Total :" + string(filepart) +  ", Size (" + string(length(lcMessage)) + ")").
  end.
  
  /************ Handle message data here (if XML, probably use TEMP-TABLE or DATASET READ-XML method) ************/
  
  /* When finished with message data, acknowledge the Frame so it is removed from the queue.                 */
  /* If the Frame is not acknowledged it will be redelivered later.  Typically this happens on a re-connect. */
  IF NOT objConsumer:ackFrame(ipobjFrame) THEN DO:
      /* Could not acknowledge the Frame/Message! */
      RUN ErrorHandler(INPUT 2,
                       INPUT "Could not acknowledge Frame consumption! This Frame will reappear next connection!",
                       INPUT ipobjFrame).
  END.
  
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
  
  IF ipiErrorLevel LE 2 THEN
    /* Quit program on severe error */
    RUN CleanUp.
  ELSE DO: /* ErrorLvl GE 3 */
      /* Do nothing - we will read log file if we are interested */
  END.
END PROCEDURE.
