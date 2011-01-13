/* Background MQ listener, reads messages, and links incoming files to tasks */

USING Stomp.*.

define shared variable uid as character.

define variable file-param as character initial "src/transfer/MQ.cfg".
define variable param-line as character.
define variable param-name as character.
define variable param-value as character.
define variable i as integer.
define variable id-ent as character.

define variable MQServer as character.
define variable MQPort as integer initial 61613.
define variable MQLogin as character initial "progress".
define variable MQPass as character initial "progress".
define variable MQTopic as character.

if search (file-param) <> ? then
do:
  input from value (file-param).
  repeat:
    import unformatted param-line.
    if substring(param-line, 1, 1) = "#" then NEXT.
    if NUM-ENTRIES(param-line, "=") < 2 then NEXT.

    param-name  = TRIM(ENTRY (1,param-line, "=")).
    param-value = TRIM(ENTRY (2,param-line, "=")).
    if param-name = "Server" then MQServer = param-value.
    if param-name = "Port"   then MQPort = INTEGER(param-value) NO-ERROR.
    if param-name = "Login"  then MQLogin = param-value.
    if param-name = "Pass"   then MQPass = param-value.
    if param-name = "Queue"  then MQTopic = param-value.
  end.
end.

define variable okflag as logical.
DEFINE VARIABLE objConsumer   AS Stomp.Consumer NO-UNDO.
DEFINE VARIABLE objLogger     AS Stomp.Logger   NO-UNDO.
define variable cwhConnected  as logical        NO-UNDO.
define variable quitflag      as logical        NO-UNDO.

objLogger   = NEW Stomp.Logger("log/consumer.log", /* Log file name */
                                                0, /* Max logging entry level */
                                                0  /* Max logging error level */).
IF not VALID-OBJECT(objLogger) THEN RETURN "ERROR".

ASSIGN objConsumer = NEW Stomp.Consumer(MQTopic, objLogger, "MessageHandler", THIS-PROCEDURE, "ErrorHandler", THIS-PROCEDURE).
IF not VALID-OBJECT(objConsumer) THEN RETURN "ERROR".

/* Connect to ActiveMQ server */
cwhConnected = false.
quitflag = false.

IF objConsumer:connect(MQServer, MQPort, MQLogin, MQPass, "cwh") THEN 
DO:
  /* Subscribe to topic */
  if valid-object (objLogger) then
    objLogger:ChangeLevel (2,2).

  IF objConsumer:subscribe("activemq.subscriptionName|cwh", TRUE) THEN 
  DO ON ERROR UNDO, LEAVE ON STOP UNDO, LEAVE ON QUIT UNDO, LEAVE:
    define variable n-run-core as integer.
    n-run-core = 0.
    repeat:
      if quitflag then leave.
      if not valid-object (objConsumer) then leave.
      if not valid-object (objLogger) then leave.
      WAIT-FOR CLOSE OF THIS-PROCEDURE pause 60.
      n-run-core = n-run-core + 1.
      run src/kernel/bgcore.p (n-run-core, uid).
      if RETURN-VALUE = "STOP" then leave.
    end.
    RUN CleanUp.
  END. /* IF objConsumer:subscribe(...) */
  ELSE DO:
    RUN CleanUp.
    RETURN.
  END.
  IF VALID-OBJECT(objLogger) THEN
    objLogger:writeEntry(1, "Received shutdown signal. Exiting gracefully...").
END. /* IF objConsumer:connect(...) */
ELSE DO:
  if cwhConnected = false and valid-object (objLogger) then
  do:
     objLogger:ChangeLevel (2,2).
     objLogger:writeError(1, "Connection " + MQServer + ":" + STRING(MQPort) + ": STOMP connect refused by server or connection timeout").
  end.

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

  /************ Handle message data here (if XML, probably use TEMP-TABLE or DATASET READ-XML method) ************/

  DEFINE VARIABLE lcMessage AS LONGCHAR NO-UNDO.
  ASSIGN lcMessage = ipobjFrame:getMessageData(). /* Read in message data.  Typically, this is XML data */

  define variable filename as character.
  define variable QueryID as character.
  define variable filesize as integer.
  define variable realfilesize as integer.
  define variable filepart as integer.
  define variable filepartnumb as integer.
  define variable Err as character.
  define variable Zipped as logical.
  define variable isBinary as logical.
  define variable md5 as character.

  QueryId  = ipobjFrame:getHeaderValue ("QueryId").
  if QueryId = ? then QueryId = "".
  filename = ipobjFrame:getHeaderValue ("FileName").
  if filename = ? then filename = "".
  filesize = INTEGER(ipobjFrame:getHeaderValue ("FileSize")).
  if filesize = ? then filesize = 0.
  realfilesize = INTEGER(ipobjFrame:getHeaderValue ("RealFileSize")).
  if realfilesize = ? then realfilesize = 0.
  filepart = INTEGER(ipobjFrame:getHeaderValue ("FilePart")).
  if filepart = ? then filepart = 0.
  filepartnumb = INTEGER(ipobjFrame:getHeaderValue ("FilePartNumb")).
  if filepartnumb = ? then filepartnumb = 0.
  zipped = LOGICAL(ipobjFrame:getHeaderValue ("Zip")).
  if zipped = ? then zipped = false.
  isBinary = LOGICAL(ipobjFrame:getHeaderValue ("Binary")).
  if isBinary = ? then isBinary = false.
  md5 = ipobjFrame:getHeaderValue ("MD5").
  if md5 = ? then md5 = "".

  if filename = "" or QueryId = "" then
    filename = "imp/" + ipobjFrame:getHeaderValue ("message-id").
  else do:
    if INDEX (filename, "/") > 0 then
      filename = SUBSTRING(filename,R-INDEX(filename, "/") + 1).
    filename = "imp/" + QueryId + "-" + filename.
  end.
  if FilePart = 0 then
  do:
    copy-lob lcMessage to file filename.
    run ConvertFile (filename, zipped, isBinary).
    if md5 <> "" then
    do:
      run CheckMd5 (filename, md5).
      if RETURN-VALUE = "ERROR" then
      do:
        objLogger:writeError(1, "Md5 sums for source and recieved file are different" ).
        Err = "File integrity error".
      end.
    end.
    if realfilesize <> 0 then
    do:
      if realfilesize <> length (lcMessage) then
      do:
        objLogger:writeError(1, "FileSize = " + STRING(length (lcMessage)) + ", Expected = " + STRING(realFileSize) ).
        Err = "FileSize = " + STRING(length (lcMessage)) + ", Expected = " + STRING(realFileSize).
      end.
    end.
    else do:
      if filesize <> 0 and filesize <> length (lcMessage) then
      do:
        objLogger:writeError(1, "FileSize = " + STRING(length (lcMessage)) + ", Expected = " + STRING(FileSize) ).
        Err = "FileSize = " + STRING(length (lcMessage)) + ", Expected = " + STRING(FileSize).
      end.
    end.
    objLogger:writeEntry(1, "Received file: " + FileName + ", Size (" + string(length(lcMessage)) + ")").
    run RegisterFile (QueryId, filename, Err).
  end.
  else do:
    if filepartnumb = 1 then
      copy-lob lcMessage to file filename.
    else
      copy-lob lcMessage to file filename append.
    objLogger:writeEntry(1, "Received file: " + FileName + ", Total :" + string(filepart) +  ", Size (" + string(length(lcMessage)) + ")").
    if filepart = realfilesize or (realfilesize = 0 and filepart = filesize) then
    do:
      run ConvertFile (filename, zipped, isBinary).
      if md5 <> "" then
      do:
        run CheckMd5 (filename, md5).
        if RETURN-VALUE = "ERROR" then
        do:
          objLogger:writeError(1, "Md5 sums for source and recieved file are different" ).
          Err = "File integrity error".
        end.
      end.
      FILE-INFORMATION:FILE-NAME = FileName.
      if FileSize <> FILE-INFORMATION:FILE-SIZE then
      do:
        objLogger:writeError(1, "FileSize = " + STRING(FILE-INFORMATION:FILE-SIZE) + ", Expected = " + STRING(FileSize) ).
        Err = "FileSize = " + STRING(FILE-INFORMATION:FILE-SIZE) + ", Expected = " + STRING(FileSize).
      end.
      run RegisterFile (QueryId, filename, Err).
    end.
  end.
  
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

  cwhConnected = true. /* there is another Listener process that subscribed as cwh */

  if valid-object (objLogger) then
    objLogger:writeError(ipiErrorLevel, ipcError).

  /* Dump raw Frame data to log, if available */
  IF ipobjFrame NE ? THEN
    if valid-object (objLogger) then
      objLogger:dumpErrorFrame(ipobjFrame, ipiErrorLevel).

  IF ipiErrorLevel LE 2 THEN
  do:
    /* Quit program on severe error */
    if valid-object (objLogger) then
      objLogger:writeEntry(1, "Exiting gracefully...").
    quitflag = true.
    RUN CleanUp.
  end.
  ELSE DO: /* ErrorLvl GE 3 */
      /* Do nothing - we will read log file if we are interested */
  END.
END PROCEDURE.

/* uudecode + unzip incoming file under Linux enviroment */
PROCEDURE ConvertFile:
  define input parameter filename as character.
  define input parameter zipped as logical.
  define input parameter isbinary as logical.

  define variable realfilename as character.
  define variable realfilename2 as character.

  if not (zipped = true or isbinary = true) then RETURN.

  realfilename = filename.
  if zipped then
    realfilename = realfilename + ".gz".
  if isBinary then
  do:
    realfilename2 = realfilename.
    realfilename = realfilename + ".uu".
  end.
  OS-RENAME VALUE (filename) VALUE (realfilename).
  if isBinary then
  do:
    os-command silent value("uudecode -o " + Realfilename2 + " " + RealFileName ).
    OS-DELETE VALUE(RealFileName).
    RealFileName = RealFileName2.
  end.
  if zipped then
  do:
    os-command silent value("gzip -d " + Realfilename ).
  end.
END.

/* Check md5sum for file */
PROCEDURE CheckMd5:
  define input parameter filename as character.
  define input parameter md5 as character.

  define variable md5value as character.

  input through value ("md5sum " + filename).
  import md5value.
  input close.
  if md5value <> md5 then RETURN "ERROR".
  RETURN "OK".
end.


PROCEDURE RegisterFile:
  define input parameter queryId as character.
  define input parameter filename as character.
  define input parameter Err as character.

  define variable rid-task as integer.
  define variable rid-doc as integer.

  run src/kernel/rmbufadoc.p.

  rid-task = INTEGER(queryId) NO-ERROR.
  if rid-task = ? then RETURN.
  run src/om/tsk2doc.p ( rid-task, OUTPUT rid-doc).
  if rid-doc = ? then RETURN. /* Task deleted */

  /* change document data and change task status */
  run src/kernel/get_ffv.p ( "Status", rid-doc ).
  if RETURN-VALUE begins "0" or RETURN-VALUE begins "1" then
  do transaction:
    run src/kernel/set_ffv.p ( "Status", rid-doc, "2. Получен файл" ).
    run src/kernel/set_ffv.p ( "File", rid-doc, filename ).
    run src/kernel/set_ffv.p ( "Error", rid-doc, Err ).
    objLogger:writeEntry(1, "-----> Attached file: " + FileName + " to QueryId = " + queryId).  

    find first document where document.rid-document = rid-doc EXCLUSIVE-LOCK NO-WAIT NO-ERROR.
    if available document then
    do:
      run src/kernel/sendevnt.p ( rid-doc, 3). /* Recalc document and update task status */
      release document.
    end.
  end.
END.
