USING Progress.Lang.*.
USING Stomp.*.

/*
run src/transfer/MQRunModuleWithAnswer.p
("oblik04,oblik05","","imp/admin/typedoc.p","",
"/queue/testanswer","testID", "FileHandler", THIS-PROCEDURE, 600).

procedure FileHandler:
  define input parameter qname as character.
  define input parameter rv as character.
  define input parameter filename as character.
  define input parameter errstr as character.

  message qname rv filename errstr view-as alert-box.
end.
*/

define input parameter qname-list as character.   /* Queue list */
define input parameter PacketFile as character.   /* tar-packet name to transfer and extract on remote-host*/
define input parameter ModuleFile as character.   /* module name to run on remote host */
define input parameter ModuleParams as character. /* module input parameters */
define input parameter replyto as character.      /* listen answer here  */
define input parameter QueryId as character.      /* Unique key for module run for answer selection */
define input parameter ProcName AS CHARACTER.     /* Handler to process incoming files and return values from remote hosts */
define input parameter ProcHandle AS HANDLE.      /* Procedure handle */
define input parameter WaitTime as integer.       /* Wait answers during WaitTime seconds */

define variable i as integer.
define variable rc as character.
define variable HeaderInfo as character.

define temp-table Tasks NO-UNDO
  field qname as character
  field msent as logical
  field manswer as logical
  field rv as character
  field filename as character
  field errorstr as character
  index i0 qname.

/* Phase 1. Sending tasks to run modules */

define variable file-param as character initial "src/transfer/MQ.cfg".
define variable param-line as character.
define variable param-name as character.
define variable param-value as character.

define variable MQServer as character.
define variable MQPort as integer initial 61613.
define variable MQLogin as character initial "progress".
define variable MQPass as character initial "progress".

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
  end.
end.

do i = 1 to NUM-ENTRIES (qname-list):

  create Tasks.
  Tasks.qname = ENTRY(i,qname-list).

  HeaderInfo = "persistent|true|QueryType|runModule|binary|true|zip|true|Listener|true".
  if QueryId = "" then QueryId = "0".
  HeaderInfo = HeaderInfo + "|reply-to|" + replyto + "|QueryId|" + QueryId + "_" + Tasks.qname.
  if ModuleFile <> "" then
    HeaderInfo = HeaderInfo + "|ModuleName|" + ModuleFile + "|ModuleParams|" + ModuleParams.

  run SendQuery (Tasks.qname, PacketFile, HeaderInfo).
  rc = RETURN-VALUE.
  if rc <> "OK" then RETURN rc.  /* Mq сервер не доступен */
  Tasks.msent = yes.
end.

/* Phase 2. Create Subscription with selector "QueryId LIKE '" + QueryId + "_%'" */


DEFINE VARIABLE objConsumer   AS Stomp.Consumer NO-UNDO.
DEFINE VARIABLE objLogger     AS Stomp.Logger   NO-UNDO.

objLogger   = NEW Stomp.Logger("log/consumer.log", /* Log file name */
                                                2, /* Max logging entry level */
                                                2  /* Max logging error level */)
.
IF not VALID-OBJECT(objLogger) THEN RETURN "ERROR".
ASSIGN objConsumer = NEW Stomp.Consumer(replyTo, objLogger, "MessageHandler", THIS-PROCEDURE, "ErrorHandler", THIS-PROCEDURE).

IF NOT objConsumer:connect(MQServer,MQPort,MQLogin,MQPass) THEN 
DO:
  RUN CleanUp.
  RETURN "ERROR".
END.
IF not objConsumer:subscribe(TRUE, "QueryId LIKE '" + QueryId + "_%'") THEN
DO:
  RUN CleanUp.
  RETURN "ERROR".
END.

define variable t1 as datetime-tz.
t1 = now.
DO ON ERROR UNDO, LEAVE ON STOP UNDO, LEAVE ON QUIT UNDO, LEAVE:
  repeat:
    i = 0.
    for each Tasks:
      if tasks.manswer = false then
      do:
        i = i + 1.
        objConsumer:waitForSocket(1). /* Wait for message during 1 seconds, if available */
      end.
    end.
    if i = 0 then
    do:
      RUN CleanUp.
      RETURN "OK".
    end.
    if (now - t1) / 1000 > WaitTime then
    do:
      RUN CleanUp.
      RETURN "no-answer".
    end.
  end.
END.

RUN CleanUp.
RETURN "ERROR".

PROCEDURE SendQuery:
  define input parameter qname as character.
  define input parameter filename as character.
  define input parameter HeaderInfo as character.

  if not (qname begins '/queue/' or qname begins '/topic/') then 
    qname = '/queue/' + qname.

  run Stomp/FileToMQ.p (filename,MQServer,MQPort,MQLogin,MQPass,qname,HeaderInfo).
  RETURN RETURN-VALUE.
END.


PROCEDURE CleanUp:
    IF VALID-OBJECT(objConsumer) THEN
      DELETE OBJECT objConsumer.
    IF VALID-OBJECT(objLogger) THEN
      DELETE OBJECT objLogger.
    DELETE WIDGET-POOL.
END PROCEDURE.

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
  define variable ReturnValue as character.

  QueryId  = ipobjFrame:getHeaderValue ("QueryId").
  if QueryId = ? then QueryId = "".
  ReturnValue  = ipobjFrame:getHeaderValue ("ReturnValue").
  if ReturnValue = ? then ReturnValue = "".
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

  if filename = "" and QueryId = "" then
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
    run RegisterFile (QueryId, filename, Err, ReturnValue).
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
      run RegisterFile (QueryId, filename, Err, ReturnValue).
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
  define input parameter ReturnValue as character.

  objLogger:writeEntry(1, "-----> Recieved file: " + FileName + " to QueryId = " + queryId
     + " Error:" + Err + " ReturnValue:" + ReturnValue).  

  FILE-INFORMATION:FILE-NAME = filename.
  if FILE-INFORMATION:FILE-SIZE = 0 then
  do:
    objLogger:writeEntry(1, "-----> Recieved empty file: " + FileName + " to QueryId = " + queryId).  
    OS-DELETE value (filename).
    filename = "".
  end.

  define variable qname as character.

  qname = ENTRY (NUM-ENTRIES(queryid,"_"),queryid, "_").
  find first tasks where tasks.qname = qname no-error.
  if available tasks then
  do:
    tasks.manswer = yes.
    tasks.rv = ReturnValue.
    tasks.filename = filename.
    tasks.errorstr = err.
    IF VALID-HANDLE(ProcHandle) THEN
    do:
      RUN VALUE(ProcName) IN ProcHandle (qname, ReturnValue, filename, err).
    end.
  end.
END PROCEDURE.


PROCEDURE ErrorHandler:
  DEFINE INPUT PARAMETER ipiErrorLevel AS INTEGER        NO-UNDO.
  DEFINE INPUT PARAMETER ipcError      AS CHARACTER      NO-UNDO.
  DEFINE INPUT PARAMETER ipobjFrame    AS Stomp.Frame    NO-UNDO.
  
  /* Level 1:    Error:   System shutdown probably best course of action         */
  /* Level 2:  Warning:   System stability may be threatened; should advise user */
  /* Level 3+: Verbose:   Most probably safe to ignore                           */
  
  /* Always log the error info */
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
