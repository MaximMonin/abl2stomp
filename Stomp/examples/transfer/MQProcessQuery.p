/* Check MQ server/read message/process it and returns answer */

USING Stomp.*.

define shared variable rid-ent as integer.
define shared stream servanswer.
define shared stream servlog.

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
define variable queue-list as character.
define variable CatalogFrom as character initial "".
define variable ServerFrom as character initial "".
define variable Q as character.
define variable r-ent as integer.
define variable rc as character.
define variable done as integer.
define variable dt as datetime-tz.

dt = NOW.

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
    if param-name = "Queue"  then queue-list = param-value.
    if param-name = "CatalogFrom"  then CatalogFrom = param-value.
    if param-name = "ServerFrom"   then ServerFrom = param-value.
  end.
end.

if MQServer = "" or queue-list = "" then RETURN.
if CatalogFrom <> "" then /* Каталог запуска скрипта не верен - копия */
do:
  FILE-INFORMATION:FILE-NAME = file-param.
  if REPLACE(FILE-INFO:FULL-PATHNAME,chr(92),'/') <> CatalogFrom + '/' + file-param then
    RETURN.
end.
if ServerFrom <> "" then /* Доступ с другого сервера - копия БД */
do:
  def variable comm as character.
  def variable comm-answer as character.
  comm = "ifconfig | grep " + ServerFrom.
  input through value (comm).
  import unformatted comm-answer.
  input close.
  if INDEX (comm-answer, ServerFrom) <= 0 then RETURN.
end.

/* Обрабатываем в паралель несколько очередей в течении 1 минуты, потом выходим */
repeat:
  done = 0.
  do i = 1 to NUM-ENTRIES(queue-list):
    Q = ENTRY(i,queue-list).
    if NUM-ENTRIES(Q, "_") > 1 then
    do:
      id-ent = ENTRY(2,Q,"_").  /* oblik101_400078 */
      find first ent where ent.id-ent = id-ent NO-LOCK NO-ERROR.
      if not available ent then NEXT.
      r-ent = ent.rid-ent.
    end.
    else
      r-ent = rid-ent.
  
    run ProcessQueue (Q).
    rc = RETURN-VALUE.
    if rc = "NO-MESSAGE" then NEXT.
    /* Если ошибка, то скорее всего она связана с проблемой связи с сервером, поэтому тоже прерываем обработку очередей */
    if rc = "ERROR" then RETURN. 
    if rc = "OK" then
    do:
      done = done + 1.
      if NOW - dt > 60 * 1000 then RETURN.
      NEXT.
    end.
  end.
  if done = 0 then RETURN.
  if NOW - dt > 60 * 1000 then RETURN.
end.

PROCEDURE ProcessQueue:
  define input parameter Q as character.

  if not (Q begins '/queue/' or Q begins '/topic/') then 
    Q = '/queue/' + Q.

  run Stomp/ReadMessage.p (MQServer,MQPort,MQLogin,MQPass,Q, "MessageHandler", THIS-PROCEDURE).
  RETURN RETURN-VALUE.
END.

PROCEDURE OblikVersion:
  define variable t1 as character.
  define variable t2 as character.
  if search ("db/version.dat") <> ? then
  do:
    INPUT FROM "db/version.dat".
    IMPORT delimiter "," t1 t2.
    INPUT CLOSE.
  end.
  RETURN t2.
END.

PROCEDURE MessageHandler:
  DEFINE INPUT PARAMETER ipobjFrame AS Stomp.Frame NO-UNDO.

  DEFINE VARIABLE lcMessage AS LONGCHAR NO-UNDO.
  define variable replyto as character.
  define variable QueryType as character.
  define variable QueryId as character.
  define variable DateFrom as date.
  define variable DateTo as date.
  define variable oblikvers as character.
  define variable filename as character.
  define variable rc as character.
  define variable oHeader as character.

  lcMessage = ipobjFrame:getMessageData(). 
  replyto   = ipobjFrame:getHeaderValue ("reply-to").
  QueryType = ipobjFrame:getHeaderValue ("QueryType").
  QueryId   = ipobjFrame:getHeaderValue ("QueryId").
  oHeader   = "QueryId|" + QueryId.

  run OblikVersion.
  oblikvers = RETURN-VALUE.

  if QueryType = "Accounts" then
  do:
    filename = "log/accdata.xml".
    if oblikvers begins "2.3" or oblikvers begins "2.1" then
      run src/transfer/export/accdata231.p (filename).
    else
      run src/transfer/export/accdata.p (filename).
    
    run Stomp/FileToMQ.p (filename, MQServer, MQPort, MQLogin, MQPass, replyto, oHeader).
    rc = RETURN-VALUE.
    OS-DELETE VALUE(filename).
    RETURN rc.
  end.
  if QueryType = "Anobject" then
  do:
    filename = "log/anobjdata.xml".
    if oblikvers begins "2.3" or oblikvers begins "2.1" then
      run src/transfer/export/anobjdata231.p (filename).
    else
      run src/transfer/export/anobjdata.p (filename).
    
    run Stomp/FileToMQ.p (filename, MQServer, MQPort, MQLogin, MQPass, replyto, oHeader).
    rc = RETURN-VALUE.
    OS-DELETE VALUE(filename).
    RETURN rc.
  end.
  if QueryType = "Clients" then
  do:
    filename = "log/clientdata.xml".
    run src/transfer/export/clientdata.p (filename).
    
    run Stomp/FileToMQ.p (filename, MQServer, MQPort, MQLogin, MQPass, replyto, oHeader).
    rc = RETURN-VALUE.
    OS-DELETE VALUE(filename).
    RETURN rc.
  end.
  if QueryType = "Wares" then
  do:
    filename = "log/waredata.xml".
    run src/transfer/export/waredata.p (filename).
    
    run Stomp/FileToMQ.p (filename, MQServer, MQPort, MQLogin, MQPass, replyto, oHeader).
    rc = RETURN-VALUE.
    OS-DELETE VALUE(filename).
    RETURN rc.
  end.
  if QueryType = "Employeers" then
  do:
    filename = "log/empdata.xml".
    run src/transfer/export/empdata.p (filename).
    
    run Stomp/FileToMQ.p (filename, MQServer, MQPort, MQLogin, MQPass, replyto, oHeader).
    rc = RETURN-VALUE.
    OS-DELETE VALUE(filename).
    RETURN rc.
  end.
  if QueryType = "Rest" then
  do:
    filename = "log/rest.xml".
    DateTo   = DATE(ipobjFrame:getHeaderValue ("DateTo")) NO-ERROR.
    if oblikvers begins "2.1" then
      run src/transfer/export/rest21.p (filename, r-ent, DateTo).
    else
      run src/transfer/export/rest.p (filename, r-ent, DateTo).
    
    oHeader = oHeader + "|" + "DateTo|"   + STRING(DateTo  ,"99/99/9999").
    run Stomp/FileToMQ.p (filename, MQServer, MQPort, MQLogin, MQPass, replyto, oHeader).
    rc = RETURN-VALUE.
    OS-DELETE VALUE(filename).
    RETURN rc.
  end.
  if QueryType = "Operat" then
  do:
    filename = "log/operat.xml".
    DateFrom = DATE(ipobjFrame:getHeaderValue ("DateFrom")) NO-ERROR.
    DateTo   = DATE(ipobjFrame:getHeaderValue ("DateTo")) NO-ERROR.
    if oblikvers begins "2.1" then
      run src/transfer/export/operat21.p (filename, r-ent, DateFrom, DateTo).
    else
      run src/transfer/export/operat.p (filename, r-ent, DateFrom, DateTo).
    
    oHeader = oHeader + "|" + "DateFrom|" + STRING(DateFrom,"99/99/9999").
    oHeader = oHeader + "|" + "DateTo|"   + STRING(DateTo  ,"99/99/9999").
    run Stomp/FileToMQ.p (filename, MQServer, MQPort, MQLogin, MQPass, replyto, oHeader).
    rc = RETURN-VALUE.
    OS-DELETE VALUE(filename).
    RETURN rc.
  end.
  if QueryType = "OperatUpdate" then
  do:
    filename = "log/operatupd.xml".
    DateFrom = DATE(ipobjFrame:getHeaderValue ("DateFrom")) NO-ERROR.
    DateTo   = DATE(ipobjFrame:getHeaderValue ("DateTo")) NO-ERROR.
    if oblikvers begins "2.1" then
      run src/transfer/export/operat_upd21.p (filename, r-ent, DateFrom, DateTo).
    else
      run src/transfer/export/operat_upd.p (filename, r-ent, DateFrom, DateTo).
    
    oHeader = oHeader + "|" + "DateFrom|" + STRING(DateFrom,"99/99/9999").
    oHeader = oHeader + "|" + "DateTo|"   + STRING(DateTo  ,"99/99/9999").
    run Stomp/FileToMQ.p (filename, MQServer, MQPort, MQLogin, MQPass, replyto, oHeader).
    rc = RETURN-VALUE.
    OS-DELETE VALUE(filename).
    RETURN rc.
  end.
  if QueryType = "AppservQuery" then
  do: 
    define variable AppQueryType as character.
    define variable QueryParams as character.
    define variable queryfilename as character.
    define variable answerfilename as character.

    filename = "log/MQappserv.txt".
    answerfilename = "log/MQappserv_answer.txt".
    queryfilename = "log/MQappserv_query.txt".
    output stream servanswer to value(filename).
    output stream servlog to value("log/appserv.log") unbuffered append keep-messages.

    COPY-LOB from lcMessage to file queryfilename.
    input from value(queryfilename).

    run appserv/getquery.p (output AppQueryType, output QueryParams).
    put stream servlog unformatted "servmain " QueryType QueryParams  skip.
      
    case AppQueryType :
      WHEN "ReportDoc" OR WHEN "ОтчетДокумент" then
      do:
        run appserv/query/docquery.p ( QueryParams ). 
      end.
      WHEN "Module" then
      do:
        run appserv/query/modquery.p ( QueryParams ). 
      end.
      WHEN "СоздатьДокументы" then
      do:
        run appserv/query/cr_docs.p ( QueryParams ). 
      end.
      WHEN "Репликация" then
      do:
        run appserv/query/replicat.p ( QueryParams ). 
      end.
      WHEN "Обмен данными" then
      do:
        run appserv/query/exchange.p ( QueryParams ). 
      end.
      OTHERWISE 
      do:
        put stream servlog unformatted "Неизвестный тип запроса " AppQueryType skip.
        put stream servanswer unformatted "Неизвестный тип запроса " AppQueryType skip skip.
      end.
    end case.
    input close.
    OS-DELETE VALUE(queryfilename).
    output stream servanswer close.
    output stream servlog close.

    define variable aline as character.
    input from value(filename).
    import unformatted aline.
    if aline = "Запрос принят" then
    do:
      output to value (answerfilename) convert target ("utf-8").
      repeat:
        import unformatted aline.
        put unformatted aline skip.
      end.
      input close.
      output close.
      OS-DELETE VALUE(filename).

      oHeader = oHeader + "|" + "ReturnValue|OK".
      run Stomp/FileToMQ.p (answerfilename, MQServer, MQPort, MQLogin, MQPass, replyto, oHeader).
      rc = RETURN-VALUE.
      OS-DELETE VALUE(answerfilename).
      RETURN rc.
    end.
    else
    do:
      input close.
      input from value(filename).
      output to value (answerfilename) convert target ("utf-8").
      repeat:
        import unformatted aline.
        put unformatted aline skip.
      end.
      input close.
      output close.
      OS-DELETE VALUE(filename).

      oHeader = oHeader + "|" + "ReturnValue|ERROR".
      run Stomp/FileToMQ.p (answerfilename, MQServer, MQPort, MQLogin, MQPass, replyto, oHeader).
      rc = RETURN-VALUE.
      OS-DELETE VALUE(answerfilename).
      RETURN rc.
    end.
  end.

  RETURN "OK".
END.
