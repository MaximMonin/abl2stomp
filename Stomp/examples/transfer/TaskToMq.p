USING Stomp.*.

define input parameter rid-task as integer.

define variable QueryId as character.
define variable QueryType as character.
define variable DateFrom as date.
define variable DateTo as date.
define variable rid-doc as integer.
define variable rid-ent as integer.
define variable HeaderInfo as character.
define variable qname as character.
define variable Err as character.

QueryId = STRING (rid-task).

run src/om/tsk2doc.p ( rid-task, OUTPUT rid-doc).
if rid-doc = ? then RETURN "ERROR". /* Task deleted */

do transaction:
  find first om-task where om-task.rid-task = rid-task EXCLUSIVE-LOCK NO-WAIT NO-ERROR.
  if not available om-task then RETURN "ERROR". /* Another Process do this task */
  if om-task.curr-status <> 1 /* В работе */ then RETURN "ERROR". /* Задача не в работе */

  find first om-typeoper of om-task NO-LOCK NO-ERROR.
  if not available om-typeoper then RETURN "ERROR".

  HeaderInfo = "persistent|true|reply-to|/topic/cwh.data|QueryId|" + QueryId.
  run src/kernel/get_ffv.p ( "1:3", rid-doc ).
  rid-ent = INTEGER(RETURN-VALUE).
  run src/kernel/get_ffv.p ( "1:5", rid-doc ).    
  DateFrom = Date (RETURN-VALUE).
  run src/kernel/get_ffv.p ( "1:6", rid-doc ).
  DateTo = Date (RETURN-VALUE).

  run src/kernel/get_ffv.p ( "Status", rid-doc ).
  if not (RETURN-VALUE BEGINS "0") then 
  do:
    run src/kernel/sendevnt.p ( rid-doc, 3). /* Recalc document and update task status */
    RETURN "ERROR". /* Wrong status to exucute this phase. */
  end.

  find first ent where ent.rid-ent = rid-ent NO-LOCK NO-ERROR.
  if not available ent then 
    Err = "Не задано предприятие".
  else 
    qname = ent.id-ent.


  if om-typeoper.id-oper = 2 then
  do:
    run src/kernel/get_ffs.p ( "1:4", rid-doc ).
    QueryType = RETURN-VALUE.
    if QueryType = "" then
      Err = "Не задан тип задания".
    else do:
      if QueryType = "Аналитика" then
      do:
        QueryType = "Anobject".
      end.
      if QueryType = "Планы счетов" then
      do:
        QueryType = "Accounts".
      end.
      if QueryType = "Клиенты" then
      do:
        QueryType = "Clients".
      end.
      if QueryType = "Товары" then
      do:
        QueryType = "Wares".
      end.
      if QueryType = "Сотрудники" then
      do:
        QueryType = "Employeers".
      end.
      if QueryType = "Остатки по счетам" then
      do:
        QueryType = "Rest".
        if DateTo = ? then
        do:
          if DateFrom = ? then
            Err = "Не указана дата переноса остатков".
          else
            DateTo = DateFrom.
        end.
        QueryType = QueryType + "|" + "DateTo|" + STRING(DateTo,"99/99/9999").
      end.
      if QueryType = "Проводки" then
      do:
        QueryType = "Operat".
        if DateFrom = ? or DateTo = ? then
          Err = "Не задан диапазон дат для переноса проводок".
        else do:
          if DateFrom > DateTo then
            Err = "Неправильный диапазон дат".
          else do:
            QueryType = QueryType + "|" + "DateFrom|" + STRING(DateFrom,"99/99/9999").
            QueryType = QueryType + "|" + "DateTo|" + STRING(DateTo,"99/99/9999").
          end.
        end.
      end.
    end.
  end.
  if om-typeoper.id-oper = 3 then
  do:
    run src/kernel/get_ffs.p ( "1:4", rid-doc ).
    QueryType = RETURN-VALUE.
    if QueryType = "Оборотка" then
    do:
      QueryType = "src/custom/appserv/rest_c.p".
      if DateFrom = ? or DateTo = ? then
        Err = "Не задан диапазон дат для переноса проводок".
      else do:
        if DateFrom > DateTo then
          Err = "Неправильный диапазон дат".
        else do:
          QueryType = QueryType + "|" + STRING(DateFrom,"99/99/9999") + "|" + STRING(DateTo, "99/99/9999") + "||yes|0|".
          define variable plan-str as character.
          plan-str = "".
          for each acc-plan where acc-plan.rid-ent = rid-ent NO-LOCK:
            if plan-str = "" then
              plan-str = STRING(acc-plan.plan).
            else
              plan-str = plan-str + "," + STRING(acc-plan.plan).
          end.
          QueryType = QueryType + plan-str.
        end.
      end.
    end.
    if QueryType = "Динамика по счету" then
    do:
      QueryType = "src/custom/appserv/turnover_c.p".
      if DateFrom = ? or DateTo = ? then
        Err = "Не задан диапазон дат для переноса проводок".
      else 
      do:
        if DateFrom > DateTo then
          Err = "Неправильный диапазон дат".
        else 
        do:
          define variable count as character.
          define variable i as integer.
          define variable rows as integer.

          run src/kernel/get_tr.p (2, rid-doc, output rows).
          do i = 1 to rows :
            run src/kernel/get_ftv.p ("2:8", rid-doc, i).
            if return-value = "yes" then NEXT.
            run src/kernel/get_ftv.p ("2:1", rid-doc, i).
            count = RETURN-VALUE.
            leave.
          end.
          if count = "" then
            Err = "Не заданы счета для динамики оборотов".
          else do:
            run src/kernel/set_ffv.p ( "1:10", rid-doc, count ).
            QueryType = QueryType + "|" + STRING(DateFrom,"99/99/9999") + "|" + STRING(DateTo, "99/99/9999").
            QueryType = QueryType + "|" + count + "|||1|1|?|?|0".
          end.
        end.
      end.
    end.
  end.

  if Err <> "" then
  do:
    run src/kernel/set_ffv.p ( "Error", rid-doc, Err ).
    run src/kernel/sendevnt.p ( rid-doc, 3). /* Recalc document and update task status */
    RETURN "ERROR".
  end.
  else do:
    run src/kernel/set_ffv.p ( "Error", rid-doc, "" ).
    if om-typeoper.id-oper = 2 then
    do:
      HeaderInfo = HeaderInfo + "|QueryType|" + QueryType.
      run SendQuery ( qname, HeaderInfo).
    end.
    if om-typeoper.id-oper = 3 then
    do:
      run src/transfer/AppservQueryToMQ.p ( qname, "/topic/cwh.data", QueryId, "Module", QueryType, "").
    end.
    if Return-value = "OK" then
    do:
      run src/kernel/set_ffv.p ( "Status", rid-doc, "1. Передано в MQ" ).
      run src/kernel/sendevnt.p ( rid-doc, 3). /* Recalc document and update task status */
      RETURN "OK".
    end.
    run src/kernel/sendevnt.p ( rid-doc, 3). /* Recalc document and update task status */
    RETURN "ERROR".
  end.
end.


PROCEDURE SendQuery:
  define input parameter qname as character.
  define input parameter HeaderInfo as character.

  define variable file-param as character initial "src/transfer/MQ.cfg".
  define variable param-line as character.
  define variable param-name as character.
  define variable param-value as character.
  define variable i as integer.

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
  if not (qname begins '/queue/' or qname begins '/topic/') then 
    qname = '/queue/' + qname.

  run Stomp/SendMessage.p ("",MQServer,MQPort,MQLogin,MQPass,qname,HeaderInfo).
  RETURN RETURN-VALUE.
END.
