define input parameter qname as character.
define input parameter ReplyTo as character.
define input parameter QueryId as character.
define input parameter query-type as character.
define input parameter query-params as character.
define input parameter query-data-file as character.

define variable querydata as longchar.
define variable HeaderInfo as character.

define stream inp-file.
define variable str as character.

HeaderInfo = "persistent|true|reply-to|" + ReplyTo + "|QueryId|" + QueryId.
HeaderInfo = HeaderInfo + "|QueryType|AppservQuery".

querydata = "/Query=" + query-type + chr(10).
if query-params <> "" then 
  querydata = querydata + query-params + chr(10).
if query-data-file <> "" then 
do:
  querydata = querydata + "/data" + chr(10).
  
  input stream inp-file from value ( query-data-file ).
  repeat:
    import stream inp-file unformatted str.
    querydata = querydata + str + chr(10).
  end.
  input stream inp-file close.
  querydata = querydata + "data/" + chr(10).
end.
querydata = querydata + "Query/" + chr(10).

run SendQuery (qname, HeaderInfo,QueryData).

PROCEDURE SendQuery:
  define input parameter qname as character.
  define input parameter HeaderInfo as character.
  define input parameter QueryData as longchar.

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

  run Stomp/SendMessage.p (QueryData,MQServer,MQPort,MQLogin,MQPass,qname,HeaderInfo).
  RETURN RETURN-VALUE.
END.
