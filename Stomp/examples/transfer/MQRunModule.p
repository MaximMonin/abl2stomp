define input parameter qname as character.
define input parameter PacketFile as character.
define input parameter ModuleFile as character.
define input parameter ModuleParams as character.
define input parameter replyto as character.
define input parameter QueryId as character.

define variable HeaderInfo as character.

HeaderInfo = "persistent|true|QueryType|runModule|binary|true|zip|true".

if QueryId = "" then QueryId = "0".
HeaderInfo = HeaderInfo + "|reply-to|" + replyto + "|QueryId|" + QueryId.

if ModuleFile <> "" then
  HeaderInfo = HeaderInfo + "|ModuleName|" + ModuleFile.
if ModuleParams <> "" then
  HeaderInfo = HeaderInfo + "|ModuleParams|" + ModuleParams.
run SendQuery (qname,PacketFile, HeaderInfo).
RETURN RETURN-VALUE.


PROCEDURE SendQuery:
  define input parameter qname as character.
  define input parameter filename as character.
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

  run Stomp/FileToMQ.p (filename,MQServer,MQPort,MQLogin,MQPass,qname,HeaderInfo).
  RETURN RETURN-VALUE.
END.
