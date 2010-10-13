USING Stomp.*.

def var qname as character.
qname = "/queue/oblik81".

run Stomp/ReadMessage.p ("10.10.4.9",61613,"progress","progress",qname,
"MessageHandler", THIS-PROCEDURE).

PROCEDURE MessageHandler:
  DEFINE INPUT PARAMETER ipobjFrame AS Stomp.Frame NO-UNDO.

  DEFINE VARIABLE lcMessage AS LONGCHAR NO-UNDO.
  define variable replyto as character.
  define variable QueryType as character.
  define variable QueryId as character.
  define variable filename as character.
  define variable rc as character.
  define variable oHeader as character.

  lcMessage = ipobjFrame:getMessageData(). 
  replyto   = ipobjFrame:getHeaderValue ("reply-to").
  QueryType = ipobjFrame:getHeaderValue ("QueryType").
  QueryId   = ipobjFrame:getHeaderValue ("QueryId").
  oHeader   = "QueryId|" + QueryId.

  if QueryType = "Accounts" then
  do:
    filename = "log/accdata.xml".
    run src/transfer/export/accdata.p (filename).
    
    run Stomp/FileToMQ.p (filename, "10.10.4.9", 61613, "progress", "progress", replyto, oHeader).
    rc = RETURN-VALUE.
    OS-DELETE VALUE(filename).
    /* Ack query, if reply (file transfer) successful */
    RETURN rc.
  end.

  RETURN "OK".
END.
