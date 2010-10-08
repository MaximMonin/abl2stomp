USING Stomp.*.

def var qname as character.
def var MessageData as character.
qname = "/queue/oblik81".
MessageData = "Test Send + Read through MQ".

run Stomp/SendMessage.p (MessageData,"10.10.4.9",61613,"progress","progress",qname,
"persistent|true|reply-to|/topic/cwh.data").

run Stomp/ReadMessage.p ("10.10.4.9",61613,"progress","progress",qname,
"MessageHandler", THIS-PROCEDURE).

message RETURN-VALUE view-as alert-box.

PROCEDURE MessageHandler:
  DEFINE INPUT PARAMETER ipobjFrame AS Stomp.Frame NO-UNDO.
  DEFINE VARIABLE lcMessage AS LONGCHAR NO-UNDO.
  define variable replyto as character.

  lcMessage = ipobjFrame:getMessageData(). 
  replyto = ipobjFrame:getHeaderValue ("reply-to").

  def var rc as character.
  rc  = lcMessage.
  message replyto rc view-as alert-box.
  RETURN "OK".
END.
