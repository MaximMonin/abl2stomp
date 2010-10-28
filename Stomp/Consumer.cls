 
 /*------------------------------------------------------------------------
    File        : Consumer
    Purpose     : Implementation of a Consumer's view of an ActiveMQ queue -
                  they can subscribe and unsubscrube to the queue as well as
                  acknowledge Frames.  Method processMessage is used to 
                  forward 'Message' Frames for further processing to user-
                  defined procedure.
    Author(s)   : Abe Voelker
    Created     : Fri Sep 11 12:47:47 CDT 2009
    Notes       : 
  ----------------------------------------------------------------------*/

USING Progress.Lang.*.

&SCOPED-DEFINE STATUS_UNSUBSCRIBED 0
&SCOPED-DEFINE STATUS_RECEIPT_WAIT 1
&SCOPED-DEFINE STATUS_SUBSCRIBED   2

&SCOPED-DEFINE SOCKET_WAIT_TIME 30

CLASS Stomp.Consumer INHERITS Stomp.Queue:

    DEFINE PRIVATE VARIABLE hParentProcess  AS HANDLE              NO-UNDO.
    DEFINE PRIVATE VARIABLE cParentProcName AS CHARACTER           NO-UNDO.
    
    DEFINE PRIVATE VARIABLE iStatus         AS INTEGER             NO-UNDO.
    DEFINE PRIVATE VARIABLE cReceiptWaiting AS CHARACTER           NO-UNDO.
    DEFINE PUBLIC  VARIABLE SubscribeHeader AS CHARACTER           NO-UNDO.

    DEFINE PRIVATE VARIABLE lReceiptProcOK  AS LOGICAL             NO-UNDO.
    
		
	CONSTRUCTOR PUBLIC Consumer(INPUT ipcDestQueue      AS CHARACTER,
                                INPUT ipobjLogger       AS Stomp.Logger,
	                            INPUT ipcParentProcName AS CHARACTER,
	                            INPUT iphParentProcess  AS HANDLE,
                                INPUT ipcErrorProcName  AS CHARACTER,
                                INPUT iphErrorProcPtr   AS HANDLE):
	    SUPER(ipcDestQueue, ipobjLogger, ipcErrorProcName, iphErrorProcPtr).
		ASSIGN cParentProcName = ipcParentProcName
		       hParentProcess  = iphParentProcess
		       iStatus         = {&STATUS_UNSUBSCRIBED}.
	END CONSTRUCTOR.
	

	DESTRUCTOR PUBLIC Consumer():
	    IF iStatus EQ {&STATUS_SUBSCRIBED} THEN
	        THIS-OBJECT:unsubscribe().
	END METHOD.
	

	METHOD PUBLIC LOGICAL subscribe(INPUT iplClientAck AS LOGICAL,
	                                INPUT ipcSelector  AS CHARACTER):
	    /* Create a subscribe Frame */
        DEFINE VARIABLE objSubscribeFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objSubscribeFrame = Stomp.FrameFactory:makeSubscribeFrame(INPUT THIS-OBJECT:cDestQueue,
                                                                         INPUT iplClientAck,
                                                                         INPUT ipcSelector,
                                                                         INPUT objLogger).
        define var i as integer.
        do i = 1 to NUM-ENTRIES (SubscribeHeader, "|") by 2:
          objSubscribeFrame:addHeaderData(ENTRY(i,SubscribeHeader, "|"), ENTRY(i + 1, SubscribeHeader, "|")).
        end.
        /* Send to internal subscribe procedure */
        RETURN THIS-OBJECT:subscribe(INPUT objSubscribeFrame).
	END METHOD.


    METHOD PUBLIC LOGICAL subscribe(INPUT iplClientAck AS LOGICAL):
	    /* Create a subscribe Frame */
        DEFINE VARIABLE objSubscribeFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objSubscribeFrame = Stomp.FrameFactory:makeSubscribeFrame(INPUT THIS-OBJECT:cDestQueue,
                                                                         INPUT iplClientAck,
                                                                         INPUT objLogger).
        define var i as integer.
        do i = 1 to NUM-ENTRIES (SubscribeHeader, "|") by 2:
          objSubscribeFrame:addHeaderData(ENTRY(i,SubscribeHeader, "|"), ENTRY(i + 1, SubscribeHeader, "|")).
        end.
        /* Send to internal subscribe procedure */
        RETURN THIS-OBJECT:subscribe(INPUT objSubscribeFrame).
	END METHOD.

    METHOD PUBLIC LOGICAL subscribe(INPUT HeaderInfo as CHARACTER, INPUT iplClientAck AS LOGICAL):
	    /* Create a subscribe Frame */
        DEFINE VARIABLE objSubscribeFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objSubscribeFrame = Stomp.FrameFactory:makeSubscribeFrame(INPUT THIS-OBJECT:cDestQueue,
                                                                         INPUT iplClientAck,
                                                                         INPUT objLogger).
        SubscribeHeader = HeaderInfo.
        define var i as integer.
        do i = 1 to NUM-ENTRIES (SubscribeHeader, "|") by 2:
          objSubscribeFrame:addHeaderData(ENTRY(i,SubscribeHeader, "|"), ENTRY(i + 1, SubscribeHeader, "|")).
        end.
        /* Send to internal subscribe procedure */
        RETURN THIS-OBJECT:subscribe(INPUT objSubscribeFrame).
	END METHOD.

    
    /* Subscribe to a STOMP queue in order to receive messages pushed to that queue */
    /* NOTE: Ideally this would be a PRIVATE procedure, but PROGRESS's compiler doesn't seem to want to allow that */
    /* (Cannot reference private member "subscribe" off of an object reference.) */
    METHOD PUBLIC LOGICAL subscribe(INPUT ipobjSubscribeFrame AS Stomp.Frame):
        IF isSubscribed() THEN DO:
            objLogger:writeError(1, "Consumer on " + cDestQueue + ": Already subscribed to a queue. You must call unsubscribe() first!").
    	    RETURN FALSE.
	    END.
	    ELSE DO:
            ASSIGN iStatus           = {&STATUS_RECEIPT_WAIT}
                   cReceiptWaiting   = ipobjSubscribeFrame:addReceipt()
                   lReceiptProcOK    = FALSE.
            objLogger:writeEntry(1, "Consumer on " + cDestQueue + ": Sending STOMP Subscribe frame to server (receipt-id '" + cReceiptWaiting + "')").
            /* Send the subscribe Frame */
            objConnection:sendFrame(ipobjSubscribeFrame).
            objConnection:waitForSocket({&SOCKET_WAIT_TIME}).
            /* Verify that we are now subscribed */
            IF lReceiptProcOK THEN DO:
                THIS-OBJECT:setStatusToSubscribed().
                if valid-object (objLogger) then
                  objLogger:writeEntry(1, "Consumer on " + cDestQueue + ": Successfully subscribed to " + cDestQueue + "; waiting for messages...").
                RETURN TRUE.
            END.
            ELSE DO:
                if valid-object (objLogger) then
                  objLogger:writeError(1, "Consumer on " + cDestQueue + ": Could not subscribe to " + cDestQueue + "; subscription refused or attempt timed " +
                                          "out due to {&SOCKET_WAIT_TIME} second wait limit. Try again later.").
                RETURN FALSE.
            END.
        END.
    END METHOD.

	
    /* Unsubscribe from the currently-subscribed STOMP queue */
	METHOD PUBLIC LOGICAL unsubscribe():
	    IF NOT isSubscribed() THEN DO:
            objLogger:writeError(1, "Consumer on " + cDestQueue + ": No queue subscription exists. Unsubscribe not possible.").
    	    RETURN FALSE.
	    END.
	    ELSE DO:
	        /* Create an unsubscribe Frame */
    	    DEFINE VARIABLE objUnsubscribeFrame AS Stomp.Frame NO-UNDO.
    	    ASSIGN objUnsubscribeFrame = Stomp.FrameFactory:makeUnsubscribeFrame(INPUT cDestQueue,
                                                                                 INPUT objLogger)
                   cReceiptWaiting     = objUnsubscribeFrame:addReceipt()
                   lReceiptProcOK      = FALSE.
    	    objConnection:sendFrame(objUnsubscribeFrame).
            objConnection:waitForSocket({&SOCKET_WAIT_TIME}).
            /* Verify that we are now unsubscribed */
            IF lReceiptProcOK THEN DO:
                THIS-OBJECT:setStatusToUnsubscribed().
                objLogger:writeEntry(1, "Consumer on " + cDestQueue + ": Successfully unsubscribed from " + cDestQueue + ".").
                RETURN TRUE.
            END.
            /* If Mq sends message on unsubscribe query, repeat one more time */
            objConnection:waitForSocket({&SOCKET_WAIT_TIME}).
            IF lReceiptProcOK THEN DO:
                THIS-OBJECT:setStatusToUnsubscribed().
                objLogger:writeEntry(1, "Consumer on " + cDestQueue + ": Successfully unsubscribed from " + cDestQueue + ".").
                RETURN TRUE.
            END.
            ELSE DO:
                objLogger:writeError(1, "Consumer on " + cDestQueue + ": Could not unsubscribe from " + cDestQueue + "; unsubscribe refused or attempt timed " +
                                    "out due to {&SOCKET_WAIT_TIME} second wait limit. Try again later.").
                RETURN FALSE.
            END.
	    END.
	END METHOD.
	

    /* AAV - Commented this code out.  Requesting a receipt on ACK Frames is not easy to deal with within PROGRESS's single-thread limitation  */
    /*       due to the fact that the next Message waiting on a queue may be sent before the receipt is, messing up the implementation's       */
    /*       expected response value not matching its state information.  As an alternative received Frames could be timestamped and then      */
    /*       buffered (large increase in program complexity), but is probably overkill for what this program would be used for and for what    */
    /*       would easily be solved with multithreading.                                                                                       */
    /*
    /* Acknowledge Message consumption using a STOMP ACK frame */
	METHOD PUBLIC LOGICAL ackFrame(INPUT ipobjFrame AS Stomp.Frame):
        DEFINE VARIABLE cMsgID AS CHARACTER NO-UNDO.
        ASSIGN cMsgID = ipobjFrame:getHeaderValue("message-id").
        IF cMsgID EQ ? THEN
            RETURN FALSE. /* No message-id is available for this Frame */
        
        /* Create the ACK Frame and send it through our Connection */
        DEFINE VARIABLE objFrameAck AS Stomp.Frame NO-UNDO.
        ASSIGN objFrameAck     = Stomp.FrameFactory:makeAckFrame(INPUT cMsgID,
                                                                 INPUT objLogger)
               cReceiptWaiting = objFrameAck:addReceipt()
               lReceiptProcOK  = FALSE.
        objLogger:writeEntry(2, "Consumer on " + cDestQueue + ": Acknowledging message consumption (confirmation receipt '" + cReceiptWaiting + "')...").
        objConnection:sendFrame(objFrameAck).
        objConnection:waitForSocket({&SOCKET_WAIT_TIME}).
        /* Verify that the server received our ACK */
        IF lReceiptProcOK THEN DO:
            objLogger:writeEntry(2, "Consumer on " + cDestQueue + ": Server has acknowledged message consumption.").
            RETURN TRUE.
        END.
        ELSE DO:
            objLogger:writeError(1, "Consumer on " + cDestQueue + ": Server refused to acknowledge messsage consumption " +
                                    "or attempt timed out due to {&SOCKET_WAIT_TIME} second wait limit.").
            RETURN FALSE.
        END.
    END METHOD.
    */

    /* Acknowledge Message consumption using a STOMP ACK frame (do not ask for receipt) */
    METHOD PUBLIC LOGICAL ackFrame(INPUT ipobjFrame AS Stomp.Frame):
        DEFINE VARIABLE cMsgID AS CHARACTER NO-UNDO.
        ASSIGN cMsgID = ipobjFrame:getHeaderValue("message-id").
        IF cMsgID EQ ? THEN
            RETURN FALSE. /* No message-id is available for this Frame */
        
        /* Create the ACK Frame and send it through our Connection */
        DEFINE VARIABLE objFrameAck AS Stomp.Frame NO-UNDO.
        ASSIGN objFrameAck = Stomp.FrameFactory:makeAckFrame(INPUT cMsgID,
                                                             INPUT objLogger).
        objLogger:writeEntry(2, "Consumer on " + cDestQueue + ": Acknowledging message consumption for message-id '" + cMsgID + "'...").
        objConnection:sendFrame(objFrameAck).
    END METHOD.


    /* Process MESSAGE frames from STOMP server.  Consumer should simply hand these to parent procedure for processing. */
    METHOD PUBLIC OVERRIDE VOID processMessage(INPUT ipobjFrameMsg AS Stomp.Frame):
        IF THIS-OBJECT:isSubscribed() THEN DO:
            objLogger:writeEntry(2, "Consumer on " + cDestQueue + ": Received a message; routing it" +
                                    " to handling procedure").
            /* Send message up to client procedure to properly handle it */
            IF VALID-HANDLE(hParentProcess) THEN
                RUN VALUE(cParentProcName) 
                    IN hParentProcess(ipobjFrameMsg).
        END.
        ELSE DO:
            objLogger:writeError(3, "Consumer on " + cDestQueue + ": Received message before subscription! Process it anyway!!!").
            objLogger:writeEntry(2, "Consumer on " + cDestQueue + ": Received a message; routing it" +
                                    " to handling procedure").
            /* Send message up to client procedure to properly handle it */
            IF VALID-HANDLE(hParentProcess) THEN
                RUN VALUE(cParentProcName) 
                    IN hParentProcess(ipobjFrameMsg).
        END.
    END METHOD.


    /* Process RECEIPT frames from STOMP server.  Consumer uses this to verify that SUBSCRIBE, UNSUBSCRIBE, and ACK frames */
    /* it has sent were received by the server.                                                                            */
    METHOD PUBLIC OVERRIDE VOID processReceipt(INPUT ipobjFrameMsg AS Stomp.Frame):
        IF ipobjFrameMsg:getReceipt() EQ cReceiptWaiting THEN
            ASSIGN lReceiptProcOK = TRUE.
        ELSE DO:
            objLogger:writeError(2, "Consumer on " + cDestQueue + ": Received an unexpected receipt! Received receipt-id '" +
                                    ipobjFrameMsg:getReceipt() + "', was expecting '" + cReceiptWaiting + "'").
            ASSIGN lReceiptProcOK = FALSE.
        END.
    END METHOD.

    
    /* Setters */
    
    METHOD PUBLIC VOID setStatusToUnsubscribed():
        ASSIGN iStatus = {&STATUS_UNSUBSCRIBED}.
    END METHOD.


    METHOD PUBLIC VOID setStatusToSubscribed():
        ASSIGN iStatus = {&STATUS_SUBSCRIBED}.
    END METHOD.
    
    /* Getters */

    METHOD PUBLIC LOGICAL isSubscribed():
        RETURN (iStatus EQ {&STATUS_SUBSCRIBED}).
    END METHOD.

    METHOD PUBLIC VOID waitForSocket(INPUT ipiWaitTime AS INTEGER):
        objConnection:waitForSocket(ipiWaitTime).
    END METHOD.
END CLASS.
