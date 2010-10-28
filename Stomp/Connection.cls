 
 /*------------------------------------------------------------------------
    File        : Connection
    Purpose     : Abstraction of a connection to a STOMP server.
    Author(s)   : Abe Voelker
    Created     : Fri Sep 11 12:44:34 CDT 2009
    Notes       : 
  ----------------------------------------------------------------------*/

USING Progress.Lang.*.

&SCOPED-DEFINE STATUS_DISCONNECTED 0
&SCOPED-DEFINE STATUS_CONNECT_WAIT 1
&SCOPED-DEFINE STATUS_CONNECTED    2

&SCOPED-DEFINE SOCKET_HANDLER_LOCATION  Stomp/SocketReader.p
&SCOPED-DEFINE SOCKET_SUB_PROC_NAME     "ReadSocketResponse"
&SCOPED-DEFINE SOCKET_WAIT_TIME         30

CLASS Stomp.Connection: 
	
	DEFINE PRIVATE VARIABLE cServer               AS CHARACTER NO-UNDO.
	DEFINE PRIVATE VARIABLE iPort                 AS INTEGER   NO-UNDO.
	DEFINE PRIVATE VARIABLE cUsername             AS CHARACTER NO-UNDO.
	DEFINE PRIVATE VARIABLE cPassword             AS CHARACTER NO-UNDO.
	DEFINE PRIVATE VARIABLE ClientId              AS CHARACTER NO-UNDO.
	DEFINE PRIVATE VARIABLE hErrorProcess         AS HANDLE    NO-UNDO.
    DEFINE PRIVATE VARIABLE cErrorProcessName     AS CHARACTER NO-UNDO.
	
	DEFINE PUBLIC  VARIABLE hSocket               AS HANDLE    NO-UNDO.
	DEFINE PRIVATE VARIABLE hSocketHandlerProcess AS HANDLE    NO-UNDO.
	
	DEFINE PRIVATE VARIABLE cSessionID            AS CHARACTER NO-UNDO.
	DEFINE PRIVATE VARIABLE cTransactionID        AS CHARACTER NO-UNDO.

    DEFINE PRIVATE VARIABLE cReceiptWaiting       AS CHARACTER NO-UNDO.
    DEFINE PRIVATE VARIABLE lReceiptProcessed     AS LOGICAL   NO-UNDO.
	
	DEFINE PRIVATE VARIABLE iStatus               AS INTEGER   NO-UNDO.
	
	DEFINE PRIVATE VARIABLE objLogger             AS Stomp.Logger NO-UNDO.
	DEFINE PRIVATE VARIABLE objQueue              AS Stomp.Queue  NO-UNDO.
    

	CONSTRUCTOR PUBLIC Connection (INPUT ipobjLogger AS Stomp.Logger,
	                               INPUT ipcServer   AS CHARACTER,
	                               INPUT ipiPort     AS INTEGER,
	                               INPUT ipcUsername AS CHARACTER,
	                               INPUT ipcPassword AS CHARACTER):
	    ASSIGN objLogger = ipobjLogger
               cServer   = ipcServer
	           iPort     = ipiPort
	           cUsername = ipcUsername
	           cPassword = ipcPassword
	           iStatus   = {&STATUS_DISCONNECTED}.
	END CONSTRUCTOR.
	CONSTRUCTOR PUBLIC Connection (INPUT ipobjLogger AS Stomp.Logger,
	                               INPUT ipcServer   AS CHARACTER,
	                               INPUT ipiPort     AS INTEGER,
	                               INPUT ipcUsername AS CHARACTER,
	                               INPUT ipcPassword AS CHARACTER,
                                       INPUT Client      AS CHARACTER):
	    ASSIGN objLogger = ipobjLogger
               cServer   = ipcServer
	           iPort     = ipiPort
	           cUsername = ipcUsername
	           cPassword = ipcPassword
	           iStatus   = {&STATUS_DISCONNECTED}
                   ClientId  = Client.
	END CONSTRUCTOR.
	

	DESTRUCTOR Connection():
	    /* If transaction open and uncommitted, assume it should be aborted */
	    IF isConnected() AND isTransactionOpen() THEN
            abortTransaction().
        /* Close the connection to server */
        IF isConnected() THEN
            THIS-OBJECT:disconnect().
	    /* Destroy persistent socket handler */
        IF VALID-HANDLE(hSocketHandlerProcess) THEN
            DELETE OBJECT hSocketHandlerProcess.
	    /* Destroy socket */
        IF VALID-HANDLE(hSocket) THEN 
        DO:
    	    IF hSocket:CONNECTED() THEN
                hSocket:DISCONNECT().
            DELETE OBJECT hSocket.
        END.
        /* Remove Queue object reference */
        THIS-OBJECT:removeQueue().
	END DESTRUCTOR.
	

    /* Connect to server:port using username/pass assigned from constructor */
    METHOD PUBLIC LOGICAL connect():
        IF NOT THIS-OBJECT:isConnected() THEN DO:
            objLogger:writeEntry(1, "Connection " + cServer + ":" + STRING(iPort) + ": Preparing connection to " +
                                    cServer + ":" + STRING(iPort)).
            /* Create a connect Frame */
            DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
            ASSIGN objFrame = Stomp.FrameFactory:makeConnectFrame(INPUT cUsername, INPUT cPassword, INPUT objLogger).
            if ClientId <> "" then
              objFrame:addHeaderData("client-id", ClientId).
            CREATE SOCKET hSocket.
            /* Start socket handler procedure persistently */
            RUN {&SOCKET_HANDLER_LOCATION} PERSISTENT
              SET hSocketHandlerProcess
              (THIS-OBJECT, objLogger).
            objLogger:writeEntry(2, "Connection " + cServer + ":" + STRING(iPort) + ": Started socket handler process" +
                                    " (unique-id = """ + STRING(hSocketHandlerProcess:UNIQUE-ID) + """)").
            /* Tell socket to use socket reader procedure to process responses */
            IF SESSION:BATCH-MODE THEN
              hSocket:SET-READ-RESPONSE-PROCEDURE({&SOCKET_SUB_PROC_NAME},
                                                  hSocketHandlerProcess).
            /* Connect the socket */
            objLogger:writeEntry(2, "Connection " + cServer + ":" + STRING(iPort) + ": Attempting socket connection to " +
                                    cServer + " on port " + STRING(iPort)).
            /* Do socket connection */
            IF hSocket:CONNECT(("-H " + cServer + " -S " + STRING(iPort))) THEN DO:
                objLogger:writeEntry(2, "Connection " + cServer + ":" + STRING(iPort) + ": Socket connect successful.").
                ASSIGN iStatus = {&STATUS_CONNECT_WAIT}.
                /* Send STOMP Connect Frame */
                objLogger:writeEntry(2, "Connection " + cServer + ":" + STRING(iPort) + ": Sending STOMP Connect frame").
                sendFrame(objFrame).
                DELETE OBJECT objFrame.
                waitForSocket({&SOCKET_WAIT_TIME}).
                /* Verify that we are now connected to STOMP server */
                IF isConnected() THEN DO:
                    /* Connected successfully! */
                    IF cSessionID NE "" THEN
                        objLogger:writeEntry(1, "Connection " + cServer + ":" + STRING(iPort) + ": Connected successfully!" +
                                                " Session ID = """ + cSessionID + """").
                    ELSE
                        objLogger:writeEntry(1, "Connection " + cServer + ":" + STRING(iPort) + ": Connected successfully!").
                    RETURN TRUE.
                END.
                ELSE DO:
                    /* Handle STOMP connect refused/time-out */
                    objLogger:writeError(1, "Connection " + cServer + ":" + STRING(iPort) + ": STOMP connect refused by server or has " +
                                            "timed out after {&SOCKET_WAIT_TIME} second wait. Try connection again later.").
                    RETURN FALSE.
                END.
            END.
            ELSE DO:
                /* Socket connection failed */
                objLogger:writeError(1, "Connection " + cServer + ":" + STRING(iPort) + ": Unable to connect to socket! Reason:" +
                                        " '" + ERROR-STATUS:GET-MESSAGE(1) + "'").
                RETURN FALSE.
            END.
        END.
        ELSE DO:
            /* Connection already connected - connect() call denied */
            objLogger:writeError(1, "Connection " + cServer + ":" + STRING(iPort) + ": Unable to connect! Reason:" +
                                    " the Connection object has already been connected! You must call disconnect() before re-connecting.").
            RETURN FALSE.
        END.
    END METHOD.
    


    /* Disconnect from STOMP server */
    METHOD PUBLIC LOGICAL disconnect():
        IF THIS-OBJECT:isConnected() THEN DO:
            /* Create a disconnect Frame */
            DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
            ASSIGN objFrame = Stomp.FrameFactory:makeDisconnectFrame(INPUT objLogger).
            /* Send the STOMP Disconnect frame, not waiting for an acknowledgement from server */
            if valid-object(objLogger) then
              objLogger:writeEntry(1, "Connection " + cServer + ":" + STRING(iPort) + ": Sending STOMP Disconnect frame...").
            sendFrame(objFrame).
            DELETE OBJECT objFrame.
            /* Disconnect the socket */
            if valid-object(objLogger) then
              objLogger:writeEntry(1, "Connection " + cServer + ":" + STRING(iPort) + ": Closing socket...").
            if not valid-handle (hSocket) then RETURN TRUE.
            RETURN hSocket:DISCONNECT().
        END.
        ELSE DO:
            /* Handle Connection not connected to anything */
            if valid-object(objLogger) then
              objLogger:writeError(3, "Connection " + cServer + ":" + STRING(iPort) + ": disconnect() called, but Connection" +
                                      " is already disconnected!").
            RETURN FALSE.
        END.
    END METHOD.
	

    /* Assign Queue object reference */
	METHOD PUBLIC LOGICAL setQueue(INPUT ipobjQueue AS Stomp.Queue):
	    IF objQueue EQ ? THEN DO:
	        ASSIGN objQueue = ipobjQueue.
	        RETURN TRUE.
	    END.
	    ELSE DO:
            if valid-object(objLogger) then
              objLogger:writeError(1, "Connection " + cServer + ":" + STRING(iPort) + ": Attempt to add new Queue """ +
                                      ipobjQueue:getDestinationName() + """ when Connection already has a reference to" +
                                      " Queue """ + objQueue:getDestinationName() + """ (a Connection can only have a" +
                                      " reference to one Queue at a time. Call removeQueue() before you add another one!)").
            RETURN FALSE.
        END.
	END METHOD.
	

    /* Remove Queue object reference */
	METHOD PUBLIC VOID removeQueue():
	    IF objQueue NE ? THEN
            ASSIGN objQueue = ?.
	END METHOD.


    /* Wait for socket to have data ready.  Use this method to wait for server responses to sent messages. */
    METHOD PUBLIC VOID waitForSocket(INPUT ipiWaitTime AS INTEGER):
        if not valid-handle (hSocket) then RETURN.

        ASSIGN hSocket:SENSITIVE = TRUE. /* Needed incase ReadSocketResponse procedure is still open on the procedure stack */
        IF SESSION:BATCH-MODE THEN
        do:
          WAIT-FOR READ-RESPONSE OF hSocket PAUSE ipiWaitTime.
        end.
        else do:
          define variable t1 as datetime-tz.
          t1 = now.
          repeat:
            if hSocket:GET-BYTES-AVAILABLE() > 0 then
            do:
              run {&SOCKET_SUB_PROC_NAME} in hSocketHandlerProcess.
              leave.
            end.
            if now - t1 > ipiWaitTime * 1000 then leave.
            pause 1 no-message.
          end.
        end.

    END METHOD.
    

    /* Sends a Frame across the socket (doesn't wait for response; calling procedure must do that) */
    METHOD PUBLIC LOGICAL sendFrame(INPUT ipobjFrame AS Stomp.Frame):
        IF isTransactionOpen() THEN
            ipobjFrame:addTransaction(cTransactionID).
        DEFINE VARIABLE mptrRawFrame AS MEMPTR  NO-UNDO.
        DEFINE VARIABLE iFrameSize   AS INTEGER NO-UNDO.
       
        if valid-object(objLogger) then
          objLogger:dumpFrame(ipobjFrame, 4).

        ASSIGN iFrameSize = LENGTH(ipobjFrame:toLongChar(), "RAW").
        SET-SIZE(mptrRawFrame) = iFrameSize + 1. /* MALLOC (Extra byte for NULL-term string) */
        PUT-STRING(mptrRawFrame, 1) = ipobjFrame:toLongChar().
        PUT-BYTE(mptrRawFrame, iFrameSize + 1) = 0.

        DEFINE VARIABLE bytesToWrite   AS INTEGER   NO-UNDO.
        DEFINE VARIABLE bufferPosition AS INTEGER   NO-UNDO.

        bytesToWrite = iFrameSize + 1.
        bufferPosition = 1.

        DO WHILE bytesToWrite > 0:
          if not valid-handle (hSocket) then LEAVE.
          hSocket:WRITE(mptrRawFrame, bufferPosition, bytesToWrite) NO-ERROR.
          if ERROR-STATUS:ERROR or hSocket:BYTES-WRITTEN = 0 then 
          do:
            if valid-object(objLogger) then
              objLogger:writeError(1, "Connection " + cServer + ":" + STRING(iPort) + ": Writing to socket error").
            leave.
          end.
          bytesToWrite = bytesToWrite - hSocket:BYTES-WRITTEN.
          bufferPosition = bufferPosition + hSocket:BYTES-WRITTEN.

          if valid-object(objLogger) then
            objLogger:writeEntry(4, "Bytes written: " + STRING(hSocket:BYTES-WRITTEN)).

        END.
        SET-SIZE(mptrRawFrame) = 0.

        if bytesToWrite > 0 then 
        do:
          if valid-object(objLogger) then
            objLogger:writeError(1, "Connection " + cServer + ":" + STRING(iPort) + ": Sending frame error").
          RETURN FALSE.
        end.
        ELSE RETURN TRUE.
    END METHOD.
    

    /* Sends 'start transaction' msg to server and waits for response */
	METHOD PUBLIC LOGICAL startTransaction(INPUT cTransID AS CHARACTER):
	    IF isConnected() AND (NOT isTransactionOpen()) THEN DO:
           /* Create a STOMP Begin frame */
            DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
            ASSIGN objFrame          = Stomp.FrameFactory:makeBeginFrame(INPUT cTransID, INPUT objLogger)
                   cReceiptWaiting   = objFrame:addReceipt()
                   lReceiptProcessed = FALSE.
            sendFrame(objFrame).
            DELETE OBJECT objFrame.
            waitForSocket({&SOCKET_WAIT_TIME}).
            IF lReceiptProcessed THEN DO:
                /* Server acknowledged transaction begin message */
                ASSIGN cTransactionID = cTransID. /* Save the transaction ID */
                if valid-object(objLogger) then
                  objLogger:writeEntry(2, "Connection " + cServer + ":" + STRING(iPort) + ": STOMP Server acknowledged " +
                                          " transaction begin request. Server requests are now in a TRANSACTION.").
                RETURN TRUE.
            END.
            ELSE DO:
                /* Server did not acknowledge transaction begin message! */
                if valid-object(objLogger) then
                  objLogger:writeError(1, "Connection " + cServer + ":" + STRING(iPort) + ": STOMP Server did not acknowledge begin" +
                                          " transaction request, or request failed due to {&SOCKET_WAIT_TIME} seconds timeout!").
                RETURN FALSE.
            END.
	    END.
        ELSE DO:
            /* Handle not connected / transaction already open */
            if valid-object(objLogger) then
              objLogger:writeError(2, "Connection " + cServer + ":" + STRING(iPort) + ": Transaction begin not possible.  Connection " +
                                      " either is not connected, or there is already an open transaction."). 
            RETURN FALSE.
        END.
	END METHOD.
	

	/* If no transaction ID provided, use current system date/time */
    /* NOTE: If the call to the other startTransaction method fails, we return a null pointer (? reference) */
	METHOD PUBLIC CHARACTER startTransaction():
	    DEFINE VARIABLE cTransID AS CHARACTER NO-UNDO.
	    ASSIGN cTransID = ISO-DATE(NOW).
	    IF THIS-OBJECT:startTransaction(INPUT cTransID) THEN
            RETURN cTransID.
        ELSE
            RETURN ?. /* startTransaction failed; return null pointer */
	END METHOD.
	

	/* Sends 'commit transaction' message to server and waits for response */
	METHOD PUBLIC LOGICAL commitTransaction():
	    IF isConnected() AND isTransactionOpen() THEN DO:
	        /* Create a commit Frame */
            DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
            ASSIGN objFrame          = Stomp.FrameFactory:makeCommitFrame(INPUT cTransactionID, INPUT objLogger)
                   cReceiptWaiting   = objFrame:addReceipt()
                   lReceiptProcessed = FALSE.
            sendFrame(objFrame).
            DELETE OBJECT objFrame.
            waitForSocket({&SOCKET_WAIT_TIME}).
            IF lReceiptProcessed THEN DO:
                /* Server acknowledged transaction commit message */
                ASSIGN cTransactionID = "". /* Clear the transaction ID */
                if valid-object(objLogger) then
                  objLogger:writeEntry(2, "Connection " + cServer + ":" + STRING(iPort) + ": STOMP Server acknowledged " +
                                          " transaction commit request. Server has committed all requests within this transaction.").
                RETURN TRUE.
            END.
            ELSE DO:
                /* Server did not acknowledge transaction commit message! */
                if valid-object(objLogger) then
                  objLogger:writeError(1, "Connection " + cServer + ":" + STRING(iPort) + ": STOMP Server did not acknowledge commit" +
                                          " transaction request, or commit failed due to {&SOCKET_WAIT_TIME} seconds timeout!").
                RETURN FALSE.
            END.
	    END.
        ELSE DO:
            /* Handle not connected / no transaction open */
            if valid-object(objLogger) then
              objLogger:writeError(2, "Connection " + cServer + ":" + STRING(iPort) + ": transaction commit not possible.  Connection " +
                                      " either is not connected, or there is no transaction open to commit."). 
            RETURN FALSE.
        END.
	END METHOD.
	

	/* Sends 'abort transaction' msg to server and waits for response */
	METHOD PUBLIC LOGICAL abortTransaction():
	    IF isConnected() AND isTransactionOpen() THEN DO:
	        /* Create an abort Frame */
            DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
            ASSIGN objFrame          = Stomp.FrameFactory:makeAbortFrame(INPUT cTransactionID, INPUT objLogger)
                   cReceiptWaiting   = objFrame:addReceipt()
                   lReceiptProcessed = FALSE.
            sendFrame(objFrame).
            DELETE OBJECT objFrame.
            waitForSocket({&SOCKET_WAIT_TIME}).
            IF lReceiptProcessed THEN DO:
                /* Server acknowledged transaction abort message */
                ASSIGN cTransactionID = "". /* Clear the transaction ID */
                if valid-object(objLogger) then
                  objLogger:writeEntry(2, "Connection " + cServer + ":" + STRING(iPort) + ": STOMP Server acknowledged " +
                                          " transaction abort request. Server has aborted all requests within this transaction.").
                RETURN TRUE.
            END.
            ELSE DO:
                /* Server did not acknowledge transaction abort message! */
                if valid-object(objLogger) then
                  objLogger:writeError(2, "Connection " + cServer + ":" + STRING(iPort) + ": STOMP Server did not acknowledge abort" +
                                          " transaction request, or abort failed due to {&SOCKET_WAIT_TIME} seconds timeout!").
                RETURN FALSE.
            END.
	    END.
        ELSE DO:
            /* Handle not connected / no transaction open */
            if valid-object(objLogger) then
              objLogger:writeError(2, "Connection " + cServer + ":" + STRING(iPort) + ": transaction abort not possible.  Connection " +
                                      " either is not connected, or there is no transaction open to abort."). 
            RETURN FALSE.
        END.
	END METHOD.
    
    
    /* Called by Socket process to route incoming STOMP Frames from server to proper Queue destination */
    /* (or to handle them within Connection object if that is where they belong)                       */
    METHOD PUBLIC VOID routeFrame(INPUT ipobjFrame AS Stomp.Frame):
        CASE ipobjFrame:getFrameType():
            WHEN "CONNECTED" THEN DO:
                /* All CONNECT Frames should be handled within the Connection object */
                IF iStatus EQ {&STATUS_CONNECT_WAIT} THEN DO:
                    ASSIGN iStatus = {&STATUS_CONNECTED}.
                    IF ipobjFrame:getHeaderValue("session") NE ? THEN
                        ASSIGN cSessionID = ipobjFrame:getHeaderValue("session").
                END.
                ELSE
                    handleError(1,
                                "Connection " + cServer + ":" + STRING(iPort) + ": Received a connection" +
                                   " authenticated response from server, but was not expecting it!",
                                ipobjFrame).
            END.
            WHEN "MESSAGE" THEN DO:
                /* All MESSAGE Frames should be routed to the Queue object */
                IF objQueue EQ ? THEN
                    handleError(1,
                                "Connection " + cServer + ":" + STRING(iPort) + ": Received a Queue message," +
                                   " but the Connection does not have a reference to a Queue object to route it to!",
                                ipobjFrame).
                ELSE DO:
                    objLogger:writeEntry(2, "Connection " + cServer + ":" + STRING(iPort) + ": Received a message;" +
                                            " routing it to Queue").
                    objQueue:processMessage(ipobjFrame).
                END.
            END.
            WHEN "RECEIPT" THEN DO:
                /* RECEIPT Frames can either go to the Connection object or the Queue object, depending on state information */
                IF cReceiptWaiting NE "" THEN DO:
                    /* Connection object is waiting for a receipt; do not hand it off to the Queue object */
                    IF ipobjFrame:getReceipt() EQ cReceiptWaiting THEN DO:
                        ASSIGN lReceiptProcessed = TRUE
                               cReceiptWaiting   = "".
                    END.
                    ELSE DO:
                        /* Receipt-id's do not match! Throw error. */
                        ASSIGN lReceiptProcessed = FALSE.
                        handleError(1,
                                    "Connection " + cServer + ":" + STRING(iPort) + ": Received a Connection receipt," +
                                       " but the receipt-id did not match the expected value! Expected '" + cReceiptWaiting +
                                       "', but received '" + ipobjFrame:getReceipt() + "'.",
                                    ipobjFrame).
                    END.
                END.
                ELSE DO:
                    /* Connection object not waiting for a receipt; therefore the Queue must be waiting for it */
                    IF objQueue EQ ? THEN
                        handleError(1,
                                    "Connection " + cServer + ":" + STRING(iPort) + ": Received a Queue receipt," +
                                       " but the Connection does not have a reference to a Queue object to route it to!",
                                    ipobjFrame).
                    ELSE DO:
                        objLogger:writeEntry(2, "Connection " + cServer + ":" + STRING(iPort) + ": Received a receipt (receipt-id '" + 
                                                ipobjFrame:getReceipt() + "'); routing it to Queue").
                        objQueue:processReceipt(ipobjFrame).
                    END.
                END.
            END.
            WHEN "ERROR" THEN
                /* All ERROR Frames should be written to log, then handed off to user-provided error-handling procedure */
                handleError(1,
                            "Connection " + cServer + ":" + STRING(iPort) + ": Received an ERROR frame!",
                            ipobjFrame).
            OTHERWISE
                /* If we receive some unknown frame type, something is probably very wrong */
                handleError(1,
                            "Connection " + cServer + ":" + STRING(iPort) + ": Received an unknown frame type!" +
                              " Unable to process/route this frame!",
                            ipobjFrame).
        END CASE.
        
    END METHOD.
    

    METHOD PUBLIC VOID handleError(INPUT ipiErrorLevel AS INTEGER,
                                      INPUT ipcError      AS CHARACTER,
                                      INPUT ipobjFrame    AS Stomp.Frame):
        /* Throw error to user-supplied error handling procedure */
        IF VALID-HANDLE(objQueue:getErrorProcessHandle()) THEN
            RUN VALUE(objQueue:getErrorProcessName()) 
                IN objQueue:getErrorProcessHandle()
                (INPUT ipiErrorLevel,
                 INPUT ipcError,
                 INPUT ipobjFrame).
        ELSE DO:
          /* Write error message to log */
          objLogger:writeError(ipiErrorLevel, ipcError).
          /* Dump Frame to log */
          IF ipobjFrame NE ? THEN
             if valid-object(objLogger) then
              objLogger:dumpErrorFrame(INPUT ipobjFrame, ipiErrorLevel).
        END.
    END METHOD.
    
    /* Getters */
    
    METHOD PUBLIC CHARACTER getServer():
        RETURN cServer.
    END METHOD.
    

    METHOD PUBLIC INTEGER getPort():
        RETURN iPort.
    END METHOD.
    

    METHOD PUBLIC HANDLE getSocket():
        RETURN hSocket.
    END METHOD.
    

    METHOD PUBLIC LOGICAL isConnected():
        IF iStatus = {&STATUS_DISCONNECTED} THEN
            RETURN FALSE.
        IF NOT VALID-HANDLE(hSocket) THEN
            RETURN FALSE.
        ELSE
            RETURN hSocket:CONNECTED().
    END METHOD.
    

    METHOD PUBLIC LOGICAL isTransactionOpen():
        RETURN (cTransactionID NE "").
    END METHOD.

END CLASS.
