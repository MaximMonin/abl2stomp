 
 /*------------------------------------------------------------------------
    File        : Producer
    Purpose     : Implementation of a Producer's view of an ActiveMQ queue -
                  they can only send Frames.  Method processReceipt used
                  to verify that STOMP server has received messages.
                  Method processMessage is not used for a Producer.
    Author(s)   : Abe Voelker
    Created     : Fri Sep 11 12:47:47 CDT 2009
    Notes       : 
  ----------------------------------------------------------------------*/

USING Progress.Lang.*.

&SCOPED-DEFINE STATUS_READY        0
&SCOPED-DEFINE STATUS_RECEIPT_WAIT 1

&SCOPED-DEFINE SOCKET_WAIT_TIME    30

CLASS Stomp.Producer INHERITS Stomp.Queue: 

    DEFINE PRIVATE VARIABLE iStatus         AS INTEGER             NO-UNDO.
    DEFINE PRIVATE VARIABLE cReceiptWaiting AS CHARACTER           NO-UNDO.
    DEFINE PRIVATE VARIABLE lReceiptProcOK  AS LOGICAL             NO-UNDO.

	CONSTRUCTOR PUBLIC Producer(INPUT ipcDestQueue     AS CHARACTER,
                                INPUT ipobjLogger      AS Stomp.Logger,
                                INPUT ipcErrorProcName AS CHARACTER,
                                INPUT iphErrorProcPtr  AS HANDLE):
        SUPER(ipcDestQueue, ipobjLogger, ipcErrorProcName, iphErrorProcPtr).
		ASSIGN iStatus = {&STATUS_READY}.
	END CONSTRUCTOR.
	

    /* Write data to a STOMP queue, verifying that it was received */
	METHOD PUBLIC LOGICAL send(INPUT iplcData AS LONGCHAR):
	    /* Create a STOMP Send frame */
        DEFINE VARIABLE objSendFrame AS Stomp.Frame NO-UNDO.
        define variable sf as logical.
        ASSIGN objSendFrame    = Stomp.FrameFactory:makeSendFrame(INPUT THIS-OBJECT:cDestQueue, INPUT iplcData, INPUT objLogger)
               iStatus         = {&STATUS_RECEIPT_WAIT}
               cReceiptWaiting = objSendFrame:addReceipt()
               lReceiptProcOK    = FALSE.
        objLogger:writeEntry(2, "Producer on " + cDestQueue + ": Sending frame to server and" +
                                " waiting for a response (receipt = '" + cReceiptWaiting + "')...").
        /* Send the frame */
        sf = objConnection:sendFrame(objSendFrame).
        DELETE OBJECT objSendFrame.
        if not sf then
        do:
           handleError(1, "Producer on " + cDestQueue + ": Socket communication error", ?).
           RETURN FALSE.
        end.

        objConnection:waitForSocket({&SOCKET_WAIT_TIME}).
        /* Note: receipt checking is done in processReceipt method */
        IF lReceiptProcOK THEN DO:
            objLogger:writeEntry(1, "Producer on " + cDestQueue + ": STOMP server acknowledged receipt of transmitted data.").
            THIS-OBJECT:setStatusToReady().
            RETURN TRUE.
        END.
        ELSE DO:
            objLogger:writeEntry(1, "Producer on " + cDestQueue + ": STOMP server either did not acknowledge receipt of " +
                                    "transmitted data, or request timed out after {&SOCKET_WAIT_TIME} seconds limit reached.").
            THIS-OBJECT:setStatusToReady(). /* So we can do another send (although best course of action is probably to shut down) */
            RETURN FALSE.
        END.
	END METHOD.

    /* Write data to a STOMP queue, verifying that it was received      */
    /* This method allows for extra headers being attached to the frame */
    METHOD PUBLIC LOGICAL send(INPUT iplcData AS LONGCHAR,
                               INPUT TABLE-HANDLE iphHeaders,
                               INPUT ipcTTHdrDataCol AS CHARACTER):
        /* Create a STOMP Send frame */
        DEFINE VARIABLE objSendFrame AS Stomp.Frame NO-UNDO.
        define variable sf as logical.
        ASSIGN objSendFrame    = Stomp.FrameFactory:makeSendFrame(INPUT THIS-OBJECT:cDestQueue, INPUT iplcData, INPUT objLogger)
               iStatus         = {&STATUS_RECEIPT_WAIT}
               cReceiptWaiting = objSendFrame:addReceipt()
               lReceiptProcOK    = FALSE.
        /* Attach input header data */
        IF objSendFrame:addHeaderData(INPUT TABLE-HANDLE iphHeaders,
                                      INPUT ipcTTHdrDataCol) THEN DO:
            DELETE OBJECT iphHeaders. /* Clean up dynamic temp-table memory */
    
            objLogger:writeEntry(2, "Producer on " + cDestQueue + ": Sending frame to server and" +
                                " waiting for a response (receipt = '" + cReceiptWaiting + "')...").
            /* Send the frame */
            sf = objConnection:sendFrame(objSendFrame).
            DELETE OBJECT objSendFrame.
            if not sf then
            do:
              handleError(1, "Producer on " + cDestQueue + ": Socket communication error", ?).
              RETURN FALSE.
            end.
            objConnection:waitForSocket({&SOCKET_WAIT_TIME}).
            /* Note: receipt checking is done in processReceipt method */
            IF lReceiptProcOK THEN DO:
                objLogger:writeEntry(1, "Producer on " + cDestQueue + ": STOMP server acknowledged receipt of transmitted data.").
                THIS-OBJECT:setStatusToReady().
                RETURN TRUE.
            END.
            ELSE DO:
                objLogger:writeEntry(1, "Producer on " + cDestQueue + ": STOMP server either did not acknowledge receipt of " +
                                        "transmitted data, or request timed out after {&SOCKET_WAIT_TIME} seconds limit reached.").
                THIS-OBJECT:setStatusToReady(). /* So we can do another send (although best course of action is probably to shut down) */
                RETURN FALSE.
            END.
        END. /* IF objSendFrame:addHeaderData */
        ELSE DO:
            objLogger:writeError(1, "Producer on " + cDestQueue + ": Could not attach headers to frame!").
            RETURN FALSE.
        END.
    END METHOD.

    /* Write data to a STOMP queue, verifying that it was received      */
    METHOD PUBLIC LOGICAL sendFile(INPUT FileName AS CHAR, INPUT UseTransaction as LOGICAL,
                               INPUT TABLE-HANDLE iphHeaders,
                               INPUT ipcTTHdrDataCol AS CHARACTER):

        /* Splitting very big file using 10MB chunks. */
        define variable FileSize as integer.
        define variable FilePartNumb as integer.
        define variable BytesFrom as integer.
        define variable BytesTo as integer.
        define variable CopyBytes as integer.
        define variable iplcData as LONGCHAR.
        define variable tempbuf as LONGCHAR.
        define variable sf as logical.

        if search(FileName) = ? then RETURN.
        FILE-INFORMATION:FILE-NAME = FileName.
        FileSize = FILE-INFORMATION:FILE-SIZE.
        if FileSize = 0 then RETURN.

        FIX-CODEPAGE(tempbuf) = 'UTF-8':U. 
       
        BytesFrom = 1.
        FilePartNumb = 0.
        repeat:
          BytesTo = BytesFrom + 10000000 - 1.
          if BytesTo > FileSize then BytesTo = FileSize.
          CopyBytes = BytesTo - BytesFrom + 1.

          /* if splitting file on chunks it is possible to divide 1 UTF-8 symbol on parts, fixing it */
          define variable i as integer.
          do i = 1 to 3:
            COPY-LOB FROM FILE FileName
              Starting AT BytesFrom FOR CopyBytes TO tempBuf NO-CONVERT NO-ERROR.
            if ERROR-STATUS:ERROR = FALSE then leave.
            CopyBytes = CopyBytes + 1.
            BytesTo = BytesTo + 1.
          end.
          tempBuf = "".
          COPY-LOB FROM FILE FileName
              Starting AT BytesFrom FOR CopyBytes TO iplcData NO-CONVERT NO-ERROR.

          if BytesFrom = 1 and CopyBytes <  FileSize and UseTransaction then
          do:
            if objConnection:startTransaction (FileName) = false then 
            do:
              handleError(1, "Producer on " + cDestQueue + ": Socket communication error", ?).
              RETURN FALSE.
            end.
          end.

          DEFINE VARIABLE objSendFrame AS Stomp.Frame NO-UNDO.
          ASSIGN objSendFrame    = Stomp.FrameFactory:makeSendFrame(INPUT THIS-OBJECT:cDestQueue, INPUT iplcData, INPUT objLogger)
                 iStatus         = {&STATUS_RECEIPT_WAIT}
                 cReceiptWaiting = objSendFrame:addReceipt().
                 lReceiptProcOK    = FALSE.
          /* Attach input header data */
          IF objSendFrame:addHeaderData(INPUT TABLE-HANDLE iphHeaders,
                                        INPUT ipcTTHdrDataCol) THEN 
          DO:
            FilePartNumb = FilePartNumb + 1.
            objSendFrame:addHeaderData ("FilePartNumb", STRING(FilePartNumb)).
            objSendFrame:addHeaderData ("FilePart", STRING(BytesTo)).
    
            if valid-object (objLogger) then
              objLogger:writeEntry(2, "Producer on " + cDestQueue + ": Sending frame to server and" +
                                    " waiting for a response (receipt = '" + cReceiptWaiting + "')...").
            /* Send the frame */
            sf = objConnection:sendFrame(objSendFrame).
            DELETE OBJECT objSendFrame.

            if not sf then
            do:
              handleError(1, "Producer on " + cDestQueue + ": Socket communication error", ?).
              RETURN FALSE.
            end.

            iplcData = "".
            objConnection:waitForSocket({&SOCKET_WAIT_TIME}).
            /* Note: receipt checking is done in processReceipt method */
            IF lReceiptProcOK THEN DO:
              if valid-object (objLogger) then
                objLogger:writeEntry(1, "Producer on " + cDestQueue + ": STOMP server acknowledged receipt of transmitted data.").
                THIS-OBJECT:setStatusToReady().
            END.
            ELSE DO:
              if valid-object (objLogger) then
                objLogger:writeEntry(1, "Producer on " + cDestQueue + ": STOMP server either did not acknowledge receipt of " +
                                        "transmitted data, or request timed out after {&SOCKET_WAIT_TIME} seconds limit reached.").
              THIS-OBJECT:setStatusToReady(). /* So we can do another send (although best course of action is probably to shut down) */
              if CopyBytes <> FileSize and UseTransaction then
                objConnection:abortTransaction ().
              RETURN FALSE.
            END.
          END. /* IF objSendFrame:addHeaderData */
          ELSE DO:
            if valid-object (objLogger) then
              objLogger:writeError(1, "Producer on " + cDestQueue + ": Could not attach headers to frame!").
            if CopyBytes <> FileSize and UseTransaction then
              objConnection:abortTransaction ().
            RETURN FALSE.
          END.
          BytesFrom = BytesFrom + CopyBytes.
          if BytesFrom > FileSize then leave.
        end.
        DELETE OBJECT iphHeaders. /* Clean up dynamic temp-table memory */
        if CopyBytes <> FileSize and UseTransaction then
          objConnection:commitTransaction ().
        RETURN TRUE.
    END METHOD.


    /* Write a raw STOMP frame to the queue, verifying that it was received      */
    /* This method relies on the caller creating the STOMP Frame object manually */
    /* NOTE: Caller should delete Frame object when this method returns!         */
    METHOD PUBLIC LOGICAL send(INPUT ipobjSendFrame AS Stomp.Frame):
        define variable sf as logical.
        /* Prepare the Frame for sending */
        ASSIGN iStatus = {&STATUS_RECEIPT_WAIT}.
        /* Add a receipt-id, if one is not already defined */
        IF ipobjSendFrame:getReceipt() EQ ? THEN
            ASSIGN cReceiptWaiting = ipobjSendFrame:addReceipt().
        objLogger:writeEntry(2, "Producer on " + cDestQueue + ": Sending frame to server and" +
                                " waiting for a response (receipt = '" + cReceiptWaiting + "')...").
        /* Send the frame */
        sf = objConnection:sendFrame(ipobjSendFrame).
        if not sf then
        do:
          handleError(1, "Producer on " + cDestQueue + ": Socket communication error", ?).
          RETURN FALSE.
        end.
        objConnection:waitForSocket({&SOCKET_WAIT_TIME}).
        /* Note: receipt checking is done in processReceipt method */
        IF lReceiptProcOK THEN DO:
            objLogger:writeEntry(1, "Producer on " + cDestQueue + ": STOMP server acknowledged receipt of transmitted data.").
            THIS-OBJECT:setStatusToReady().
            RETURN TRUE.
        END.
        ELSE DO:
            objLogger:writeEntry(1, "Producer on " + cDestQueue + ": STOMP server either did not acknowledge receipt of " +
                                    "transmitted data, or request timed out after {&SOCKET_WAIT_TIME} seconds limit reached.").
            THIS-OBJECT:setStatusToReady(). /* So we can do another send (although best course of action is probably to shut down) */
            RETURN FALSE.
        END.
    END METHOD.


    /* Process MESSAGE frames from STOMP server.  Producer should have no reason to receive a MESSAGE frame. Throw error */
    METHOD PUBLIC OVERRIDE VOID processMessage(INPUT ipobjFrameMsg AS Stomp.Frame):
        /* Bad frame type received */
        handleError(1,
                    "Producer on " + cDestQueue + ": Received a Frame that should NOT have" +
                      " been delivered to it! Received frame type '" + ipobjFrameMsg:getFrameType() + "'",
                    ipobjFrameMsg).
    END METHOD.


    /* Process RECEIPT frames from STOMP server.  Producer assumes all receipts are acknowledgements of SEND frames */
    METHOD PUBLIC OVERRIDE VOID processReceipt(INPUT ipobjFrameMsg AS Stomp.Frame):
        IF iStatus EQ {&STATUS_RECEIPT_WAIT} THEN DO:
            /* Producer is expecting a receipt; process it */
            IF ipobjFrameMsg:getReceipt() EQ cReceiptWaiting THEN
                /* Receipt received was expected value; clear receipt wait status */
                ASSIGN cReceiptWaiting = ""
                       lReceiptProcOK  = TRUE.
            ELSE
                /* Receipt received was NOT expected value! Throw fatal error. */
                handleError(1,
                            "Producer on " + cDestQueue + ": Received an unexpected receipt! Received receipt-id '" +
                              ipobjFrameMsg:getReceipt() + "', was expecting '" + cReceiptWaiting + "'",
                            ipobjFrameMsg).
        END.
        ELSE
            /* Producer was not expecting a receipt! Throw critical error. */
            handleError(2,
                        "Producer on " + cDestQueue + ": Received an unexpected receipt! Receipt-id is '" + 
                          ipobjFrameMsg:getReceipt() + "'",
                        ipobjFrameMsg).
    END METHOD.
 
    
    /* Setters */
    
    METHOD PUBLIC VOID setStatusToReady():
        ASSIGN iStatus = {&STATUS_READY}.
    END METHOD.
    
    METHOD PUBLIC VOID setStatusToWait():
        ASSIGN iStatus = {&STATUS_RECEIPT_WAIT}.
    END METHOD.

    /* Getter */

    METHOD PUBLIC LOGICAL getStatusIsReady():
        RETURN (iStatus EQ {&STATUS_READY}).
    END METHOD.
    
END CLASS.
