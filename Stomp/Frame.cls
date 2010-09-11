 
 /*------------------------------------------------------------------------
    File        : Frame
    Purpose     : An object for holding a frame - frame type, the headers,
                  and the frame payload (message).
    Author(s)   : Abe Voelker
    Created     : Mon Sep 14 12:49:47 CDT 2009
    Notes       : 
  ----------------------------------------------------------------------*/

USING Progress.Lang.*.

&SCOPED-DEFINE NEWLINE          CHR(10)
&SCOPED-DEFINE NULL             CHR(00)
&SCOPED-DEFINE DELIMITER        |

CLASS Stomp.Frame:
    DEFINE PRIVATE VARIABLE cFrameType AS CHARACTER NO-UNDO.
    DEFINE PRIVATE TEMP-TABLE ttHeaders NO-UNDO
      FIELDS cName AS CHARACTER
      FIELDS cData AS CHARACTER.
    DEFINE PRIVATE VARIABLE lcMessageData AS LONGCHAR NO-UNDO.
    
    DEFINE PRIVATE VARIABLE objRecGen     AS Stomp.ReceiptGenerator NO-UNDO.
    DEFINE PRIVATE VARIABLE objLogger     AS Stomp.Logger           NO-UNDO.
    

    CONSTRUCTOR PUBLIC Frame(INPUT ipobjLogger AS Stomp.Logger):
        ASSIGN objLogger = ipobjLogger
               objRecGen = Stomp.ReceiptGenerator:getInstance().
    END CONSTRUCTOR.
    
    /* Construct a Frame from raw LONGCHAR data */
    CONSTRUCTOR PUBLIC Frame(INPUT lcRaw AS LONGCHAR, INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE cNxtHdr AS CHARACTER NO-UNDO.
        DEFINE VARIABLE cHdrName AS CHARACTER NO-UNDO.
        DEFINE VARIABLE cHdrData AS CHARACTER NO-UNDO.
        
        DEFINE VARIABLE lcHeaders AS LONGCHAR NO-UNDO.
        DEFINE VARIABLE lcMessage AS LONGCHAR NO-UNDO.
        
        DEFINE VARIABLE i AS INTEGER NO-UNDO.
        DEFINE VARIABLE iFrmTypeBoundary AS INTEGER NO-UNDO.
        DEFINE VARIABLE iHdrBoundary AS INTEGER NO-UNDO.
        DEFINE VARIABLE iEndBoundary AS INTEGER NO-UNDO.
        
        ASSIGN objLogger = ipobjLogger.
        objLogger:WriteEntry (4,string(LENGTH(lcRaw))).
        /* Fix for socket data starting with an extraneous newline character */
        IF INDEX(lcRaw, {&NEWLINE}, 1) EQ 1 THEN
            ASSIGN lcRaw = SUBSTRING(lcRaw, 2).
        
    	/* Get the frame type: */
        ASSIGN iFrmTypeBoundary = INDEX(lcRaw, {&NEWLINE}, 1).
        IF iFrmTypeBoundary GT 0 THEN
            ASSIGN cFrameType = SUBSTRING(lcRaw, 1, (iFrmTypeBoundary - 1)).
    	/* Get each header */
        ASSIGN iHdrBoundary = INDEX(lcRaw, ({&NEWLINE} + {&NEWLINE}), 1).
        
        IF iFrmTypeBoundary EQ iHdrBoundary THEN DO:
            /* No headers! */
            ASSIGN iEndBoundary = INDEX(lcRaw, {&NULL}, iFrmTypeBoundary).
            IF iEndBoundary EQ 0 THEN
                ASSIGN iEndBoundary = LENGTH(lcRaw) + 1.
            ASSIGN lcMessage = SUBSTRING(lcRaw, (iFrmTypeBoundary + 1), (iEndBoundary - (iFrmTypeBoundary + 1))).
        END.
        ELSE DO:
            /* Headers exist */
            ASSIGN lcHeaders = SUBSTRING(lcRaw, (iFrmTypeBoundary + 1), (iHdrBoundary - (iFrmTypeBoundary + 1))).
            DO i=1 TO NUM-ENTRIES(lcHeaders, {&NEWLINE}):
                ASSIGN cNxtHdr  = ENTRY(i, lcHeaders, {&NEWLINE}).
                IF INDEX(cNxtHdr, ":") EQ 0 THEN
                    objLogger:writeError(1, "FRAME: Unable to parse message header!").
                ELSE DO TRANSACTION:
                    CREATE ttHeaders.
                    ASSIGN ttHeaders.cName = TRIM(SUBSTRING(cNxtHdr, 1, (INDEX(cNxtHdr, ":") - 1)))
                           ttHeaders.cData = TRIM(SUBSTRING(cNxtHdr, (INDEX(cNxtHdr, ":") + 1))).
                END. /* TRANSACTION */
            END.
            
            /* Read until the end of the frame to get Message data */
            ASSIGN iEndBoundary = LENGTH(lcRaw) + 1.
            ASSIGN lcMessageData = SUBSTRING(lcRaw, (iHdrBoundary + 2), (iEndBoundary - (iHdrBoundary + 1))).
        END.
    END CONSTRUCTOR.

    /* Dump a Frame into raw LONGCHAR */
    METHOD PUBLIC LONGCHAR toLongChar():
        DEFINE VARIABLE lcString AS LONGCHAR NO-UNDO.
        ASSIGN lcString = cFrameType + {&NEWLINE}.
        FOR EACH ttHeaders NO-LOCK:
            ASSIGN lcString = lcString + ttHeaders.cName + ":"
                              + ttHeaders.cData + {&NEWLINE}.
        END.
        ASSIGN lcString = lcString + {&NEWLINE} /* header/payload separator */
                          + lcMessageData + {&NULL}.
        RETURN lcString.
    END METHOD.
    
    /* Getters: */
    METHOD PUBLIC CHARACTER getFrameType():
        RETURN cFrameType.
    END METHOD.
    
    METHOD PUBLIC LONGCHAR  getMessageData():
        RETURN lcMessageData.
    END METHOD.

    METHOD PUBLIC CHARACTER getReceipt():
        RETURN THIS-OBJECT:getHeaderValue(INPUT "receipt-id").
    END METHOD.
    
    METHOD PUBLIC HANDLE getHeaderData():
        RETURN TEMP-TABLE ttHeaders:HANDLE.
    END METHOD.
    
    METHOD PUBLIC CHARACTER getHeaderValue(INPUT ipcHeaderName AS CHARACTER):
        FIND FIRST ttHeaders
          WHERE ttHeaders.cName EQ ipcHeaderName
          NO-LOCK NO-ERROR.
        IF AVAILABLE ttHeaders THEN
            RETURN ttHeaders.cData.
        ELSE
            RETURN ?.
    END METHOD.
    
    /* Setters: */
    METHOD PUBLIC VOID setFrameType(INPUT cFrmType AS CHARACTER):
        ASSIGN cFrameType = cFrmType.
    END METHOD.


    /* Set header data from a delimited string list */
    METHOD PUBLIC VOID setHeaderData(INPUT lcHdrData AS LONGCHAR, INPUT cDelim AS CHARACTER):
        DEFINE VARIABLE i AS INTEGER NO-UNDO.
        EMPTY TEMP-TABLE ttHeaders NO-ERROR. /* Clear any old headers out */
        /* Only process if there is an even number of items */
        IF (NUM-ENTRIES(lcHdrData, cDelim) MODULO 2) EQ 0 THEN DO:
            DO i=1 TO NUM-ENTRIES(lcHdrData, cDelim) BY 2:
                CREATE ttHeaders.
                ASSIGN ttHeaders.cName = ENTRY(i, lcHdrData, cDelim)
                       ttHeaders.cData = ENTRY((i + 1), lcHdrData, cDelim).
            END.
        END.        
    END METHOD.
    

    /* Add header data from a temp-table.  Example call:             */
    /* objFrame:setHeaderData(ttHdrs, "cHeaderData").                */
    /* Put Header Name in extent 1, Header Value in extent 2 , e.g.: */
    /*    ttHdrs.cHeaderData[1] = "DestinationLocation"              */
    /*    ttHdrs.cHeaderData[2] = "ECMM"                             */
    METHOD PUBLIC LOGICAL addHeaderData(INPUT TABLE-HANDLE iphHeaders,
                                        INPUT ipcTTHdrDataCol AS CHARACTER):
        DEFINE VARIABLE hqryDataInput AS HANDLE NO-UNDO.
        DEFINE VARIABLE hbfDataInput  AS HANDLE NO-UNDO.
        DEFINE VARIABLE hbfFieldHdrData AS HANDLE NO-UNDO.
        
        /* Verify the table is OK to be parsed: */
        IF NOT iphHeaders:PREPARED THEN
            RETURN FALSE.
        
        /* The record buffer for each record retrieved */
        ASSIGN hbfDataInput = iphHeaders:DEFAULT-BUFFER-HANDLE.
        /* The buffer-fields representing the header name/data columns */
        ASSIGN hbfFieldHdrData = hbfDataInput:BUFFER-FIELD(ipcTTHdrDataCol).
        /* Create the query that will access the table */
        CREATE QUERY hqryDataInput.
        hqryDataInput:SET-BUFFERS(hbfDataInput).
    	/* Prepare the query to create the result list */
        hqryDataInput:QUERY-PREPARE("FOR EACH " + iphHeaders:NAME + " NO-LOCK").
        hqryDataInput:QUERY-OPEN().
        
        /* Copy each record into this object: */
        repblk:
        REPEAT:
            hqryDataInput:GET-NEXT(NO-LOCK). /* Attempt to get next record */
            IF (hqryDataInput:QUERY-OFF-END) THEN
                LEAVE repblk. /* Hit the end of the query */
            CREATE ttHeaders.
            ASSIGN ttHeaders.cName = hbfFieldHdrData:BUFFER-VALUE(1)
                   ttHeaders.cData = hbfFieldHdrData:BUFFER-VALUE(2).
        END.
        hqryDataInput:QUERY-CLOSE().
        DELETE OBJECT hqryDataInput.
        DELETE OBJECT iphHeaders. /* Clean up dynamic temp-table memory */
        RETURN TRUE.
    END METHOD.


    /* Add a header data/value pair (overwrites previous header data) */
    METHOD PUBLIC VOID addHeaderData(INPUT cHeaderName AS CHARACTER,
                                     INPUT cHeaderData AS CHARACTER):
        FIND FIRST ttHeaders
          WHERE ttHeaders.cName EQ cHeaderName
          EXCLUSIVE-LOCK NO-ERROR.
        IF AVAILABLE ttHeaders THEN DO:
            ASSIGN ttHeaders.cData = cHeaderData.
        END.
        ELSE DO:
            CREATE ttHeaders.
            ASSIGN ttHeaders.cName = cHeaderName
                   ttHeaders.cData = cHeaderData.
        END.
    END METHOD.
    
    METHOD PUBLIC VOID setMessageData(INPUT lcMsgData AS LONGCHAR):
        ASSIGN lcMessageData = lcMsgData.
    END METHOD.
    
    /* Add a receipt request to the Frame */
    METHOD PUBLIC CHARACTER addReceipt():
        DO TRANSACTION:
            FIND FIRST ttHeaders
              WHERE ttHeaders.cName EQ "receipt"
              EXCLUSIVE-LOCK NO-ERROR.
            IF AVAILABLE ttHeaders THEN DO:
                ASSIGN ttHeaders.cData = objRecGen:getReceipt().
                RETURN ttHeaders.cData.
            END.
            ELSE DO:
                CREATE ttHeaders.
                ASSIGN ttHeaders.cName = "receipt"
                       ttHeaders.cData = objRecGen:getReceipt().
                RETURN ttHeaders.cData.
            END.
        END.
    END METHOD.
    
    /* Add a transaction ID to the Frame */
    METHOD PUBLIC VOID addTransaction(INPUT cTransID AS CHARACTER):
        IF (cFrameType EQ "BEGIN")  OR (cFrameType EQ "SEND") OR
           (cFrameType EQ "COMMIT") OR (cFrameType EQ "ABORT") OR
           (cFrameType EQ "ACK") THEN DO TRANSACTION:
            FIND FIRST ttHeaders
              WHERE ttHeaders.cName EQ "transaction"
              EXCLUSIVE-LOCK NO-ERROR.
            IF AVAILABLE ttHeaders THEN DO:
                ASSIGN ttHeaders.cData = cTransID.
            END.
            ELSE DO:
                CREATE ttHeaders.
                ASSIGN ttHeaders.cName = "transaction"
                       ttHeaders.cData = cTransID.
            END.
        END.
    END METHOD.

END CLASS.
