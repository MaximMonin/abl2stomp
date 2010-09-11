
 /*------------------------------------------------------------------------
    File        : SocketReader.p
    Purpose     : Read-response procedure for the socket connection.
    Description : 
    Author(s)   : Abe Voelker
    Created     : Mon Oct 12 11:14:52 CDT 2009
    Notes       : Runs as a persistent procedure for constant socket access.
  ----------------------------------------------------------------------*/


/* LIMIT SOCKET READS TO 30K */
&SCOPED-DEFINE BUFFER_MAX 30000

/*--------------------------------------------------------------------------*/
/*                             DEFINE VARIABLES                             */
/*--------------------------------------------------------------------------*/

DEFINE INPUT PARAMETER objConnection      AS Stomp.Connection NO-UNDO.
DEFINE INPUT PARAMETER ipobjLogger        AS Stomp.Logger     NO-UNDO.
DEFINE VARIABLE        lcFullResponseData AS LONGCHAR         NO-UNDO.
/*--------------------------------------------------------------------------*/
/*                                PROCEDURES                                */
/*--------------------------------------------------------------------------*/

/* ReadSocketResponse: READS RESPONSE FROM SOCKET */
PROCEDURE ReadSocketResponse:
    DEFINE VARIABLE mResponseData      AS MEMPTR    NO-UNDO.
    DEFINE VARIABLE iBytesAvailable    AS INTEGER   NO-UNDO.
    DEFINE VARIABLE cResponseData      AS CHARACTER NO-UNDO.
    DEFINE VARIABLE iStartReadPosition AS INTEGER   NO-UNDO.
    DEFINE VARIABLE iResponseLength    AS INTEGER   NO-UNDO.
     
    SELF:SENSITIVE = NO.
    ERROR-STATUS:ERROR = NO.

    iBytesAvailable = MINIMUM(SELF:GET-BYTES-AVAILABLE(), {&BUFFER_MAX}) NO-ERROR.

    IF ERROR-STATUS:ERROR THEN
        ipobjLogger:writeError(1, "SocketReader: Getting bytes available").
    ELSE DO:
        ipobjLogger:writeEntry(4, "SocketReader: Bytes available=" + STRING(iBytesAvailable)).        
        
        SET-SIZE(mResponseData) = 0.
        SET-SIZE(mResponseData) = iBytesAvailable + 1.
        
        ipobjLogger:writeEntry(4, "SocketReader: Reading response...").
        
        ERROR-STATUS:ERROR = NO.
    
        SELF:READ(mResponseData, 1, iBytesAvailable, READ-EXACT-NUM) NO-ERROR.
        
        IF ERROR-STATUS:ERROR THEN DO:
            ipobjLogger:writeError(1, "SocketReader: Reading from socket").
            SET-SIZE(mResponseData) = 0.
        END. /* ERROR-STATUS:ERROR */
        ELSE DO: /* ELSE / ERROR-STATUS:ERROR */
        
            /* IT MIGHT TAKE MULTIPLE SOCKET READS TO GET A GIVEN MESSAGE IN */
            /* ITS ENTIRITY, SO EACH SOCKET READ MUST BE INTERROGATED TO     */
            /* DETERMINE IF IT IS THE START OF A NEW MESSAGE OR PART OF THE  */
            /* PREVIOUS MESSAGE.                                             */

            iStartReadPosition = 1.
            
            DO WHILE iStartReadPosition < iBytesAvailable:

                cResponseData   = GET-STRING(mResponseData, iStartReadPosition).
                iResponseLength = LENGTH(cResponseData, "RAW").

                ipobjLogger:writeEntry(4, "SocketReader: Start pos=" + STRING(iStartReadPosition) + "," +
                                      "Line length=" + STRING(iResponseLength) + "," +
                                      "Read data=" + cResponseData).

                
                
                ASSIGN
                    iStartReadPosition = iStartReadPosition + iResponseLength
                    lcFullResponseData = lcFullResponseData  + cResponseData
                    cResponseData      = "".
                
                IF iStartReadPosition <= iBytesAvailable AND
                   GET-BYTE(mResponseData, iStartReadPosition) = 0 THEN DO:
                    DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
                    ASSIGN objFrame = NEW Stomp.Frame(INPUT lcFullResponseData, INPUT ipobjLogger).
                    lcFullResponseData = "".
                    objConnection:routeFrame(objFrame).
                    DELETE OBJECT objFrame.
                END.

                iStartReadPosition = iStartReadPosition + 1.
            END.

            SET-SIZE(mResponseData) = 0.
        END. /* not ERROR-STATUS:ERROR */
    END. /* not ERROR-STATUS:ERROR */
    
    SELF:SENSITIVE = YES.

END PROCEDURE. /* ReadSocketResponse */
