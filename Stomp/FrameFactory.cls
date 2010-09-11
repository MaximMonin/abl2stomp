 
 /*------------------------------------------------------------------------
    File        : FrameFactory
    Purpose     : Class for simplifying the building of known Frame types.
    Description : Uses Factory Method OO design pattern.
    Author(s)   : Abe Voelker
    Created     : Tue Sep 15 15:35:57 CDT 2009
    Notes       : 
  ----------------------------------------------------------------------*/

USING Progress.Lang.*.

CLASS Stomp.FrameFactory: 

    /* DISCONNECT Frame */
    METHOD STATIC FINAL Stomp.Frame makeDisconnectFrame(INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("DISCONNECT").
        RETURN objFrame.
    END METHOD.
    
    /* CONNECT Frame */
    METHOD STATIC FINAL Stomp.Frame makeConnectFrame(INPUT ipcUsername   AS CHARACTER,
                                                     INPUT ipcPassword   AS CHARACTER,
                                                     INPUT ipobjLogger   AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("CONNECT").
        objFrame:setHeaderData(("login|" + ipcUsername + "|" +
                               "passcode|" + ipcPassword), "|").
        RETURN objFrame.
    END METHOD.
    
    /* SEND Frame */
    METHOD STATIC FINAL Stomp.Frame makeSendFrame(INPUT ipcDest     AS CHARACTER,
                                                  INPUT iplcData    AS LONGCHAR,
                                                  INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("SEND").
        objFrame:setHeaderData(("destination|" + ipcDest), "|").
        objFrame:setMessageData(iplcData).
        RETURN objFrame.
    END METHOD.
    
    /* SUBSCRIBE Frame that defaults to client ACK mode */
    METHOD STATIC FINAL Stomp.Frame makeSubscribeFrame(INPUT ipcDest     AS CHARACTER,
                                                       INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("SUBSCRIBE").
        objFrame:setHeaderData(("destination|" + ipcDest + "|" +
                                "ack|client|" +
                                "activemq.prefetchSize|1"), "|").
        RETURN objFrame.
    END METHOD.
    
    /* SUBSRIBE Frame with optional ack mode header (T=client, F=auto) */
    METHOD STATIC FINAL Stomp.Frame makeSubscribeFrame(INPUT ipcDest     AS CHARACTER,
                                                       INPUT iplAckMode  AS LOGICAL,
                                                       INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("SUBSCRIBE").
        IF iplAckMode THEN
            objFrame:setHeaderData(("destination|"  + ipcDest + "|" +
                                    "ack|client|" +
                                    "activemq.prefetchSize|1"), "|").
        ELSE
            objFrame:setHeaderData(("destination|"  + ipcDest + "|" +
                                    "ack|auto|" +
                                    "activemq.prefetchSize|1"), "|").
        RETURN objFrame.
    END METHOD.
    
    /* SUBSRIBE Frame with ACK hdr and a selector */
    METHOD STATIC FINAL Stomp.Frame makeSubscribeFrame(INPUT ipcDest     AS CHARACTER,
                                                       INPUT iplAckMode  AS LOGICAL,
                                                       INPUT ipcSelector AS CHARACTER,
                                                       INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("SUBSCRIBE").
        IF iplAckMode THEN
            objFrame:setHeaderData(("destination|"  + ipcDest + "|" +
                                    "ack|client|" +
                                    "activemq.prefetchSize|1|" +
                                    "selector|" + ipcSelector), "|").
        ELSE
            objFrame:setHeaderData(("destination|"  + ipcDest + "|" +
                                    "ack|auto|" +
                                    "activemq.prefetchSize|1|" +
                                    "selector|" + ipcSelector), "|").
        RETURN objFrame.
    END METHOD.
    
    /* UNSUBSCRIBE Frame */
    METHOD STATIC FINAL Stomp.Frame makeUnsubscribeFrame(INPUT ipcDest     AS CHARACTER,
                                                         INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("UNSUBSCRIBE").
        objFrame:setHeaderData(("destination|" + ipcDest), "|").
        RETURN objFrame.
    END METHOD.
    
    /* ACK Frame */
    METHOD STATIC FINAL Stomp.Frame makeAckFrame(INPUT ipcMesgID   AS CHARACTER,
                                                 INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("ACK").
        objFrame:setHeaderData(("message-id|" + ipcMesgID), "|").
        RETURN objFrame.
    END METHOD.
    
    /* BEGIN Frame */
    METHOD STATIC FINAL Stomp.Frame makeBeginFrame(INPUT ipcTransID  AS CHARACTER,
                                                   INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("BEGIN").
        objFrame:addTransaction(ipcTransID).
        RETURN objFrame.
    END METHOD.
    
    /* COMMIT Frame */
    METHOD STATIC FINAL Stomp.Frame makeCommitFrame(INPUT ipcTransID  AS CHARACTER,
                                                    INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("COMMIT").
        objFrame:addTransaction(ipcTransID).
        RETURN objFrame.
    END METHOD.
    
    /* ABORT Frame */
    METHOD STATIC FINAL Stomp.Frame makeAbortFrame(INPUT ipcTransID  AS CHARACTER,
                                                   INPUT ipobjLogger AS Stomp.Logger):
        DEFINE VARIABLE objFrame AS Stomp.Frame NO-UNDO.
        ASSIGN objFrame = NEW Stomp.Frame(ipobjLogger).
        objFrame:setFrameType("ABORT").
        objFrame:addTransaction(ipcTransID).
        RETURN objFrame.
    END METHOD.
END CLASS.
