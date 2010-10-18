 
 /*------------------------------------------------------------------------
    File        : Queue
    Purpose     : Abstraction of a Stomp destination (class name derived
	              from original focus on ActiveMQ queues).  Intended to be an 
                  abstract superclass of the Consumer and Producer
                  (methods processMessage and processReceipt needing to be
                  implemented by those subprocedures), however, PROGRESS
                  has no keywords to enforce ABSTRACT methods being
                  implemented in subclasses.
    Author(s)   : Abe Voelker
    Created     : Sun Oct 04 15:53:12 CDT 2009
    Notes       : 
  ----------------------------------------------------------------------*/

USING Progress.Lang.*.

CLASS Stomp.Queue: 
    DEFINE PROTECTED VARIABLE objConnection     AS Stomp.Connection NO-UNDO.
    DEFINE PROTECTED VARIABLE cDestQueue        AS CHARACTER        NO-UNDO.
    DEFINE PROTECTED VARIABLE objLogger         AS Stomp.Logger     NO-UNDO.

    DEFINE PROTECTED VARIABLE cErrorProcedureName    AS CHARACTER NO-UNDO.
    DEFINE PROTECTED VARIABLE hErrorProcedurePointer AS HANDLE    NO-UNDO.


    CONSTRUCTOR PROTECTED Queue(INPUT ipcDest          AS CHARACTER,
                                INPUT ipobjLogger      AS Stomp.Logger,
                                INPUT ipcErrorProcName AS CHARACTER,
                                INPUT iphErrorProcPtr  AS HANDLE):
	    ASSIGN THIS-OBJECT:cDestQueue             = ipcDest
		       THIS-OBJECT:objLogger              = ipobjLogger
               THIS-OBJECT:cErrorProcedureName    = ipcErrorProcName
               THIS-OBJECT:hErrorProcedurePointer = iphErrorProcPtr.
    END CONSTRUCTOR.


    DESTRUCTOR PUBLIC Queue():
      THIS-OBJECT:disconnect().
	END DESTRUCTOR.


    /* Connect the queue object to a STOMP server */
    METHOD PUBLIC LOGICAL connect(INPUT ipcServer   AS CHARACTER,
                                  INPUT ipiPort     AS INTEGER,
                                  INPUT ipcUsername AS CHARACTER,
                                  INPUT ipcPassword AS CHARACTER):
        ASSIGN objConnection = NEW Stomp.Connection(objLogger,
                                                    ipcServer,
                                                    ipiPort,
                                                    ipcUsername,
                                                    ipcPassword).
        IF objConnection EQ ? THEN
            RETURN FALSE.
        /* Register the Queue with the Connection */
	IF NOT objConnection:setQueue(THIS-OBJECT) THEN DO:
            DELETE OBJECT objConnection.
            RETURN FALSE.
        END.
        /* Connect to server holding target queue */
        RETURN objConnection:connect().
    END METHOD.

    METHOD PUBLIC LOGICAL connect(INPUT ipcServer   AS CHARACTER,
                                  INPUT ipiPort     AS INTEGER,
                                  INPUT ipcUsername AS CHARACTER,
                                  INPUT ipcPassword AS CHARACTER,
                                  INPUT Client      AS CHARACTER):
        ASSIGN objConnection = NEW Stomp.Connection(objLogger,
                                                    ipcServer,
                                                    ipiPort,
                                                    ipcUsername,
                                                    ipcPassword, 
                                                    Client).
        IF objConnection EQ ? THEN
            RETURN FALSE.
        /* Register the Queue with the Connection */
	IF NOT objConnection:setQueue(THIS-OBJECT) THEN 
        DO:
            DELETE OBJECT objConnection.
            RETURN FALSE.
        END.
        /* Connect to server holding target queue */
        RETURN objConnection:connect().
    END METHOD.


    /* Disconnect the queue object from the STOMP server */
    METHOD PUBLIC LOGICAL disconnect():
        IF objConnection NE ? /* AND
           NOT objConnection:isConnected() */ THEN
            RETURN objConnection:disconnect().
        ELSE DO:
            objLogger:writeError(3, "Queue object disconnect() method called, but Connection object either " + 
                                    "doesn't exist or is not connected to a server!").
            RETURN TRUE.
        END.
    END METHOD.
    

    /* Inheriting classes *should* override the following two methods! */
    /* NOTE: These are supposed to be abstract methods, but PROGRESS currently has no ABSTRACT keyword to */
    /*       force subclasses to override these methods at compile-time.  Does it surprise you?           */
    METHOD PUBLIC VOID processMessage(INPUT ipobjFrameMsg AS Stomp.Frame): END METHOD.
    METHOD PUBLIC VOID processReceipt(INPUT ipobjFrameMsg AS Stomp.Frame): END METHOD.
    

    METHOD PROTECTED VOID handleError(INPUT ipiErrorLevel AS INTEGER,
                                      INPUT ipcError      AS CHARACTER,
                                      INPUT ipobjFrame    AS Stomp.Frame):
        /* Throw error to user-supplied error handling procedure */
        IF VALID-HANDLE(hErrorProcedurePointer) THEN
            RUN VALUE(cErrorProcedureName) 
                IN hErrorProcedurePointer
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
    
    METHOD PUBLIC CHARACTER getDestinationName():
        RETURN THIS-OBJECT:cDestQueue.
    END METHOD.


    METHOD PUBLIC LOGICAL isConnected():
        IF objConnection EQ ? THEN
            RETURN FALSE.
        ELSE
            RETURN objConnection:isConnected().
    END METHOD.


    METHOD PUBLIC HANDLE getSocketHandle():
        IF objConnection EQ ? THEN
            RETURN ?.
        ELSE
	        RETURN objConnection:getSocket().
	END METHOD.


    METHOD PUBLIC HANDLE getErrorProcessHandle():
        RETURN THIS-OBJECT:hErrorProcedurePointer.
    END METHOD.
    

    METHOD PUBLIC CHARACTER getErrorProcessName():
        RETURN THIS-OBJECT:cErrorProcedureName.
    END METHOD.

END CLASS.
