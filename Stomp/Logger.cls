 
 /*------------------------------------------------------------------------
    File        : Logger
    Purpose     : Simple logging class.  This class used to be based on a 
                  simple Singleton design pattern, but has since been wrapped
                  by the LoggerMultiton class to more easily spawn multiple,
                  different Logger objects.
    Author(s)   : Abe Voelker
    Created     : Wed Sep 09 13:17:03 CDT 2009
    Notes       : * This class should only be instantialized from a
                    LoggerMultiton object!
                  * The output file is opened and closed each write so we can
                    read the log file while the process is running and know
                    that we are reading the full file (don't have to wait
                    for the buffer to fill up because it is flushed on each
                    OUTPUT CLOSE call).
  ----------------------------------------------------------------------*/

USING Progress.Lang.*.

&SCOPED-DEFINE NEWLINE CHR(10)

CLASS Stomp.Logger:
    DEFINE PRIVATE        STREAM   sFile.
    DEFINE PRIVATE        VARIABLE cLogLocation  AS CHARACTER       NO-UNDO.
    DEFINE PRIVATE        VARIABLE iMaxEntryLvl  AS INTEGER         NO-UNDO.
    DEFINE PRIVATE        VARIABLE iMaxErrorLvl  AS INTEGER         NO-UNDO.

    CONSTRUCTOR PUBLIC Logger(INPUT ipcLogFileName AS CHARACTER,
                              INPUT ipiMaxEntryLvl AS INTEGER,
                              INPUT ipiMaxErrorLvl AS INTEGER):
        ASSIGN cLogLocation = ipcLogFileName
               iMaxEntryLvl = ipiMaxEntryLvl
               iMaxErrorLvl = ipiMaxErrorLvl.
    END CONSTRUCTOR.
    
    /* Write CHARACTER message */
    METHOD PRIVATE VOID writeLog(INPUT ipcMsg AS CHAR):
        OUTPUT STREAM sFile TO VALUE(cLogLocation) APPEND.
        PUT    STREAM sFile UNFORMATTED REPLACE(ipcMsg, {&NEWLINE}, "") SKIP.
        OUTPUT STREAM sFile CLOSE.
    END METHOD.
    
    /* Write LONGCHAR message */
    METHOD PUBLIC VOID writeLogLC(INPUT iplcMsg AS LONGCHAR):
        OUTPUT STREAM sFile TO VALUE(cLogLocation) APPEND.
        EXPORT STREAM sFile iplcMsg.
        PUT    STREAM sFile SKIP.
        OUTPUT STREAM sFile CLOSE.
    END METHOD.
    
    METHOD PUBLIC VOID writeEntry(INPUT ipiEntryLevel AS INT, INPUT ipcMsg AS CHAR):
        IF ipiEntryLevel LE iMaxEntryLvl THEN
            writeLog(STRING(DATETIME(TODAY, MTIME), "99/99/9999 HH:MM:SS.SSS+HH:MM")
                            + " - " + ipcMsg).
    END METHOD.
    
    METHOD PUBLIC VOID writeError(INPUT ipiErrorLevel AS INT, INPUT ipcMsg AS CHAR):
        IF ipiErrorLevel LE iMaxErrorLvl THEN
            writeLog(STRING(DATETIME(TODAY, MTIME), "99/99/9999 HH:MM:SS.SSS+HH:MM")
                            + " - ERROR " + ipcMsg).
    END METHOD.

    METHOD PUBLIC VOID dumpFrame(INPUT ipobjFrame AS Stomp.Frame):
        DEFINE VARIABLE lcMessage    AS LONGCHAR  NO-UNDO.
        ASSIGN lcMessage = STRING(DATETIME(TODAY, MTIME), "99/99/9999 HH:MM:SS.SSS+HH:MM") + " - RAW FRAME DUMP:" + {&NEWLINE} + 
                          "===================="  + {&NEWLINE} +
                          ipobjFrame:toLongChar() + {&NEWLINE} +
                          "===================="  + {&NEWLINE}.
        writeLogLC(lcMessage).
    END METHOD.
END CLASS.
