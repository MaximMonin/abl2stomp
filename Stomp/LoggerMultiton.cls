 
 /*------------------------------------------------------------------------
    File        : LoggerMultiton
    Purpose     : Class for managing Logger instances using the Multiton
                  design pattern.  Effectively acts as a filename->Logger
                  instance map.  Removes file I/O conflicts where multiple
                  objects in same ABL session need to write to same log file;
                  if multiple objects request access to the same log file,
                  they are given access to only the first Logger object
                  instance, guaranteeing synchronized file I/O.
    Author(s)   : Abe Voelker
    Created     : Thu Mar 04 13:17:03 CDT 2010
    Notes       : 
  ----------------------------------------------------------------------*/

USING Progress.Lang.*.

&SCOPED-DEFINE NEWLINE CHR(10)

CLASS Stomp.LoggerMultiton:

    DEFINE PRIVATE        STREAM   sFile.
    DEFINE PRIVATE        VARIABLE cLogLocation  AS CHARACTER       NO-UNDO.

    DEFINE PRIVATE STATIC TEMP-TABLE ttLoggerInstances NO-UNDO
      FIELD cFileName    AS CHARACTER CASE-SENSITIVE
      FIELD iMaxEntryLvl AS INTEGER
      FIELD iMaxErrorLvl AS INTEGER
      FIELD objInstance  AS OBJECT
      INDEX IXPK_LoggerInstances cFileName.

    /* Private constructor prevents instantiation from other classes */
    CONSTRUCTOR PRIVATE LoggerMultiton(): END CONSTRUCTOR.
    
    /* Call this method when you need a "NEW" Logger object */
    METHOD PUBLIC STATIC Stomp.Logger getLogger(INPUT ipcLogFileName AS CHARACTER,
                                                INPUT ipiMaxEntryLvl AS INTEGER,
                                                INPUT ipiMaxErrorLvl AS INTEGER):
        FIND FIRST ttLoggerInstances NO-LOCK
          WHERE ttLoggerInstances.cFileName EQ ipcLogFileName
          NO-ERROR.
        IF NOT AVAILABLE ttLoggerInstances THEN DO:
            CREATE ttLoggerInstances.
            ASSIGN ttLoggerInstances.cFileName    = ipcLogFileName
                   ttLoggerInstances.iMaxEntryLvl = ipiMaxEntryLvl
                   ttLoggerInstances.iMaxErrorLvl = ipiMaxErrorLvl
                   ttLoggerInstances.objInstance  = NEW Stomp.Logger(INPUT ipcLogFileName,
                                                                     INPUT ipiMaxEntryLvl,
                                                                     INPUT ipiMaxErrorLvl).
        END.
        RETURN CAST(ttLoggerInstances.objInstance, Stomp.Logger).
    END METHOD.

END CLASS.
