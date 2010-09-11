 
 /*------------------------------------------------------------------------
    File        : ReceiptGenerator
    Purpose     : Simple class utilizing Singleton design pattern to generate
                  unique receipt numbers for adding to Frames.
    Author(s)   : Abe Voelker
    Created     : Mon Sep 28 09:56:03 CDT 2009
    Notes       : 
  ----------------------------------------------------------------------*/

USING Progress.Lang.*.

&SCOPED-DEFINE INTEGER32_MAX 2147483647
&SCOPED-DEFINE INTEGER32_MIN -2147483648

CLASS Stomp.ReceiptGenerator FINAL:
    
    DEFINE PRIVATE STATIC VARIABLE objInstance   AS Stomp.ReceiptGenerator NO-UNDO.
    DEFINE PRIVATE STATIC VARIABLE lInitialized  AS LOGICAL                NO-UNDO.
    DEFINE PRIVATE        VARIABLE iReceiptID    AS INTEGER                NO-UNDO.

    /* Private constructor prevents instantiation from other classes */
    CONSTRUCTOR PRIVATE ReceiptGenerator():
        ASSIGN iReceiptID = {&INTEGER32_MIN}.
    END CONSTRUCTOR.
    
    DESTRUCTOR ReceiptGenerator():
        IF lInitialized THEN
            DELETE OBJECT objInstance.
    END DESTRUCTOR.
    
    /* Call this method when you need a "NEW" ReceiptGenerator object */
    METHOD PUBLIC STATIC Stomp.ReceiptGenerator getInstance():
        IF NOT lInitialized THEN
            ASSIGN lInitialized = TRUE
                   objInstance  = NEW Stomp.ReceiptGenerator().
        RETURN objInstance.
    END METHOD.
    
    /* Returns a "unique" receipt-id for use in verifying Frames sent to server */
    METHOD PUBLIC CHARACTER getReceipt():
        IF iReceiptID EQ {&INTEGER32_MAX} THEN
            ASSIGN iReceiptID = {&INTEGER32_MIN}.
        ELSE
            ASSIGN iReceiptID = iReceiptID + 1.
        RETURN STRING(iReceiptID, "-9999999999").
    END METHOD.

END CLASS.
