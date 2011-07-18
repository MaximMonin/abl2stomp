/*                                                       */
/* --------- -= Example procedure typedoc.p =- --------- */
/*                                                       */

define input  parameter ModuleParams as character.
define output parameter ResultFile   as character initial "".
define output parameter ResultString as character initial "".

/* ---------------- Begin procedure -------------------- */

{libs.i}

define variable hDS   as HANDLE     NO-UNDO.
define variable fn    as character  NO-UNDO.

fn = "temp/typedoc.xml".

define temp-table t_typedoc NO-UNDO
  field id-typedoc    as integer     FORMAT ">>>>>9" LABEL "Номер"
  field name-typedoc  as character   FORMAT "X(50)"  LABEL "Вид документа"
  field tpclass       as character   FORMAT "X(20)"  LABEL "Класс"
index i0 id-typedoc.

define temp-table t_class NO-UNDO
  field id-class      as integer     FORMAT ">>>>>9" LABEL "Класс"
  field name-class    as character   FORMAT "X(50)"  LABEL "Название класса"
index i0 id-class.

/* ------------------- procedure --------------------- */

for each typedoc no-lock:
  create t_typedoc.
  assign t_typedoc.id-typedoc   = typedoc.id-typedoc
         t_typedoc.name-typedoc = typedoc.name-typedoc.

  find first doc-class of typedoc no-lock no-error.
  if available doc-class 
     then t_typedoc.tpclass = doc-class.name-doc-class.
end.

for each doc-class no-lock:
  create t_class.
  assign t_class.id-class   = doc-class.id-doc-class
         t_class.name-class = doc-class.name-doc-class.
end.

/* -------------------- output ----------------------- */

CASE ModuleParams :

    WHEN "temptable" then do: /* Вывод 1 таблицы со сборкой : t_typedoc */
       hDS = TEMP-TABLE t_typedoc:HANDLE.
       run src/transfer/XmlWriteTable.p (fn, TABLE-HANDLE hDS).
       ResultFile = fn.
    end.

    WHEN "DS" then do: /* Вывод 2-х таблиц со сборкой: t_typedoc, t_class */
       define dataset tpclass for t_typedoc, t_class.
       hDS = DATASET tpclass:HANDLE.
       run src/transfer/XmlWriteDataset.p (fn, DATASET-HANDLE hDS).
       ResultFile = fn.
    end.

    OTHERWISE do: /* Стандартный вывод в виде файлов екселя: n - файлов  */
       Resultfile = tab2xml ("t_typedoc,t_class").
    end.

END CASE.
/* --------------- End procedure ----------------------- */
