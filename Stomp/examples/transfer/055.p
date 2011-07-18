/* Запуск модулей на удаленных БД/Рассылка файлов на удаленные БД/Получение файлов результатов  */

{trigmain.i}

PROCEDURE OnNewDocument :
  define input parameter rid-doc as integer.

  run src/kernel/set_ffv.p ( "1:1", rid-doc, STRING(TODAY) ).
END.

PROCEDURE OnCloseDocument :
  define input parameter rid-doc as integer.

  define variable doc-date      as date.
  define variable doc-num       as integer.
  define variable doc-descr     as character.
  define variable rows          as integer.
  define variable i             as integer.
  define variable j             as integer.
  define variable EntStatus     as character.
  define variable DateFrom      as date.
  define variable DateTo        as date.
  
  run src/kernel/get_ffv.p ( "1:1", rid-doc ).
  doc-date = DATE ( RETURN-VALUE ).
  run src/kernel/get_ffv.p ( "1:2", rid-doc ).
  doc-num = INTEGER ( RETURN-VALUE ).

  run src/kernel/get_ffv.p ( "1:3", rid-doc ).
  doc-descr = RETURN-VALUE.
  run src/kernel/get_ffv.p ( "1:4", rid-doc ).
  doc-descr = doc-descr + "|" + RETURN-VALUE.
  run src/kernel/get_ffv.p ( "1:5", rid-doc ).
  doc-descr = doc-descr + "(" + RETURN-VALUE + ")".
  
  run src/kernel/get_tr.p ( 2, rid-doc, OUTPUT rows ).
  do j = 1 to rows :
    run src/kernel/get_ftv.p ( "2:2", rid-doc, j ).
    EntStatus = RETURN-VALUE.

    if EntStatus Begins "2." then
    do:
      i = i + 1.
    end.
  end.
  if i > 0 then
    doc-descr = STRING(i) + "/" + STRING(rows) + " " + doc-descr.

  run src/kernel/gen_doc.p ( rid-doc,
    doc-num,1,doc-date,doc-descr,0,0 ).

  define variable rid-task as integer.
  run src/om/doc2tsk.p ( rid-doc, OUTPUT rid-task ).                                                          
  run src/om/chtask.p ( rid-task, ?, doc-descr ).  
  run src/om/s_tskuid.p ( rid-task, "ubatch" ).
END.

PROCEDURE OnDeleteDocument :
  define input parameter rid-doc as integer.
  
  RETURN "DELETE".
END.

PROCEDURE ONModifyField :
  define input parameter rid-doc as integer.
  define input parameter fld as character.
  define input parameter row as integer.
  
  if fld = "1:1" then /* Обновить номер при изменении даты  */
  do:
    define variable num as integer.
    run src/kernel/get_ffv.p ( "1:1", rid-doc ).
    run src/kernel/new_dnum.p ( DATE (RETURN-VALUE), rid-doc, OUTPUT num ).
    run src/kernel/set_ffv.p ( "1:2", rid-doc, STRING(num) ). 
  end.
END.

PROCEDURE OnChoose :
  define input parameter rid-doc as integer.                                    
  define input parameter fld as character.               

  define variable row as integer.
  if fld = "1:6" then
  do:
    run src/kernel/deltable.p (2, rid-doc).

    row = 0.
    for each ent where ent.id-ent begins "oblik" NO-LOCK:
      row = row + 1.
      run src/kernel/set_ftv.p ( "2:1", rid-doc, row, STRING(ent.rid-ent) ). 
      run src/kernel/set_ftv.p ( "2:2", rid-doc, row, STRING("0. Создано") ).
    end.
  end.
  if fld = "1:8" then
  do:
    run Report ( rid-doc ).
  end.
  if fld = "1:9" then
  do:
    run PackFiles ( rid-doc ).
  end.
  if fld = "1:10" then
  do:
    run CreatePivotTable ( rid-doc ).
  end.
END.

PROCEDURE OnCopyDocument :
  define input parameter rid-doc  as integer.
  define input parameter rid-main as integer.

  run src/kernel/cpdoc.p ( rid-main, rid-doc ).

  run src/kernel/set_ffv.p ( "1:1", rid-doc, "" ).
  run src/kernel/set_ffv.p ( "1:1", rid-doc, STRING(TODAY) ).
END.



PROCEDURE OnChangeTaskStatus :                   
  define input parameter rid-doc as integer.      
  define input parameter new-status as character. 
  define input parameter rid-main as integer.     

  define variable rident as integer.
  define variable j as integer.
  define variable rows as integer.
  define variable EntStatus as character.
  define variable packetname as character.
  define variable modulename as character.
  define variable moduleparams as character.
  define variable rid-task as integer.

  if new-status = "0" /* Не начат */ then
  do:          
    /* Поставить документ на хранение */
    run src/kernel/sendevnt.p ( rid-doc, 22 ).
  end.
  if new-status = "1" /* В работе */ or new-status = "3" /* Отложена */ or new-status = "4" /* В Ожидании */ then
  do:          
    message "Идет отправка заданий на БД....".
    /* Поставить документ на хранение */
    run src/kernel/sendevnt.p ( rid-doc, 23 ).

    run src/kernel/get_tr.p ( 2, rid-doc, OUTPUT rows ).
    do j = 1 to rows :
      run src/kernel/get_ftv.p ( "2:1", rid-doc, j ).
      RidEnt = INTEGER(RETURN-VALUE).
      run src/kernel/get_ftv.p ( "2:2", rid-doc, j ).
      EntStatus = RETURN-VALUE.

      if EntStatus Begins "0." then
      do:
        find first ent where ent.rid-ent = rident NO-LOCK NO-ERROR.
        if available ent then
        do:
          message ent.name-ent.
          run src/kernel/get_ffv.p ( "1:3", rid-doc ).
          packetname = RETURN-VALUE.
          run src/kernel/get_ffv.p ( "1:4", rid-doc ).
          modulename = RETURN-VALUE.
          run src/kernel/get_ffv.p ( "1:5", rid-doc ).
          moduleparams = RETURN-VALUE.
/*
          if packetname = "" and modulename <> "" then
          do:
            packetname = modulename.
            modulename = "".
          end.
*/
          run src/om/doc2tsk.p (rid-doc, output rid-task).
          run src/transfer/MQRunModule.p (ent.id-ent, packetname, modulename, moduleparams, "/topic/cwh.data", string(rid-task) + "_" + ent.id-ent).
          if RETURN-VALUE = "OK" then
            run src/kernel/set_ftv.p ( "2:2", rid-doc, j, "1. Передано в MQ" ).
        end.
      end.
    end.
    hide message.
  end.
  
  if new-status = "2" /* Завершен */ then 
  do:          
  end.
END.

Define temp-table Result NO-UNDO
  field entname as character label "Предприятие"
  field entstatus as character label "Статус"
  field resultstring as character label "Результат"
  field filename as character label "Имя файла"
  index i0 entname.

{libs.i}
{libs_ds.i}

PROCEDURE Report:
  define input parameter rid-doc as integer.

  define variable j as integer.
  define variable rows as integer.
  run src/kernel/get_tr.p ( 2, rid-doc, OUTPUT rows ).
  do j = 1 to rows :
    run src/kernel/get_ftv.p ( "2:1", rid-doc, j ).
    if RETURN-VALUE = "" then NEXT.

    create result.
    run src/kernel/get_fts.p ( "2:1", rid-doc, j ).
    result.entname = RETURN-VALUE.
    run src/kernel/get_ftv.p ( "2:2", rid-doc, j ).
    result.EntStatus = RETURN-VALUE.
    run src/kernel/get_ftv.p ( "2:3", rid-doc, j ).
    result.resultstring = RETURN-VALUE.
    run src/kernel/get_ftv.p ( "2:4", rid-doc, j ).
    result.filename = RETURN-VALUE.
  end.

  tab2xml ("Result").
END.

PROCEDURE PackFiles:
  define input parameter rid-doc as integer.

  define variable j as integer.
  define variable rows as integer.
  define variable zipfile as character.
  define variable filename as character.
  define variable rid-task as integer.
  define variable filelist as character.

  run src/om/doc2tsk.p (rid-doc, output rid-task).
  zipfile = "temp/" + string(rid-task) + ".zip".
  if search (zipfile) <> ? then
    os-delete value (zipfile).

  run src/kernel/get_tr.p ( 2, rid-doc, OUTPUT rows ).
  do j = 1 to rows :
    run src/kernel/get_ftv.p ( "2:4", rid-doc, j ).
    filename = return-value.
    if RETURN-VALUE = "" then NEXT.
    if search (filename) = ? then NEXT.
    if filelist = "" then filelist = filename.
    else filelist = filelist + " " + filename.
  end.
  os-command silent value("zip " + zipfile + " " + filelist + " > null" ).
  silent-print = TRUE.
  silent-printcount = 1.
  RUN src/prn_dvs.w ( FALSE, "EXCEL#" + zipfile + ";", "OUT-ONLY=Да" ).
  silent-print = FALSE.
  silent-printcount = 0.
END.

/* Объеденим таблицы, которые пришли в xml файл в одну таблицу с добавлением столбцов id-ent name-ent. */

PROCEDURE CreatePivotTable:
  define input parameter rid-doc as integer.

  define variable j as integer.
  define variable i as integer.
  define variable rows as integer.
  define variable filename as character.

  define variable hTable as handle.
  define variable hDS as handle.
  define variable isDS as logical.
  define variable ResultData as handle.
  define variable ResultDS as handle.
  define variable hBuffer as handle.
  define variable hBufferData as handle.
  define variable qBuf as handle.
  define variable hQuery as handle.
  define variable tn as character.
  define variable rident as integer.
  define variable fieldcount as integer.
  define variable fieldvalue as character.
  define variable outfile as character.

  message "Идет обработка и объединение файлов...".
  run src/kernel/get_tr.p ( 2, rid-doc, OUTPUT rows ).
  do j = 1 to rows :
    run src/kernel/get_ftv.p ( "2:4", rid-doc, j ).
    filename = return-value.
    if RETURN-VALUE = "" then NEXT.
    if search (filename) = ? then NEXT.
    run src/transfer/XmlReadDataset.p (FileName, OUTPUT DATASET-HANDLE hDS).
    isDS = LOGICAL(RETURN-VALUE). 
    if not isDS then
    do:
      run src/transfer/XmlReadTable.p (FileName, OUTPUT TABLE-HANDLE hTable).
      if RETURN-VALUE <> "yes" then
      do:
        message "Файлы для сборки должны быть либо xml table или xml dataset" view-as alert-box.
        RETURN.
      end.
    end.
    leave.
  end.
  if not isDS then /* Входные файлы - это одна временная таблица, объединяем ее по всем предприятиям */
  do:
    CREATE TEMP-TABLE ResultData.
    hBuffer = hTable:DEFAULT-BUFFER-HANDLE.
    ResultData:CREATE-LIKE(hBuffer).
    ResultData:ADD-NEW-FIELD ("id-ent",   "character", ?, "X(15)", "", "Код предприятия" ). 
    ResultData:ADD-NEW-FIELD ("name-ent", "character", ?, "X(40)", "", "Предприятие" ). 
    ResultData:TEMP-TABLE-PREPARE(hBuffer:NAME).
    CREATE BUFFER hBufferData FOR TABLE ResultData:DEFAULT-BUFFER-HANDLE.
    CREATE QUERY hQuery.
    hQuery:SET-BUFFERS(hBufferData).
    delete object hTable.
  
    run src/kernel/get_tr.p ( 2, rid-doc, OUTPUT rows ).
    do j = 1 to rows :
      run src/kernel/get_ftv.p ( "2:4", rid-doc, j ).
      filename = return-value.
      if RETURN-VALUE = "" then NEXT.
      if search (filename) = ? then NEXT.
  
      run src/kernel/get_ftv.p ( "2:1", rid-doc, j ).
      rident = INTEGER(RETURN-VALUE).
      find first ent where ent.rid-ent = rident NO-LOCK NO-ERROR.
      if not available ent then NEXT.
  
      run src/transfer/XmlReadTable.p (FileName, OUTPUT TABLE-HANDLE hTable).
      hBuffer = hTable:DEFAULT-BUFFER-HANDLE.
      tn = hBuffer:NAME.
  
      CREATE QUERY qBuf.
      qBuf:SET-BUFFERS(hBuffer).
      qBuf:QUERY-PREPARE("FOR EACH " + tn + " NO-LOCK").
      qBuf:QUERY-OPEN().
      qBuf:GET-FIRST().
      repeat:
        IF NOT hBuffer:AVAILABLE then LEAVE.
        hBufferData:BUFFER-CREATE().
        hBufferData:BUFFER-COPY(hBuffer).
  
        hBufferData:BUFFER-FIELD("id-ent"):BUFFER-VALUE = ent.id-ent.
        hBufferData:BUFFER-FIELD("name-ent"):BUFFER-VALUE = ent.name-ent.
  
        qBuf:GET-NEXT().
      end.
  
      delete object qBuf.
      delete object hTable.
    end.
    message "Формируется выходной файл...".
  
    create dataset ResultDS.
    ResultDS:ADD-Buffer (hBufferData).
    tab2xmlDS (Dataset-handle ResultDS).
   
    if valid-handle (hQuery) then
      delete object hQuery.
    if valid-handle (ResultDS) then
      delete object ResultDS.
    if valid-handle (hBufferData) then
      delete object hBufferData.
    if valid-handle (ResultData) then
      delete object ResultData. 
  END.
  else do:  /* Входные файлы - это уже наборы Dataset, и нужно объеденить сразу несколько таблиц в выходной файл. */
    /* Читаем структуру с первого файла */
    create dataset ResultDS.
    DO i = 1 TO hDS:NUM-BUFFERS:
      hBuffer = hDS:GET-BUFFER-HANDLE(i).
      hTable = hBuffer:TABLE-HANDLE.

      CREATE TEMP-TABLE ResultData.
      ResultData:CREATE-LIKE(hBuffer).
      ResultData:ADD-NEW-FIELD ("id-ent",   "character", ?, "X(15)", "", "Код предприятия" ). 
      ResultData:ADD-NEW-FIELD ("name-ent", "character", ?, "X(40)", "", "Предприятие" ). 
      ResultData:TEMP-TABLE-PREPARE(hBuffer:NAME).
      CREATE BUFFER hBufferData FOR TABLE ResultData:DEFAULT-BUFFER-HANDLE.
      ResultDS:ADD-Buffer (hBufferData).
    end.
    run src/kernel/get_tr.p ( 2, rid-doc, OUTPUT rows ).
    do j = 1 to rows :
      run src/kernel/get_ftv.p ( "2:4", rid-doc, j ).
      filename = return-value.
      if RETURN-VALUE = "" then NEXT.
      if search (filename) = ? then NEXT.
  
      run src/kernel/get_ftv.p ( "2:1", rid-doc, j ).
      rident = INTEGER(RETURN-VALUE).
      find first ent where ent.rid-ent = rident NO-LOCK NO-ERROR.
      if not available ent then NEXT.
  
      run src/transfer/XmlReadDataset.p (FileName, OUTPUT DATASET-HANDLE hDS).
      DO i = 1 TO hDS:NUM-BUFFERS:
        hBuffer = hDS:GET-BUFFER-HANDLE(i).
        hTable = hBuffer:TABLE-HANDLE.
        hBufferData = ResultDS:GET-BUFFER-HANDLE(i).

        CREATE QUERY qBuf.
        qBuf:SET-BUFFERS(hBuffer).
        qBuf:QUERY-PREPARE("FOR EACH " + hBuffer:NAME + " NO-LOCK").
        qBuf:QUERY-OPEN().
        qBuf:GET-FIRST().
        repeat:
          IF NOT hBuffer:AVAILABLE then LEAVE.

          hBufferData:BUFFER-CREATE().
          hBufferData:BUFFER-COPY(hBuffer).
  
          hBufferData:BUFFER-FIELD("id-ent"):BUFFER-VALUE = ent.id-ent.
          hBufferData:BUFFER-FIELD("name-ent"):BUFFER-VALUE = ent.name-ent.
  
          qBuf:GET-NEXT().
        end.
        delete object qBuf.
      end.
    end.

    message "Формируется выходной файл...".
    tab2xmlDS (Dataset-handle ResultDS).

    if valid-handle (hQuery) then
      delete object hQuery.
    if valid-handle (ResultDS) then
      delete object ResultDS.
    if valid-handle (hBufferData) then
      delete object hBufferData.
    if valid-handle (ResultData) then
      delete object ResultData. 
  end.
END.  
  