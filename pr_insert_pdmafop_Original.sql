PROCEDURE pr_insert_pdmafop (p_in_TEST_ID      IN     VARCHAR2,
                             p_in_process_id    IN     NUMBER,
                             p_in_chunk_id      IN     NUMBER,
p_out_dml_count      out number,
                             p_out_err_cd          OUT NUMBER)
IS
   l_insert_str   VARCHAR2 (32767);
   v_err_retVal   NUMBER;
   v_context      VARCHAR2 (1000) := NULL;
BEGIN
   --    Fields accessed - oidpdmu,universeflg,mafsrc,action,opdate
   v_context :=
         'Insert into MAFOP, for process_id and chunk_id:'
      || p_in_process_id
      || p_in_chunk_id;

   l_insert_str :=
         'INSERT INTO '
      || pv_loader_config_tbl_rec.prdt_schema
      || '.MAFOP
        (
        TEST_ID,
        PROCESS_ID,
        CHUNK_ID,
        OIDPDMU,
        UNIVERSEFLG,
        MAFSRC,
        ACTION,
        OPDATE,
        OPERATION
        )
        (
        SELECT
        :test_id,
        :process_id,
        :chunk_id,
        a_oidmu oidpdmu,      -- OIDPDMU
        NULL universeflg,     -- UNIVERSEFLG
        a_mafsrc,             -- MAFSRC
        (
       CASE
        WHEN a_mafsrc IN (''061'', ''063'', ''065'') THEN    -- DAAL ACTION CODES
        (
         CASE
          WHEN  SUM(CASE WHEN a_action = ''A''                      THEN ''1'' ELSE ''0'' END) > 0 -- postcendaaladd OR precendaaladd
            AND SUM(CASE WHEN a_action = ''D''                      THEN ''1'' ELSE ''0'' END) > 0 -- postcendaaldel OR precendaaldel
            AND SUM(CASE WHEN f_action = ''A'' AND f_vintage = ''70'' THEN ''1'' ELSE ''0'' END) > 0 -- tabaaction
            AND SUM(CASE WHEN f_action = ''D'' AND f_vintage = ''70'' THEN ''1'' ELSE ''0'' END) > 0 -- tabdaction
           THEN ''M'' -- BLOCK MOVE
          ELSE SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1)
         END
        ) -- END DAAL
        WHEN a_mafsrc IN (''026'') THEN    -- BLOCK CANVASSING HIERARCHY
        (
         CASE
          WHEN SUM(CASE WHEN f_action = ''A'' AND f_opcode = ''103''  THEN ''1'' ELSE ''0'' END) > 0          -- collaaction
           AND SUM(CASE WHEN f_action = ''D'' AND f_opcode = ''103''  THEN ''1'' ELSE ''0'' END) > 0 THEN ''M'' -- colldaction
          WHEN SUM(CASE WHEN a_action = ''A''                       THEN ''1'' ELSE ''0'' END) > 0          -- srcaaction
           AND SUM(CASE WHEN a_action = ''V''                       THEN ''1'' ELSE ''0'' END) > 0 THEN ''E'' -- srcvaction
          WHEN SUM(CASE WHEN a_action = ''A''                       THEN ''1'' ELSE ''0'' END) > 0 THEN ''A''
          WHEN SUM(CASE WHEN a_action = ''C''                       THEN ''1'' ELSE ''0'' END) > 0 THEN ''C'' -- srccaction
          WHEN SUM(CASE WHEN a_action = ''V''                       THEN ''1'' ELSE ''0'' END) > 0 THEN ''V''
          WHEN SUM(CASE WHEN a_action = ''N''                       THEN ''1'' ELSE ''0'' END) > 0 THEN ''N'' -- srcnaction
          WHEN SUM(CASE WHEN a_action = ''D'' AND a_unitstat = ''07'' THEN ''1'' ELSE ''0'' END) > 0 THEN ''2'' -- srcdustat7
          WHEN SUM(CASE WHEN a_action = ''D'' AND a_unitstat = ''31'' THEN ''1'' ELSE ''0'' END) > 0 THEN ''U'' -- srcdustat31
          WHEN SUM(CASE WHEN a_action = ''D'' AND a_unitstat = ''04'' THEN ''1'' ELSE ''0'' END) > 0 THEN ''D'' -- srcdustat4
          ELSE SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1)
         END
        ) -- END BLOCK CANVASS
        WHEN a_mafsrc IN (''019'', ''020'', ''022'', ''023'', ''024'', ''025'', ''027'', ''029'', ''034'') THEN  -- LUCA APPEALS,UE,UUE,CIFU,NRFU,MISALLOC
        (
         CASE
          WHEN a_mafsrc = ''027''
           AND (SUM(CASE WHEN a_action = ''V'' THEN ''1'' ELSE ''0'' END) > 0              -- srcvaction
            OR  SUM(CASE WHEN a_action = ''C'' THEN ''1'' ELSE ''0'' END) > 0) THEN ''A''  -- srccaction
          WHEN  SUM(CASE WHEN a_action = ''A'' THEN ''1'' ELSE ''0'' END) > 0  THEN ''A''  -- srccaction
          WHEN  SUM(CASE WHEN a_action = ''C'' THEN ''1'' ELSE ''0'' END) > 0  THEN ''C''  -- srccaction
          WHEN  SUM(CASE WHEN a_action = ''V'' THEN ''1'' ELSE ''0'' END) > 0  THEN ''V''  -- srcvaction
          WHEN  SUM(CASE WHEN a_action = ''N'' THEN ''1'' ELSE ''0'' END) > 0  THEN ''N''  -- srcnaction
          WHEN  SUM(CASE WHEN a_action = ''D'' THEN ''1'' ELSE ''0'' END) > 0  THEN ''D''  -- srcdaction
          ELSE  SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1)
         END
        ) -- END LUCA 98 APPEALS
        WHEN a_mafsrc IN (''006'') THEN  -- ADDRESS LISTING ACTION CODE
        (
         CASE
          WHEN SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1) IN (''O'', ''U'', ''R'', ''B'') THEN ''A''
          ELSE SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1)
         END
        ) -- END ADDRESS LISTING ACTION CODE
        WHEN a_mafsrc = ''012'' THEN DECODE(SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1),''A'',''C''
                                             ,SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1))    -- ACSTOI ACTION CODE
        WHEN a_mafsrc = ''035'' THEN DECODE(SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD;'')||a_collator||a_action),-1,1),''U'',''R''
                                             ,SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1))    -- CQR ACTION CODE
        WHEN a_mafsrc = ''014'' THEN            -- SPECIAL CENSUS
        (
         CASE
          WHEN SUM(CASE WHEN a_action  = ''A'' THEN ''1'' ELSE ''0'' END) > 0  -- srcaaction
           AND SUM(CASE WHEN a_action  = ''D'' THEN ''1'' ELSE ''0'' END) > 0  -- srcdaction
           THEN   -- POTENTIAL BLOCK MOVE
           (
            CASE
             WHEN SUM(CASE WHEN f_vintage = ''70'' AND f_ispref = ''Y'' THEN ''1'' ELSE ''0'' END) = 0 -- offtab NO OFFICIAL TABBLOCK
              THEN ''A''  -- NOT POTENTIAL BLOCK MOVE
             ELSE -- HAS OFFICIAL TAB BLOCK
             (
              CASE
               WHEN SUM(CASE WHEN f_action = ''A'' AND f_vintage = ''70'' THEN ''1'' ELSE ''0'' END) > 0 -- tabaaction
                AND SUM(CASE WHEN f_action = ''D'' AND f_vintage = ''70'' THEN ''1'' ELSE ''0'' END) > 0 -- tabdaction
                THEN ''M''
               ELSE ''A''
              END
             )
            END
           )
          WHEN SUM(CASE WHEN a_action = ''A'' THEN ''1'' ELSE ''0'' END) > 0 THEN ''A''                         -- srccaction
          WHEN SUM(CASE WHEN a_action = ''C'' THEN ''1'' ELSE ''0'' END) > 0                                  -- srccaction
           AND SUM(CASE WHEN a_action = ''D'' AND a_unitstat = ''07'' THEN ''1'' ELSE ''0'' END) = 0            -- srcdustat7
           AND SUM(CASE WHEN a_action = ''D'' AND a_unitstat = ''31'' THEN ''1'' ELSE ''0'' END) = 0            -- srcdustat31
           THEN ''C''
          WHEN SUM(CASE WHEN a_action = ''D'' AND a_unitstat = ''07'' THEN ''1'' ELSE ''0'' END) > 0 THEN ''2''   -- srcdustat7
          WHEN SUM(CASE WHEN a_action = ''V'' THEN ''1'' ELSE ''0'' END) > 0 THEN ''V''                         -- srcvaction
          WHEN SUM(CASE WHEN a_action = ''N'' THEN ''1'' ELSE ''0'' END) > 0 THEN ''N''                         -- srcnaction
          WHEN SUM(CASE WHEN a_action = ''D'' AND a_unitstat = ''31''   THEN ''1'' ELSE ''0'' END) > 0 THEN ''U'' -- srcdustat31
          WHEN SUM(CASE WHEN a_action = ''D'' THEN ''1'' ELSE ''0'' END) > 0 THEN ''D''                         -- srcdaction
          ELSE SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1)
         END
        ) -- END SPECIAL CENSUS ACTION CODES
        ELSE DECODE(SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1),''J'',''A''
                                        ,SUBSTR(MAX(TO_CHAR(a_opdate, ''YYYYMMDD'')||a_collator||a_action),-1,1))    -- ALL OTHER ACTION CODES
       END
        ) action,                -- END ACTION CODES
        MAX(a_opdate)  opdate,   '''
      || pv_loader_config_tbl_rec.operation
      || '''  
        FROM
         (
          SELECT
           a.TEST_ID,
           a.PROCESS_ID,
           a.CHUNK_ID,
           f.opcode     f_opcode,
           f.ispref     f_ispref,
           f.action     f_action,
           f.vintage    f_vintage,
           a.oidmu      a_oidmu,
           NULL         a_universeflg,
           a.mafsrc     a_mafsrc,
           a.unitstat   a_unitstat,
           a.opdate     a_opdate,
           a.action     a_action,
           -- Determine if the given action is negative, positive or neither (unaccounted for),
           -- then assign a dummy prefix to influence the sort order of the actions so
           -- that positive actions move to the top of the stack, thus allowing the
           -- MAX() functions used above to always choose a positive action over a
           -- negative or unaccounted for action, when both/multiple exist
           CASE
              WHEN ACT.ACTION_RESULT = ''D'' THEN -- Dependency on other columns to determine if Positive or Negative
                 CASE
                    WHEN A.ACTION = ''H'' THEN
                       CASE
                          WHEN G.GQHUFLAG = ''0'' AND R.RESSTAT = ''1'' THEN -- Housing Unit
                             ''999''||TO_CHAR(a.oidadsrc,''FM00000000000000000000'') -- Positive
                          ELSE
                             ''500''||TO_CHAR(a.oidadsrc,''FM00000000000000000000'') -- Negative
                       END
                    WHEN A.ACTION = ''G'' THEN
                       CASE
                          WHEN G.GQHUFLAG = ''2'' THEN                     -- Group Quarters
                             ''999''||TO_CHAR(a.oidadsrc,''FM00000000000000000000'') -- Positive
                          ELSE
                             ''500''||TO_CHAR(a.oidadsrc,''FM00000000000000000000'') -- Negative
                       END
                 END
              ELSE
                 CASE
                    WHEN ACT.ACTION_RESULT = ''P'' THEN -- Positive
                       ''999''||TO_CHAR(a.oidadsrc,''FM00000000000000000000'') -- Positive Action
                    WHEN ACT.ACTION_RESULT = ''N'' THEN -- Negative
                       ''500''||TO_CHAR(a.oidadsrc,''FM00000000000000000000'') -- Negative Action
                    ELSE -- Any action that is neither positive nor negative, is assigned the smallest collator prefix
                       ''001''||TO_CHAR(a.oidadsrc,''FM00000000000000000000'') -- Unaccounted for Action
                 END
           END          a_collator
          FROM
           addrsrcrel_gt a,  
           FMRSRCREL_gt  f,
           (SELECT
             OIDMU, GQHUFLAG
            FROM
           SPGQ_GT)      G,
           (SELECT
             OIDMU, RESSTAT
            FROM
             RESSTAT_DELTYPE_GT)  R,
           ref_action_lu   ACT
          WHERE a.mafsrc <> ''057''
           AND a.oidmu  = f.oidmu(+)
           AND a.mafsrc = f.mafsrc(+)
           AND a.oidmu  = G.oidmu(+)
           AND a.oidmu  = R.oidmu(+)
           AND a.ACTION = ACT.action(+)
           AND a.mafsrc NOT IN (SELECT mafsrc FROM REF_EXCLUDED_MAFSRC_LU)
         )  
        GROUP BY a_oidmu, a_mafsrc
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.ac04_dunv,
        flags.ac04_mafsrc,
        flags.ac04_act,
        flags.ac04_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.ac04_act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.ac06_dunv,
        flags.ac06_mafsrc,
        flags.ac06_act,
        flags.ac06_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.ac06_act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.adcan08_dunv,
        flags.adcan08_mafsrc,
        flags.adcan08_act,
        flags.adcan08_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''  
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.adcan08_act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.adcan10_117dunv,
        flags.adcan10_117mafsrc,
        flags.adcan10_117act,
        flags.adcan10_117date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.adcan10_117act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.adcan10_167dunv,
        flags.adcan10_167mafsrc,
        flags.adcan10_167act,
        flags.adcan10_167date, '''
      || pv_loader_config_tbl_rec.operation
      || '''  
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.adcan10_167act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.luca08_dunv,
        flags.luca08_mafsrc,
        flags.luca08_act,
        flags.luca08_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.luca08_act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.luca10_dunv,
        flags.luca10_mafsrc,
        flags.luca10_act,
        flags.luca10_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.luca10_act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.ulmail04_dunv,
        flags.ulmail04_mafsrc,
        flags.ulmail04_act,
        flags.ulmail04_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.ulmail04_act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.ue06_dunv,
        flags.ue06_mafsrc,
        flags.ue06_act,
        flags.ue06_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.ue06_act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.nrfu06_dunv,
        flags.nrfu06_mafsrc,
        flags.nrfu06_act,
        flags.nrfu06_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.nrfu06_act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.gqv06_dunv,
        flags.gqv06_mafsrc,
        flags.gqv06_act,
        flags.gqv06_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.gqv06_act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.gqv08_dunv,
        flags.gqv08_mafsrc,
        flags.gqv08_act,
        flags.gqv08_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.gqv08_act IS NOT NULL
        UNION ALL
        SELECT
        mu.TEST_ID,
        mu.PROCESS_ID,
        mu.CHUNK_ID,
        flags.oidmu,
        flags.gqv10_unv,
        flags.gqv10_mafsrc,
        flags.gqv10_act,
        flags.gqv10_date, '''
      || pv_loader_config_tbl_rec.operation
      || '''    
        FROM MAFUNIT_GT mu JOIN '
      || pv_loader_config_tbl_rec.source_schema4
      || '.PDB_MU_OPER_HISTORY_LU flags ON (flags.oidmu = mu.oid)
        WHERE flags.gqv10_act IS NOT NULL
        )';
       
    -- INSERT INTO REF_DYNAMIC_SQL(REF_SQL,PROC_NAME) VALUES(l_insert_str,'pr_insert_pdmafop');    
       
    EXECUTE IMMEDIATE l_insert_str
      USING p_in_TEST_ID, p_in_process_id, p_in_chunk_id;

   p_out_dml_count := sql%rowcount;
   
   p_out_err_cd := SUCCESS_CD;
EXCEPTION
   WHEN OTHERS
   THEN
      p_out_err_cd := SQLCODE;
      v_err_retVal :=
         fn_log_Error (p_in_err_msg      => SQLERRM,
                       p_in_context      => v_context,
                       p_in_op_code      => p_out_err_cd,
                       p_in_process_id   => p_in_process_id,
                       p_in_chunk_id     => p_in_chunk_id,
                       p_in_oidmu        => NULL);
END pr_insert_pdmafop;
