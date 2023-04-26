create or replace PROCEDURE  pr_insert_pdb_pdmafop(out_err_cd      OUT NUMBER) IS
                                     
        
       v_context           VARCHAR2(1000)  ; 
       v_scid              VARCHAR2(5)  ;     
      

       BEGIN
           
          v_context := 'Inserting the data into the gt table PDB_PDMAFOP_VW_gt :';

         IF NVL(pv_apv_config_tbl_rec.DECNL_FLAG,'XX')='S' THEN
            v_scid := SUBSTR(pv_apv_config_tbl_rec.test_id,-5) ;
          ELSIF NVL(pv_apv_config_tbl_rec.DECNL_FLAG,'XX')<>'S' THEN
              v_scid := NULL;
         END IF;
               
         INSERT INTO PDMAF_VIEW.PDB_PDMAFOP_VW_gt
        (
        OIDPDMU,
       UNIVERSEFLG,
        MAFSRC,
        ACTION,
        OPDATE,
        DT_CREATED,        
        SCID
        )
        (
        SELECT 
       a_oidmu oidpdmu,      
        NULL universeflg,     
        a_mafsrc,             
        (
        SUBSTR(MAX(TO_CHAR(a_opdate, 'YYYYMMDD')||a_collator||a_action),-1,1)   -- remove the decode not necessary   
        ) action,
        MAX(a_opdate)  opdate,
        SYSDATE,
        v_scid
FROM
(
select 
           
          a.oidmu      a_oidmu,
           NULL         a_universeflg,
           a.mafsrc     a_mafsrc,
           a.unitstat   a_unitstat,
           a.opdate     a_opdate,
           a.action     a_action,
           CASE 
           WHEN ACT.ACTION_RESULT = 'D' THEN -- Dependency on other columns to determine if Positive or Negative
           NULL
           ELSE
          CASE
          WHEN ACT.ACTION_RESULT in ('P','M','N')  THEN -- Positive Action
          act.action_id||TO_CHAR(a.oidadsrc,'FM00000000000000000000')
          ELSE -- Any action that is neither positive nor negative, is assigned the smallest collator prefix
         '001'||TO_CHAR(a.oidadsrc,'FM00000000000000000000')
          END
           END a_collator
           
      from PDMAF_VIEW.PDB_ADS_OTHERSRC_VW_gt a,        
          PDMAF_VIEW.pdb_action_lu          ACT
          WHERE a.mafsrc <> '057'
         AND a.mafsrc NOT IN (SELECT mafsrc FROM PDMAF_VIEW.pdb_excluded_mafsrc_lu) 
          AND a.ACTION = ACT.action(+)
          )
           GROUP BY a_oidmu, a_mafsrc
        );
        
  dbms_output.put_line('Number Of rows inserted for pr_insert_pdb_pdmafop :'||sql%rowcount);

            v_context := 'Gather statistics on PDB_PDMAFOP_VW_GT' ;

          dbms_stats.gather_table_stats('PDMAF_VIEW','PDB_PDMAFOP_VW_GT');
        
                   
           out_err_cd := 0;
                   
        EXCEPTION
           WHEN OTHERS THEN
          dbms_output.put_line('Failed while :'||v_context||'sqlerror : '||sqlcode||' sqlerrormsg : '||sqlerrm);
          
          out_err_cd := sqlcode;


        INSERT INTO PDMAF_VIEW.PDB_ERROR (TEST_ID,STATEFP,COUNTYFP,TRACT,LAST_RUN_DATE,RT_ERROR_MESSAGE) 
				 VALUES (in_test_id , in_state,in_county,in_tract,SYSDATE,'Failed while :'||v_context);
          
        END pr_insert_pdb_pdmafop;
		
    