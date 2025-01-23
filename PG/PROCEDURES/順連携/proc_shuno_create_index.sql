--------------------------------------------------------
--  DDL for Procedure  proc_shuno_create_index
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_shuno_create_index()
LANGUAGE 'plpgsql'
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 収納履歴情報連携                                                                                        */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : Ver.1.01.001        2014/04/17   (AIC) T.Yamauchi       新規作成                                        */
/**********************************************************************************************************************/

DECLARE
   lc_sql                         character varying(1000);

BEGIN

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX f_shuno_idx01 ';
      lc_sql := lc_sql || 'ON dlgmain.f_shuno ';
      lc_sql := lc_sql || '(kojin_no) ';
      --lc_sql := lc_sql || 'PCTFREE 10 ';
      --lc_sql := lc_sql || 'INITRANS 2 ';
      --lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      --lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      --lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX f_shuno_idx10 ';
      lc_sql := lc_sql || 'ON dlgmain.f_shuno ';
      lc_sql := lc_sql || '(kibetsu_cd) ';
      --lc_sql := lc_sql || 'PCTFREE 10 ';
      --lc_sql := lc_sql || 'INITRANS 2 ';
      --lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      --lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      --lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX f_shuno_idx101 ';
      lc_sql := lc_sql || 'ON dlgmain.f_shuno ';
	  --TODO: Confirm
      --lc_sql := lc_sql || '(TO_CHAR(fuka_nendo,''FM0999'')||TO_CHAR(kibetsu_cd,''FM0999'')) ';
      lc_sql := lc_sql || '(fuka_nendo, kibetsu_cd) '; -- temp
      --lc_sql := lc_sql || 'PCTFREE 10 ';
      --lc_sql := lc_sql || 'INITRANS 2 ';
      --lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      --lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      --lc_sql := lc_sql || 'LOGGING ';
	  
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX f_shuno_idx11 ';
      lc_sql := lc_sql || 'ON dlgmain.f_shuno ';
      lc_sql := lc_sql || '(kibetsu_key, shuno_ymd) ';
      --lc_sql := lc_sql || 'PCTFREE 10 ';
      --lc_sql := lc_sql || 'INITRANS 2 ';
      --lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      --lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      --lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX f_shuno_idx13 ';
      lc_sql := lc_sql || 'ON dlgmain.f_shuno ';
      lc_sql := lc_sql || '(fuka_nendo, soto_nendo, zeimoku_cd, tsuchisho_no, kankatsu_cd)';
      --lc_sql := lc_sql || 'PCTFREE 10 ';
      --lc_sql := lc_sql || 'INITRANS 2 ';
      --lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      --lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      --lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX f_shuno_idx20 ';
      lc_sql := lc_sql || 'ON dlgmain.f_shuno ';
	  --TODO: Confirm
      --lc_sql := lc_sql || '(doitsunin_kojin_no) ';
	  lc_sql := lc_sql || '(kojin_no) '; -- temp
      --lc_sql := lc_sql || 'PCTFREE 10 ';
      --lc_sql := lc_sql || 'INITRANS 2 ';
      --lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      --lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      --lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

END;
$$;
