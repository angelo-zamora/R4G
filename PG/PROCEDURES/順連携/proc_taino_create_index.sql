--------------------------------------------------------
--  DDL for Procedure  proc_taino_create_index IF NOT EXISTS
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_taino_create_index ()
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 調定情報連携 　　                                                                                       */
/* 引数 IN  : 処理区分 0:INDEXを再作成しない 1:INDEXを再作成する                                    */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : Ver.1.01.001        2014/04/17   (AIC) T.Yamauchi       新規作成                                        */
/**********************************************************************************************************************/

DECLARE
   lc_sql                         character varying(1000);

BEGIN

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX IF NOT EXISTS f_taino_idx01 ';
      lc_sql := lc_sql || 'ON dlgmain.f_taino ';
      lc_sql := lc_sql || '(kojin_no) ';
      -- lc_sql := lc_sql || 'PCTFREE 10 ';
      -- lc_sql := lc_sql || 'INITRANS 2 ';
      -- lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      -- lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      -- lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX IF NOT EXISTS f_taino_idx02 ';
      lc_sql := lc_sql || 'ON dlgmain.f_taino ';
      lc_sql := lc_sql || '(fuka_nendo, kibetsu_cd) ';
      -- lc_sql := lc_sql || 'PCTFREE 10 ';
      -- lc_sql := lc_sql || 'INITRANS 2 ';
      -- lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      -- lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      -- lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX IF NOT EXISTS f_taino_idx03 ';
      lc_sql := lc_sql || 'ON dlgmain.f_taino ';
      lc_sql := lc_sql || '(kanrinin_kojin_no) ';
      -- lc_sql := lc_sql || 'PCTFREE 10 ';
      -- lc_sql := lc_sql || 'INITRANS 2 ';
      -- lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      -- lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      -- lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX IF NOT EXISTS f_taino_idx04 ';
      lc_sql := lc_sql || 'ON dlgmain.f_taino ';
      lc_sql := lc_sql || '(noki_ymd) ';
      -- lc_sql := lc_sql || 'PCTFREE 10 ';
      -- lc_sql := lc_sql || 'INITRANS 2 ';
      -- lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      -- lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      -- lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX IF NOT EXISTS f_taino_idx05 ';
      lc_sql := lc_sql || 'ON dlgmain.f_taino ';
      lc_sql := lc_sql || '(fuka_nendo, soto_nendo, zeimoku_cd, kibetsu_cd, kojin_no)';
      -- lc_sql := lc_sql || 'PCTFREE 10 ';
      -- lc_sql := lc_sql || 'INITRANS 2 ';
      -- lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      -- lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      -- lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   -- BEGIN
   --    lc_sql := '';
   --    lc_sql := lc_sql || 'CREATE INDEX IF NOT EXISTS f_taino_idx06 ';
   --    lc_sql := lc_sql || 'ON dlgmain.f_taino ';
   --    lc_sql := lc_sql || '(doitsunin_kojin_no) ';
      -- lc_sql := lc_sql || 'PCTFREE 10 ';
      -- lc_sql := lc_sql || 'INITRANS 2 ';
      -- lc_sql := lc_sql || 'MAXTRANS 255 ';
   --    lc_sql := lc_sql || 'TABLESPACE indx; ';
      -- lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      -- lc_sql := lc_sql || 'LOGGING ';

   --    EXECUTE lc_sql;
   -- EXCEPTION
   --    WHEN OTHERS THEN NULL;
   -- END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX IF NOT EXISTS f_taino_idx88 ';
      lc_sql := lc_sql || 'ON dlgmain.f_taino ';
      lc_sql := lc_sql || '(tsuchisho_no) ';
      -- lc_sql := lc_sql || 'PCTFREE 10 ';
      -- lc_sql := lc_sql || 'INITRANS 2 ';
      -- lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      -- lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      -- lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;
   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX IF NOT EXISTS f_taino_idx100 ';
      lc_sql := lc_sql || 'ON dlgmain.f_taino ';
      lc_sql := lc_sql || '(kojin_no, fuka_nendo, kibetsu_cd) ';
      -- lc_sql := lc_sql || 'PCTFREE 10 ';
      -- lc_sql := lc_sql || 'INITRANS 2 ';
      -- lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      -- lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      -- lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := '';
      lc_sql := lc_sql || 'CREATE INDEX IF NOT EXISTS f_taino_idx101 ';
      lc_sql := lc_sql || 'ON dlgmain.f_taino ';
      lc_sql := lc_sql || '(fuka_nendo, kibetsu_cd, kojin_no) ';
      -- lc_sql := lc_sql || 'PCTFREE 10 ';
      -- lc_sql := lc_sql || 'INITRANS 2 ';
      -- lc_sql := lc_sql || 'MAXTRANS 255 ';
      lc_sql := lc_sql || 'TABLESPACE indx; ';
      -- lc_sql := lc_sql || 'STORAGE(INITIAL 1M NEXT 256K MINEXTENTS 1 MAXEXTENTS 2147483645 buffer_pool DEFAULT) ';
      -- lc_sql := lc_sql || 'LOGGING ';

      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

END;
$$;
