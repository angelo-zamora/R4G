--------------------------------------------------------
--  DDL for Procedure  proc_taino_drop_index
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_taino_drop_index ( )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 調定情報連携 　　                                                                                       */
/* 引数 IN  : 処理区分 0:INDEXを削除しない 1:INDEXを削除する                                        */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : Ver.1.01.001        2014/04/17   (AIC) T.Yamauchi       新規作成                                        */
/**********************************************************************************************************************/

DECLARE
   lc_sql                         character varying(1000);

BEGIN

   BEGIN
      lc_sql := 'DROP INDEX IF EXISTS f_taino_idx01';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX IF EXISTS f_taino_idx02';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX IF EXISTS f_taino_idx03';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX IF EXISTS f_taino_idx04';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX IF EXISTS f_taino_idx05';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   -- BEGIN
      -- lc_sql := 'DROP INDEX IF EXISTS f_taino_idx06';
      -- EXECUTE lc_sql;
   -- EXCEPTION
      -- WHEN OTHERS THEN NULL;
   -- END;

   BEGIN
      lc_sql := 'DROP INDEX IF EXISTS f_taino_idx88';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX IF EXISTS f_taino_idx100';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX IF EXISTS f_taino_idx101';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;
END;
$$;
