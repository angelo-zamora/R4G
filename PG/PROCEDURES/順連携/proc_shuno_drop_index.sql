--------------------------------------------------------
--  DDL for Procedure  proc_shuno_drop_index
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE  proc_shuno_drop_index ( in_n_shori_kbn IN numeric )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 収納履歴情報連携                                                                                        */
/* 引数 IN  : in_n_shori_kbn … 処理区分 0:INDEXを削除しない 1:INDEXを削除する                                        */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : Ver.1.01.001        2014/04/17   (AIC) T.Yamauchi       新規作成                                        */
/**********************************************************************************************************************/

DECLARE
   lc_sql                         character varying(1000);

BEGIN

   BEGIN
      lc_sql := 'DROP INDEX dlgmain.f_shuno_idx01';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX dlgmain.f_shuno_idx10';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX dlgmain.f_shuno_idx101';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX dlgmain.f_shuno_idx11';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX dlgmain.f_shuno_idx13';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   BEGIN
      lc_sql := 'DROP INDEX dlgmain.f_shuno_idx20';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

END;
$$;
