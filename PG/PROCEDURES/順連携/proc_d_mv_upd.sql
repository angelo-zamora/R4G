--------------------------------------------------------
--  DDL for Procedure proc_d_mv_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_d_mv_upd ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, out_n_result_code INOUT numeric, out_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 機能概要 : mv_kanrinin 更新                                                                                          */
/* 入力 IN  : in_n_renkei_data_cd  連携データコード                                                                     */
/*            in_n_renkei_seq      連携SEQ（連携の一意性を保つSEQ）                                                   */
/*            in_n_shori_ymd       処理日 （連携の処理を行う日付）                                                    */
/* 出力 OUT : out_n_result_co      結果エラーが発生した場合のエラーコード                                               */
/*            out_c_err_text       結果エラーが発生した場合のエラーメッセージ                                           */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 更新履歴 : 新規作成                                                                                                */
/**********************************************************************************************************************/

DECLARE
   lc_sql           character varying(1000);

   ld_kaishi_datetime   timestamp;
   ld_shuryo_datetime   timestamp;


   ERRM character varying(1000);

BEGIN

   ld_kaishi_datetime := CURRENT_TIMESTAMP(0);

   -- マテリアルを更新
   BEGIN
      -- lc_sql := 'CALL dbms_mview.REFRESH(''mv_taino_kanrinin'')';
      
      lc_sql := 'REFRESH MATERIALIZED VIEW mv_taino_kanrinin';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   -- マテリアルを更新
   BEGIN
      lc_sql := 'REFRESH MATERIALIZED VIEW mv_tsuchisho_no';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   -- インデックス削除（古いもの）
   BEGIN
      lc_sql := 'DROP INDEX dlgmain.mv_taino_kanrinin_idx01 ';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   -- インデックスを作成
   BEGIN
      lc_sql := 'CREATE UNIQUE INDEX dlgmain.mv_taino_kanrinin_idx01 ';
      lc_sql := lc_sql || ' ON dlgmain.mv_taino_kanrinin ';
      lc_sql := lc_sql || ' (kanrinin_kojin_no, del_flg); ';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;
END;
$$;
