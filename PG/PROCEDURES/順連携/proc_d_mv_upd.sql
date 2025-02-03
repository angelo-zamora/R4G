--------------------------------------------------------
--  DDL for Procedure proc_d_mv_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_d_mv_upd ( 
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   out_n_result_code INOUT numeric, 
   out_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 機能概要 : マテリアライズドビュー更新                                                                                 */
/* 入力 IN  : in_n_renkei_data_cd  連携データコード                                                                     */
/*            in_n_renkei_seq      連携SEQ（連携の一意性を保つSEQ）                                                     */
/*            in_n_shori_ymd       処理日 （連携の処理を行う日付）                                                      */
/* 出力 OUT : out_n_result_co      結果エラーが発生した場合のエラーコード                                                 */
/*            out_c_err_text       結果エラーが発生した場合のエラーメッセージ                                             */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/02/03  CRESS-INFO.Drexler     新規作成     マテリアライズドビュー更新                                 */
/**********************************************************************************************************************/

DECLARE
   lc_sql               character varying(1000);
BEGIN
   -- マテリアルを更新
   BEGIN
      lc_sql := 'REFRESH MATERIALIZED VIEW mv_tsuchisho_no';
      EXECUTE lc_sql;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;
END;
$$;
