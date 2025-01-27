--------------------------------------------------------
--  DDL for Procedure  proc_upd_log
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_upd_log ( rec_log IN f_renkei_log, io_c_err_code INOUT character varying, io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : データ連携ログ更新                                                                                          */
/* 引数 IN  : rec_log … データ連携ログ                                                                                   */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                           */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                           */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/

BEGIN
   
   UPDATE dlgrenkei.f_renkei_log
   SET proc_kaishi_datetime = rec_log.proc_kaishi_datetime
      , proc_shuryo_datetime = rec_log.proc_shuryo_datetime
      , proc_shori_count = rec_log.proc_shori_count
      , proc_ins_count = rec_log.proc_ins_count
      , proc_upd_count = rec_log.proc_upd_count
      , proc_del_count = rec_log.proc_del_count
      , proc_jogai_count = rec_log.proc_jogai_count
      , proc_alert_count = rec_log.proc_alert_count
      , proc_err_count = rec_log.proc_err_count
	WHERE seq_no_renkei = rec_log.seq_no_renkei;
   
   EXCEPTION
      WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text := SQLERRM;
         RETURN;
END;
$$;
