--------------------------------------------------------
--  DDL for Procedure proc_r4g_tokusokuteishi
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_tokusokuteishi (
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 督促停止情報（統合収滞納）                                                                              */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                  */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                      */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/

DECLARE
   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;
   rec_log                        f_renkei_log%ROWTYPE;

   ln_para01                      numeric DEFAULT 0;
   lc_seq_no_tokusokuteishi       character varying;
   lc_kojin_no                    character varying;
   lc_sql                         character varying;
    
   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;
    
   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_tokusoku_teishi
   WHERE saishin_flg = '1'
      AND result_cd < 8;

   rec_main            i_r4g_tokusoku_teishi%ROWTYPE;
    
   cur_lock CURSOR FOR
   SELECT *
   FROM f_tokusokuteishi
   WHERE seq_no_tokusokuteishi = lc_seq_no_tokusokuteishi
      AND kojin_no = lc_kojin_no;
    
   rec_lock             f_tokusokuteishi%ROWTYPE;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;
   
   -- 1. パラメータ情報の取得
   OPEN cur_parameter;
   LOOP
      FETCH cur_parameter INTO rec_parameter;
      EXIT WHEN NOT FOUND;

      IF rec_parameter.parameter_no = 1 THEN
         ln_para01 := rec_parameter.parameter_value;
      END IF;
   END LOOP;
   CLOSE cur_parameter;

   -- 2. 連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_tokusokuteishi;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_tokusokuteishi;';
         EXECUTE lc_sql;
      EXCEPTION
         WHEN OTHERS THEN
            io_c_err_code := SQLSTATE;
            io_c_err_text    := SQLERRM;

            RETURN;
      END;
   END IF;

   -- 3. 中間テーブルデータのエラーチェック
   CALL proc_error_check(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);
      IF io_c_err_code <> '0' THEN
         RETURN;
      END IF;

   -- 4. 桁数設定情報取得
   -- r4gでは不要

   -- 5. 連携データの作成・更新
   ln_shori_count := 0;

   OPEN cur_main;
      LOOP

         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;
		 
         ln_shori_count                 := ln_shori_count + 1;
         lc_err_cd                      := '0';
         ln_result_cd                   := 0;
         lc_err_text                    := NULL;
         rec_lock                       := NULL;

         lc_seq_no_tokusokuteishi := rec_main.tokusoku_teishi_kanri_no;
         lc_kojin_no := rec_main.atena_no;

         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         IF rec_lock IS NULL THEN
            BEGIN
               INSERT INTO f_tokusokuteishi (
                  seq_no_tokusokuteishi
                  , kojin_no
                  , teishi_ymd
                  , teishi_jiyu_cd
                  , kaijo_ymd
                  , teishi_kaijo_riyu_cd
                  , ins_datetime
                  , upd_datetime
                  , upd_tantosha_cd
                  , upd_tammatsu
                  , del_flg
               ) VALUES (
                  lc_seq_no_tokusokuteishi
                  , lc_kojin_no
                  , get_date_to_num(to_date(rec_main.tokusoku_teishi_ymd, 'yyyy-mm-dd'))
                  , CASE WHEN rec_main.tokusoku_kaijo_jiyu IS NOT NULL OR rec_main.tokusoku_kaijo_jiyu <> '' THEN rec_main.tokusoku_kaijo_jiyu::numeric ELSE 0 END
                  , get_date_to_num(to_date(rec_main.tokusoku_kaijo_ymd, 'yyyy-mm-dd'))
                  , rec_main.tokusoku_teishi_jiyu
                  , concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                  , concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                  , rec_main.sosasha_cd
                  , 'SERVER'
                  , CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> '' THEN rec_main.del_flg::numeric ELSE 0 END
               );

               ln_ins_count := ln_ins_count + 1;
               lc_err_text := '';
               lc_err_cd := '0';
               ln_result_cd := 1;

               EXCEPTION
                  WHEN OTHERS THEN
                     ln_err_count := ln_err_count + 1;
                     lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                     lc_err_cd := '9';
                     ln_result_cd := 9;
            END;
         ELSE
            BEGIN
               UPDATE f_tokusokuteishi
               SET teishi_ymd = get_date_to_num(to_date(rec_main.tokusoku_teishi_ymd, 'yyyy-mm-dd'))
                  , teishi_jiyu_cd = CASE WHEN rec_main.tokusoku_kaijo_jiyu IS NOT NULL OR rec_main.tokusoku_kaijo_jiyu <> '' THEN rec_main.tokusoku_kaijo_jiyu::numeric ELSE 0 END
                  , kaijo_ymd = get_date_to_num(to_date(rec_main.tokusoku_kaijo_ymd, 'yyyy-mm-dd'))
                  , teishi_kaijo_riyu_cd = rec_main.tokusoku_teishi_jiyu
                  , upd_datetime = CURRENT_TIMESTAMP
                  , upd_tantosha_cd = rec_main.sosasha_cd
                  , upd_tammatsu = 'SERVER'
                  , del_flg = rec_main.del_flg::numeric
               WHERE seq_no_tokusokuteishi = lc_seq_no_tokusokuteishi
                  AND kojin_no = lc_kojin_no;

                  ln_upd_count := ln_upd_count + 1;
                  lc_err_text := '';
                  lc_err_cd := '0';
                  ln_result_cd := 2;

            EXCEPTION
               WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
                  lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                  lc_err_cd := '9';
                  ln_result_cd := 9;
            END;
         END IF;
		 
       BEGIN
		 -- 中間テーブル更新
         UPDATE i_r4g_tokusoku_teishi 
         SET result_cd = ln_result_cd
            , error_cd = lc_err_cd
            , error_text = lc_err_text
         WHERE shikuchoson_cd = rec_main.shikuchoson_cd
            AND tokusoku_teishi_kanri_no = rec_main.tokusoku_teishi_kanri_no
            AND atena_no = rec_main.atena_no;
       EXCEPTION
            WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
               lc_err_cd := '9';
               ln_result_cd := 9;
       END;
			
      END LOOP;
   CLOSE cur_main;
   
   rec_log.seq_no_renkei := in_n_renkei_seq;
   rec_log.proc_shuryo_datetime := CURRENT_TIMESTAMP;
   rec_log.proc_shori_count := ln_shori_count;
   rec_log.proc_ins_count := ln_ins_count;
   rec_log.proc_upd_count := ln_upd_count;
   rec_log.proc_del_count := ln_del_count;
   rec_log.proc_err_count := ln_err_count;
   
   -- データ連携ログ更新
   CALL proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;
   
EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;