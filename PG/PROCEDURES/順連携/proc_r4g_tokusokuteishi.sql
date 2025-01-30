--------------------------------------------------------
--  DDL for Procedure proc_r4g_tokusokuteishi
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_tokusokuteishi (
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
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :  2025/01/28 CRESS-INFO.Angelo     新規作成     036o008「督促停止情報（統合収滞納）」の取込を行う        */
/**********************************************************************************************************************/

DECLARE
   rec_f_tokusokuteishi           f_tokusokuteishi%ROWTYPE;  
   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;

   ln_para01                      numeric DEFAULT 0;
   lc_sql                         character varying;

   ln_result_cd_add               numeric DEFAULT 1; -- 追加
   ln_result_cd_upd               numeric DEFAULT 2; -- 更新
   ln_result_cd_err               numeric DEFAULT 9; -- エラー

   lc_err_cd_normal               character varying = '0'; -- 通常
   lc_err_cd_err                  character varying = '9'; -- エラー

   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;
    
   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;
    
   cur_main CURSOR FOR
   SELECT *
   FROM dlgrenkei.i_r4g_tokusoku_teishi
   WHERE saishin_flg = '1'
      AND result_cd < 8;

   rec_main            dlgrenkei.i_r4g_tokusoku_teishi%ROWTYPE;
    
   cur_lock CURSOR FOR
   SELECT *
   FROM f_tokusokuteishi
   WHERE seq_no_tokusokuteishi = rec_f_tokusokuteishi.seq_no_tokusokuteishi
      AND kojin_no = rec_f_tokusokuteishi.kojin_no;
    
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
         lc_sql := 'TRUNCATE TABLE dlgmain.f_tokusokuteishi';
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
         lc_err_cd                      := lc_err_cd_normal;
         ln_result_cd                   := 0;
         lc_err_text                    := NULL;
         rec_lock                       := NULL;

         -- 督促停止管理番号
         rec_f_tokusokuteishi.seq_no_tokusokuteishi := rec_main.tokusoku_teishi_kanri_no;
         -- 個人番号
         rec_f_tokusokuteishi.kojin_no := rec_main.atena_no;
         -- 督促停止年月日
         rec_f_tokusokuteishi.teishi_ymd := get_ymd_str_to_num(rec_main.tokusoku_teishi_ymd);
         -- 督促停止事由コード
         rec_f_tokusokuteishi.teishi_jiyu_cd := get_str_to_num(rec_main.tokusoku_kaijo_jiyu);
         -- 督促停止解除年月日
         rec_f_tokusokuteishi.kaijo_ymd := get_ymd_str_to_num(rec_main.tokusoku_kaijo_ymd);
         -- 督促停止解除事由コード
         rec_f_tokusokuteishi.teishi_kaijo_riyu_cd := rec_main.tokusoku_teishi_jiyu;
         -- データ作成日時
         rec_f_tokusokuteishi.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時 
         rec_f_tokusokuteishi.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_f_tokusokuteishi.upd_tantosha_cd := rec_main.sosasha_cd;
         -- 更新端末名称
         rec_f_tokusokuteishi.upd_tammatsu := 'SERVER';
         -- 削除フラグ
         rec_f_tokusokuteishi.del_flg := get_str_to_num(rec_main.del_flg);

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
                  rec_f_tokusokuteishi.seq_no_tokusokuteishi
                  , rec_f_tokusokuteishi.kojin_no
                  , rec_f_tokusokuteishi.teishi_ymd
                  , rec_f_tokusokuteishi.teishi_jiyu_cd
                  , rec_f_tokusokuteishi.kaijo_ymd
                  , rec_f_tokusokuteishi.teishi_kaijo_riyu_cd
                  , rec_f_tokusokuteishi.ins_datetime
                  , rec_f_tokusokuteishi.upd_datetime
                  , rec_f_tokusokuteishi.upd_tantosha_cd
                  , rec_f_tokusokuteishi.upd_tammatsu
                  , rec_f_tokusokuteishi.del_flg
               );

               ln_ins_count := ln_ins_count + 1;
               lc_err_text := '';
               lc_err_cd := lc_err_cd_normal;
               ln_result_cd := ln_result_cd_add;

            EXCEPTION
               WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
                  lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                  lc_err_cd := lc_err_cd_err;
                  ln_result_cd := ln_result_cd_err;
            END;
         ELSE
            BEGIN
               UPDATE f_tokusokuteishi
               SET teishi_ymd = rec_f_tokusokuteishi.teishi_ymd
                  , teishi_jiyu_cd = rec_f_tokusokuteishi.teishi_jiyu_cd
                  , kaijo_ymd = rec_f_tokusokuteishi.kaijo_ymd
                  , teishi_kaijo_riyu_cd = rec_f_tokusokuteishi.teishi_kaijo_riyu_cd
                  , upd_datetime = rec_f_tokusokuteishi.upd_datetime
                  , upd_tantosha_cd = rec_f_tokusokuteishi.upd_tantosha_cd
                  , upd_tammatsu = rec_f_tokusokuteishi.upd_tammatsu
                  , del_flg = rec_f_tokusokuteishi.del_flg
               WHERE seq_no_tokusokuteishi = rec_main.seq_no_tokusokuteishi
                  AND kojin_no = rec_main.atena_no;

                  ln_upd_count := ln_upd_count + 1;
                  lc_err_text := '';
                  lc_err_cd := lc_err_cd_normal;
                  ln_result_cd := ln_result_cd_upd;

            EXCEPTION
               WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
                  lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                  lc_err_cd := lc_err_cd_err;
                  ln_result_cd := ln_result_cd_err;
            END;
         END IF;

         -- 中間テーブルの「削除フラグ」が「1」のデータは「3：削除」を指定
         IF get_str_to_num(rec_main.del_flg) = 1 THEN
            ln_del_count := ln_del_count + 1;
            ln_result_cd := ln_result_cd_del;
         END IF;

         BEGIN
            -- 中間テーブル更新
            UPDATE i_r4g_tokusoku_teishi 
            SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
               , seq_no_renkei = in_n_renkei_seq
               , shori_ymd = in_n_shori_ymd
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND tokusoku_teishi_kanri_no = rec_main.tokusoku_teishi_kanri_no
               AND atena_no = rec_main.atena_no;
         EXCEPTION
               WHEN OTHERS THEN NULL;
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
   CALL dlgrenkei.proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;