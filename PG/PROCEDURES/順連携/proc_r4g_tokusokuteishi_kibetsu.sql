--------------------------------------------------------
--  DDL for Procedure proc_r4g_tokusokuteishi_kibetsu
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_tokusokuteishi_kibetsu (
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
/* 履歴　　 :  2025/01/30 CRESS-INFO.Angelo     新規作成     036o009「督促停止情報（統合収滞納）」の取込を行う        */
/**********************************************************************************************************************/

DECLARE
   rec_f_tokusokuteishi_kibetsu   f_tokusokuteishi_kibetsu%ROWTYPE;
   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;
   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;

   ln_para01                      numeric DEFAULT 0;
   lc_kibetsu_key                 character varying;
   lc_sql                         character varying;

   ln_result_cd_add               numeric DEFAULT 1; -- 追加
   ln_result_cd_upd               numeric DEFAULT 2; -- 更新
   ln_result_cd_err               numeric DEFAULT 9; -- エラー

   lc_err_cd_normal               character varying = '0'; -- 通常
   lc_err_cd_err                  character varying = '9'; -- エラー
    
   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;
    
   cur_main CURSOR FOR
   SELECT *
   FROM dlgrenkei.i_r4g_tokusoku_teishi_kibetsu
   WHERE saishin_flg = '1'
      AND result_cd < 8;

   rec_main                      dlgrenkei.i_r4g_tokusoku_teishi_kibetsu%ROWTYPE;
    
   cur_lock CURSOR FOR
   SELECT * 
   FROM f_tokusokuteishi_kibetsu
   WHERE seq_no_tokusokuteishi = rec_main.tokusoku_teishi_kanri_no
      AND kibetsu_key = rec_f_tokusokuteishi_kibetsu.kibetsu_key;

   rec_lock                      f_tokusokuteishi_kibetsu%ROWTYPE;

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
         lc_sql := 'TRUNCATE TABLE dlgmain.f_tokusokuteishi_kibetsu';
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
         rec_f_tokusokuteishi_kibetsu.seq_no_tokusokuteishi := rec_main.tokusoku_teishi_kanri_no;
         -- 期別明細KEY
         rec_f_tokusokuteishi_kibetsu.kibetsu_key := get_kibetsu_key(
            rec_main.fuka_nendo,
            rec_main.soto_nendo,
            rec_main.zeimoku_cd,
            rec_main.kibetsu_cd,
            rec_main.tokucho_shitei_no,
            rec_main.jido_atena_no,
            rec_main.tsuchisho_no,
            rec_main.jigyo_nendo_no,
            rec_main.shinkoku_rireki_no
         );
         -- 督促停止年月日
         rec_f_tokusokuteishi_kibetsu.tokusoku_teishi_ymd := get_ymd_str_to_num(rec_main.tokusoku_teishi_ymd);
         -- 督促停止事由
         rec_f_tokusokuteishi_kibetsu.tokusoku_kaijo_jiyu := get_str_to_num(rec_main.tokusoku_kaijo_jiyu);
         -- 督促停止解除年月日
         rec_f_tokusokuteishi_kibetsu.tokusoku_kaijo_ymd := get_ymd_str_to_num(rec_main.tokusoku_kaijo_ymd);
         -- 督促停止解除事由
         rec_f_tokusokuteishi_kibetsu.tokusoku_teishi_jiyu := rec_main.tokusoku_teishi_jiyu;
         -- データ作成日時
         rec_f_tokusokuteishi_kibetsu.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時
         rec_f_tokusokuteishi_kibetsu.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_f_tokusokuteishi_kibetsu.upd_tantosha_cd := rec_main.sosasha_cd;
         -- 更新端末名称
         rec_f_tokusokuteishi_kibetsu.upd_tammatsu := 'SERVER';
         -- 削除フラグ
         rec_f_tokusokuteishi_kibetsu.del_flg := get_str_to_num(rec_main.del_flg);
         
         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         IF rec_lock IS NULL THEN
            BEGIN
               INSERT INTO f_tokusokuteishi_kibetsu(
                  seq_no_tokusokuteishi
                  , kibetsu_key
                  , tokusoku_teishi_ymd
                  , tokusoku_kaijo_jiyu
                  , tokusoku_kaijo_ymd
                  , tokusoku_teishi_jiyu
                  , ins_datetime
                  , upd_datetime
                  , upd_tantosha_cd
                  , upd_tammatsu 
                  , del_flg
               ) VALUES (
                  seq_no_tokusokuteishi
                  , rec_f_tokusokuteishi_kibetsu.kibetsu_key
                  , rec_f_tokusokuteishi_kibetsu.tokusoku_teishi_ymd
                  , rec_f_tokusokuteishi_kibetsu.tokusoku_kaijo_jiyu
                  , rec_f_tokusokuteishi_kibetsu.tokusoku_kaijo_ymd
                  , rec_f_tokusokuteishi_kibetsu.tokusoku_teishi_jiyu
                  , rec_f_tokusokuteishi_kibetsu.ins_datetime
                  , rec_f_tokusokuteishi_kibetsu.upd_datetime
                  , rec_f_tokusokuteishi_kibetsu.upd_tantosha_cd
                  , rec_f_tokusokuteishi_kibetsu.upd_tammatsu 
                  , rec_f_tokusokuteishi_kibetsu.del_flg
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
               UPDATE f_tokusokuteishi_kibetsu
                  SET tokusoku_teishi_ymd = rec_f_tokusokuteishi_kibetsu.tokusoku_teishi_ymd
                  , tokusoku_kaijo_jiyu = rec_f_tokusokuteishi_kibetsu.tokusoku_kaijo_jiyu
                  , tokusoku_kaijo_ymd = rec_f_tokusokuteishi_kibetsu.tokusoku_kaijo_ymd
                  , tokusoku_teishi_jiyu = rec_f_tokusokuteishi_kibetsu.tokusoku_teishi_jiyu
                  , upd_datetime = rec_f_tokusokuteishi_kibetsu.upd_datetime
                  , upd_tantosha_cd = rec_f_tokusokuteishi_kibetsu.upd_tantosha_cd
                  , upd_tammatsu = rec_f_tokusokuteishi_kibetsu.upd_tammatsu 
                  , del_flg = rec_f_tokusokuteishi_kibetsu.del_flg
               WHERE seq_no_tokusokuteishi = rec_f_tokusokuteishi_kibetsu.seq_no_tokusokuteishi
                  AND kibetsu_key = rec_f_tokusokuteishi_kibetsu.kibetsu_key;

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
            UPDATE dlgrenkei.i_r4g_tokusoku_teishi_kibetsu 
            SET result_cd      = ln_result_cd
               , error_cd      = lc_err_cd
               , error_text    = lc_err_text
               , seq_no_renkei = in_n_renkei_seq
               , shori_ymd     = in_n_shori_ymd
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND tokusoku_teishi_kanri_no = rec_main.tokusoku_teishi_kanri_no
               AND fuka_nendo = rec_main.fuka_nendo
               AND soto_nendo = rec_main.soto_nendo
               AND tsuchisho_no = rec_main.tsuchisho_no
               AND zeimoku_cd = rec_main.zeimoku_cd
               AND tokucho_shitei_no = rec_main.tokucho_shitei_no
               AND kibetsu_cd = rec_main.kibetsu_cd
               AND shinkoku_rireki_no = rec_main.shinkoku_rireki_no
               AND jigyo_nendo_no = rec_main.jigyo_nendo_no
               AND jido_atena_no = rec_main.jido_atena_no
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