--------------------------------------------------------
--  DDL for Procedure proc_r4g_dairinin
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_dairinin (
   in_n_renkei_data_cd IN numeric,
   in_n_renkei_seq IN numeric,
   in_n_shori_ymd IN numeric,
   io_c_err_code INOUT character varying,
   io_c_err_text INOUT character varying
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 代理人情報（統合収滞納）                                                                                */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :  2025/01/30 CRESS-INFO.Angelo   新規作成     036o013「代理人情報（統合収滞納）」の取込を行う            */
/**********************************************************************************************************************/

DECLARE
   rec_f_dairinin                      f_dairinin%ROWTYPE;
   ln_para01                           numeric DEFAULT 0;
   ln_zeimoku_cd                       numeric;
   ln_dairinin_yukokikan_kaishi_ymd    numeric;
   ln_dairinin_yukokikan_shuryo_ymd    numeric;
   
   lc_nozeigimusha_kojin_no            character varying;
   lc_dairinin_kojin_no                character varying;
   lc_gyomu_cd                         character varying;
   lc_zeimoku_cd                       character varying;
   lc_denwa                            character varying;
   lc_sql                              character varying(1000);
   
   ln_shori_count                      numeric DEFAULT 0;
   ln_ins_count                        numeric DEFAULT 0;
   ln_upd_count                        numeric DEFAULT 0;
   ln_del_count                        numeric DEFAULT 0;
   ln_err_count                        numeric DEFAULT 0;
   lc_err_text                         character varying(100);
   ln_result_cd                        numeric DEFAULT 0;
   lc_err_cd                           character varying;

   ln_result_cd_add                    numeric DEFAULT 1; -- 追加
   ln_result_cd_upd                    numeric DEFAULT 2; -- 更新
   ln_result_cd_err                    numeric DEFAULT 9; -- エラー

   lc_err_cd_normal                    character varying = '0'; -- 通常
   lc_err_cd_err                       character varying = '9'; -- エラー

   rec_log                             dlgrenkei.f_renkei_log%ROWTYPE;
   
   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                       dlgrenkei.f_renkei_parameter%ROWTYPE;
   
   cur_main CURSOR FOR
   SELECT
    *
   FROM
      dlgrenkei.i_r4g_dairinin
   WHERE
      saishin_flg = '1'
      AND katagaki <> '09'
      AND result_cd < 8;

   rec_main                            dlgrenkei.i_r4g_dairinin%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM f_dairinin
   WHERE nozeigimusha_kojin_no = rec_f_dairinin.nozeigimusha_kojin_no
      AND dairinin_kojin_no = rec_f_dairinin.dairinin_kojin_no
      AND gyomu_cd = rec_f_dairinin.gyomu_cd
      AND zeimoku_cd = rec_f_dairinin.zeimoku_cd
      AND dairinin_yukokikan_kaishi_ymd = rec_f_dairinin.dairinin_yukokikan_kaishi_ymd;
   
   rec_lock                            f_dairinin%ROWTYPE;
   
BEGIN
   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;
   ln_err_count = 0;

   CALL proc_error_check( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );
   
   IF io_c_err_code <> '0'  THEN
      RETURN;
   END IF;

   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN
           ln_para01 := rec_parameter.parameter_value;
         END IF;
      
      END LOOP;
   CLOSE cur_parameter;

   IF ln_para01 = 1 THEN
      BEGIN
         UPDATE
            f_taino
         SET
            kanrinin_kojin_no = 0
         where
            kanrinin_cd <> 0
            and del_flg = 0;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_dairinin';
      	EXECUTE lc_sql;
      EXCEPTION
         WHEN OTHERS THEN
            io_c_err_code := SQLSTATE;
            io_c_err_text := SQLERRM;
            RETURN;
      END;
   END IF;

   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

         -- 納税義務者_宛名番号
         rec_f_dairinin.nozeigimusha_kojin_no := rec_main.atena_no;
         -- 代理人_宛名番号
         rec_f_dairinin.dairinin_kojin_no := rec_main.dairinin_atena_no;
         -- 業務コード
         rec_f_dairinin.gyomu_cd := rec_main.gyomu_id;
         -- 税目コード
         rec_f_dairinin.zeimoku_cd := get_r4g_code_conv(0, 3, rec_main.zeimoku_cd, NULL)::numeric;
         -- 代理人_有効期間（開始年月日）
         rec_f_dairinin.dairinin_yukokikan_kaishi_ymd := get_ymd_str_to_num(rec_main.dairinin_yukokikan_kaishi_ymd);
         -- 代理人_有効期間（終了年月日）
         rec_f_dairinin.dairinin_yukokikan_shuryo_ymd := get_ymd_str_to_num(rec_main.dairinin_yukokikan_shuryo_ymd);
         -- 代理人_肩書
         rec_f_dairinin.dairinin_katagaki := rec_main.katagaki;
         -- メモ
         rec_f_dairinin.memo := rec_main.memo;
         -- 連絡先区分
         rec_f_dairinin.renrakusaki_kbn := get_str_to_num(rec_main.renrakusaki_kbn);
         -- 電話番号
         rec_f_dairinin.denwa_no := rec_main.denwa_no;
         -- データ作成日時
         rec_f_dairinin.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時
         rec_f_dairinin.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_f_dairinin.upd_tantosha_cd := rec_main.sosasha_cd;
         -- 更新端末名称
         rec_f_dairinin.upd_tammatsu := 'SERVER';
         -- 削除フラグ
         rec_f_dairinin.del_flg := get_str_to_num(rec_main.del_flg);

         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         IF rec_lock IS NULL THEN
            BEGIN
               INSERT INTO f_dairinin(
                  nozeigimusha_kojin_no
                  , dairinin_kojin_no
                  , gyomu_cd
                  , zeimoku_cd
                  , dairinin_yukokikan_kaishi_ymd
                  , dairinin_yukokikan_shuryo_ymd
                  , dairinin_katagaki
                  , memo
                  , renrakusaki_kbn
                  , denwa_no
                  , ins_datetime
                  , upd_datetime
                  , upd_tantosha_cd
                  , upd_tammatsu
                  , del_flg)
               VALUES (
                  rec_f_dairinin.nozeigimusha_kojin_no
                  , rec_f_dairinin.dairinin_kojin_no
                  , rec_f_dairinin.gyomu_cd
                  , rec_f_dairinin.zeimoku_cd
                  , rec_f_dairinin.dairinin_yukokikan_kaishi_ymd
                  , rec_f_dairinin.dairinin_yukokikan_shuryo_ymd
                  , rec_f_dairinin.dairinin_katagaki
                  , rec_f_dairinin.memo
                  , rec_f_dairinin.renrakusaki_kbn
                  , rec_f_dairinin.denwa_no
                  , rec_f_dairinin.ins_datetime
                  , rec_f_dairinin.upd_datetime
                  , rec_f_dairinin.upd_tantosha_cd
                  , rec_f_dairinin.upd_tammatsu
                  , rec_f_dairinin.del_flg
                  );

               ln_ins_count := ln_ins_count + 1;
               lc_err_cd    := lc_err_cd_normal;
               lc_err_text  := '';
               ln_result_cd := ln_result_cd_add;

            EXCEPTION WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_cd    := lc_err_cd_err;
               lc_err_text  := SUBSTRING( SQLERRM, 1, 100 );
               ln_result_cd := ln_result_cd_err;
            END;
         ELSE
            BEGIN
               UPDATE f_dairinin
               SET dairinin_yukokikan_shuryo_ymd = rec_f_dairinin.dairinin_yukokikan_shuryo_ymd
                  , dairinin_katagaki = rec_f_dairinin.dairinin_katagaki
                  , memo = rec_f_dairinin.memo
                  , renrakusaki_kbn = rec_f_dairinin.renrakusaki_kbn
                  , denwa_no = rec_f_dairinin.denwa_no
                  , upd_datetime = rec_f_dairinin.upd_datetime
                  , upd_tantosha_cd = rec_f_dairinin.upd_tantosha_cd
                  , upd_tammatsu = rec_f_dairinin.upd_tammatsu
                  , del_flg = rec_f_dairinin.del_flg
               WHERE nozeigimusha_kojin_no = rec_f_dairinin.nozeigimusha_kojin_no
                  AND dairinin_kojin_no = rec_f_dairinin.dairinin_atena_no
                  AND gyomu_cd = rec_f_dairinin.gyomu_cd
                  AND zeimoku_cd = rec_f_dairinin.zeimoku_cd
                  AND dairinin_yukokikan_kaishi_ymd = rec_f_dairinin.dairinin_yukokikan_kaishi_ymd;

               ln_upd_count := ln_upd_count + 1;
               lc_err_cd    := lc_err_cd_normal;
               lc_err_text  := '';
               ln_result_cd := ln_result_cd_upd;
                  
            EXCEPTION
               WHEN OTHERS THEN
                  ln_result_cd := ln_result_cd_err;
                  ln_err_count := ln_err_count + 1;
                  lc_err_cd    := lc_err_cd_err;
                  lc_err_text  := SUBSTRING( SQLERRM, 1, 100 );
            END;
         END IF;

         -- 中間テーブルの「削除フラグ」が「1」のデータは「3：削除」を指定
         IF get_str_to_num(rec_main.del_flg) = 1 THEN
               ln_del_count := ln_del_count + 1;
               ln_result_cd := ln_result_cd_del;
         END IF;

         BEGIN 
            UPDATE dlgrenkei.i_r4g_dairinin
            SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
               , seq_no_renkei = in_n_renkei_seq
               , shori_ymd = in_n_shori_ymd
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
            AND atena_no = rec_main.atena_no
            AND dairinin_atena_no = rec_main.dairinin_atena_no
            AND gyomu_id = rec_main.gyomu_id
            AND zeimoku_cd = rec_main.zeimoku_cd
            AND dairinin_yukokikan_kaishi_ymd = rec_main.dairinin_yukokikan_kaishi_ymd;
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
