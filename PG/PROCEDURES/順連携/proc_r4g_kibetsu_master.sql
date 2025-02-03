--------------------------------------------------------
--  DDL for Procedure proc_r4g_kibetsu_master
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_kibetsu_master (
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 期別マスタ情報（統合収滞納）                                                                            */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : 2025/02/03 CRESS-INFO.Angelo     新規作成     036o015「期別マスタ情報（統合収滞納）」の取込を行う       */
/**********************************************************************************************************************/

DECLARE
   rec_t_kibetsu                  t_kibetsu%ROWTYPE;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   ln_shori_count                 numeric DEFAULT 0;
   lc_err_cd                      character varying;
   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;

   ln_para01                      numeric DEFAULT 0;
   ln_fuka_nendo                  numeric;
   ln_nendo_kbn                   numeric DEFAULT 0;
   ln_kankatsu_cd                 numeric DEFAULT 0;
   ln_zeimoku_cd                  numeric;
   ln_kibetsu_cd                  numeric;
   lc_sql                         character varying;
   lc_zeimoku_cd                  character varying;
   lc_kibetsu                     character varying;
   lc_kibetsu_seishiki            character varying;
   lc_sort                        character varying;
   ln_result_cd_add               numeric DEFAULT 1; -- 追加
   ln_result_cd_upd               numeric DEFAULT 2; -- 更新
   ln_result_cd_err               numeric DEFAULT 9; -- エラー
   ln_yusen_flg                   numeric DEFAULT 0;

   lc_err_cd_normal               character varying = '0'; -- 通常
   lc_err_cd_err                  character varying = '9'; -- エラー

   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;

   cur_main CURSOR FOR
   SELECT *
   FROM dlgrenkei.i_r4g_kibetsu_master
   WHERE saishin_flg = '1'
   AND result_cd < 8;

   rec_main                      dlgrenkei.i_r4g_kibetsu_master%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM t_kibetsu
   WHERE fuka_nendo = rec_t_kibetsu.fuka_nendo
      AND zeimoku_cd = rec_t_kibetsu.zeimoku_cd
      AND kibetsu_cd = rec_t_kibetsu.kibetsu_cd;

   rec_lock                      t_kibetsu%ROWTYPE;

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
         lc_sql := 'TRUNCATE TABLE dlgmain.t_kibetsu';
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
         lc_err_text                    := '';
         rec_lock                       := NULL;
         rec_t_kibetsu                  := NULL;

         -- 賦課年度
         rec_t_kibetsu.fuka_nendo := get_str_to_num(rec_main.fuka_nendo);
         -- 年度区分
         rec_t_kibetsu.nendo_kbn := 0;
         -- 管轄コード
         rec_t_kibetsu.kankatsu_cd := 0;
         -- 税目コード
         rec_t_kibetsu.zeimoku_cd := get_str_to_num(get_r4g_code_conv(1, 3, rec_main.zeimoku_cd, null));
         -- 期別コード
         rec_t_kibetsu.kibetsu_cd := get_str_to_num(rec_main.kibetsu_cd);
         -- 期別
         rec_t_kibetsu.kibetsu := SUBSTRING(get_trimmed_space( rec_main.kibetsu_mei), 1, 1) || SUBSTRING(get_trimmed_space( rec_main.kibetsu_mei), 4, 1) || SUBSTRING((regexp_matches(get_trimmed_space( rec_main.kibetsu_mei), '\d+', 'g'))[1], 1, 2);
         -- 期別正式名称
         rec_t_kibetsu.kibetsu_seishiki := get_trimmed_space( rec_main.kibetsu_mei);
         -- 年月
         rec_t_kibetsu.nen_tsuki := get_trimmed_space(rec_main.ym);
         -- 納付書表示用期別
         rec_t_kibetsu.nofusho_kibetsu := ''; -- 納付書仕様確定後に決定
         -- 随時期フラグ
         rec_t_kibetsu.zuijiki_flg := 0;
         -- 表示順
         rec_t_kibetsu.sort_no := CONCAT(rec_main.fuka_nendo, LPAD(rec_t_kibetsu.nendo_kbn, 2, '0'), LPAD(rec_t_kibetsu.kankatsu_cd, 2, '0'), LPAD(rec_t_kibetsu.zeimoku_cd, 3, '0'), LPAD(rec_t_kibetsu.kibetsu_cd, 4, '0'));
         -- データ作成日時
         rec_t_kibetsu.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時
         rec_t_kibetsu.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_t_kibetsu.upd_tantosha_cd := rec_main.sosasha_cd;
         -- 更新端末名称
         rec_t_kibetsu.upd_tammatsu := 'SERVER';
         -- 削除フラグ
         rec_t_kibetsu.del_flg := get_str_to_num(rec_main.del_flg);

         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         IF rec_lock IS NULL THEN
            BEGIN
               INSERT INTO t_kibetsu(
                  fuka_nendo
                  , nendo_kbn
                  , kankatsu_cd
                  , zeimoku_cd
                  , kibetsu_cd
                  , kibetsu
                  , kibetsu_seishiki
                  , nen_tsuki
                  , nofusho_kibetsu
                  , zuijiki_flg
                  , sort_no
                  , ins_datetime
                  , upd_datetime
                  , upd_tantosha_cd
                  , upd_tammatsu
                  , del_flg
               ) VALUES (
                  rec_t_kibetsu.fuka_nendo
                  , rec_t_kibetsu.nendo_kbn
                  , rec_t_kibetsu.kankatsu_cd
                  , rec_t_kibetsu.zeimoku_cd
                  , rec_t_kibetsu.kibetsu_cd
                  , rec_t_kibetsu.kibetsu
                  , rec_t_kibetsu.kibetsu_seishiki
                  , rec_t_kibetsu.nen_tsuki
                  , rec_t_kibetsu.nofusho_kibetsu
                  , rec_t_kibetsu.zuijiki_flg
                  , rec_t_kibetsu.sort_no
                  , rec_t_kibetsu.ins_datetime
                  , rec_t_kibetsu.upd_datetime
                  , rec_t_kibetsu.upd_tantosha_cd
                  , rec_t_kibetsu.upd_tammatsu
                  , rec_t_kibetsu.del_flg
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
               UPDATE t_kibetsu
               SET kibetsu = rec_t_kibetsu.kibetsu
                  , kibetsu_seishiki = rec_t_kibetsu.kibetsu_seishiki
                  , nen_tsuki = rec_t_kibetsu.nen_tsuki
                  , zuijiki_flg = rec_t_kibetsu.zuijiki_flg
                  , upd_datetime = rec_t_kibetsu.upd_datetime
                  , upd_tantosha_cd = rec_t_kibetsu.upd_tantosha_cd
                  , upd_tammatsu = rec_t_kibetsu.upd_tammatsu
                  , del_flg = rec_t_kibetsu.del_flg
               WHERE fuka_nendo = rec_t_kibetsu.fuka_nendo
                  AND zeimoku_cd = rec_t_kibetsu.zeimoku_cd
                  AND kibetsu_cd = rec_t_kibetsu.kibetsu_cd;

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
            UPDATE dlgrenkei.i_r4g_kibetsu_master 
            SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
               , seq_no_renkei = in_n_renkei_seq
               , shori_ymd = in_n_shori_ymd
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND fuka_nendo = rec_main.fuka_nendo
               AND zeimoku_cd = rec_main.zeimoku_cd
               AND kibetsu_cd = rec_main.kibetsu_cd;
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