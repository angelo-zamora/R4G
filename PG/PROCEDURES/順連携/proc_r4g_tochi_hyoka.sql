--------------------------------------------------------
--  DDL for Procedure proc_r4g_tochi_hyoka
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_tochi_hyoka ( 
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : f_土地評価_連携（f_tochihyoka_renkei）の追加／更新／削除を実施する                                           */
/* 引数 IN  :  in_n_renkei_data_cd   … 連携データコード                                                                 */
/*            in_n_renkei_seq        … 連携SEQ（処理単位で符番されるSEQ）                                                */
/*            in_n_shori_ymd         … 処理日 （処理単位で設定される処理日）                                              */
/*      OUT : io_c_err_code          … 例外エラー発生時のエラーコード                                                    */
/*            io_c_err_text          … 例外エラー発生時のエラー内容                                                      */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/24  CRESS-INFO.Drexler     新規作成     012o017「土地評価情報」の取込を行う                         */
/**********************************************************************************************************************/

DECLARE

   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;
   rec_f_tochihyoka_renkei        f_tochihyoka_renkei%ROWTYPE;
   ln_para01                      numeric DEFAULT 0;
   ln_shori_count                 numeric DEFAULT 0;             -- 処理件数用変数
   ln_ins_count                   numeric DEFAULT 0;             -- 追加件数用変数
   ln_upd_count                   numeric DEFAULT 0;             -- 更新件数用変数
   ln_del_count                   numeric DEFAULT 0;             -- 削除件数用変数
   ln_err_count                   numeric DEFAULT 0;             -- エラー件数用変数
   lc_err_cd                      character varying;             -- エラーコード用変数
   lc_err_text                    character varying(100):='';    -- エラー内容用変数
   ln_result_cd                   numeric DEFAULT 0;             -- 結果区分更新用変数
   ln_result_cd_add               numeric DEFAULT 1;             -- 追加フラグ
   ln_result_cd_upd               numeric DEFAULT 2;             -- 更新フラグ
   ln_result_cd_del               numeric DEFAULT 3;             -- 削除フラグ
   ln_result_cd_warning           numeric DEFAULT 7;             -- 警告フラグ
   ln_result_cd_err               numeric DEFAULT 9;             -- エラーフラグ

   lc_err_cd_normal               character varying = '0';       -- 通常フラグ
   lc_err_cd_err                  character varying = '9';       -- エラーフラグ

   lc_sql                         character varying;             -- SQL文用変数
   
   -- メインカーソル
   cur_main CURSOR FOR
   SELECT *
   FROM dlgrenkei.i_r4g_tochi_hyoka
   WHERE saishin_flg = '1'
   AND kazei_nendo = (
         SELECT MAX(kazei_nendo)
         FROM dlgrenkei.i_r4g_tochi_hyoka
   )
   AND hyoka_nendo = (
         SELECT MAX(hyoka_nendo)
         FROM dlgrenkei.i_r4g_tochi_hyoka
   )
   AND hyoka_rireki_no = (
         SELECT MAX(hyoka_rireki_no)
         FROM dlgrenkei.i_r4g_tochi_hyoka
   )
   AND result_cd < 8;

   rec_main                          dlgrenkei.i_r4g_tochi_hyoka%ROWTYPE;
   
   -- パラメータ取得カーソル
   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                     dlgrenkei.f_renkei_parameter%ROWTYPE;

   -- 行ロック用カーソル
   cur_lock CURSOR FOR
   SELECT *
   FROM f_tochihyoka_renkei
   WHERE bukken_no = rec_main.bukken_no;

   rec_lock                          f_tochihyoka_renkei%ROWTYPE;

BEGIN
   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   --パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;

      END LOOP;
   CLOSE cur_parameter;

   --連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_tochihyoka_renkei;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_tochihyoka_renkei';
         EXECUTE lc_sql;
      EXCEPTION
         WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text := SQLERRM;
         RETURN;
      END; 
   END IF;

   --中間テーブルデータのエラーチェック
   CALL proc_error_check(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

   IF io_c_err_code <> '0' THEN
      RETURN;
   END IF;

   --連携データの作成・更新
   ln_shori_count := 0;

   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

            -- 物件番号
            rec_f_tochihyoka_renkei.bukken_no := rec_main.bukken_no;
            -- 課税年度
            rec_f_tochihyoka_renkei.kazei_nendo := rec_main.kazei_nendo::numeric;
            -- 評価年度
            rec_f_tochihyoka_renkei.hyoka_nendo := rec_main.hyoka_nendo::numeric;
            -- 評価額
            rec_f_tochihyoka_renkei.hyokagaku := rec_main.hyokagaku::numeric;
            -- 登録年月日
            rec_f_tochihyoka_renkei.toroku_ymd := get_hyphen_nashi_date(rec_main.sosa_ymd);
            -- 土地評価_履歴番号
            rec_f_tochihyoka_renkei.hyoka_rireki_no := rec_main.hyoka_rireki_no::numeric;
            -- データ作成日時
            rec_f_tochihyoka_renkei.ins_datetime := CONCAT(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            -- データ更新日時
            rec_f_tochihyoka_renkei.upd_datetime := CONCAT(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            -- 更新担当者コード
            rec_f_tochihyoka_renkei.upd_tantosha_cd := rec_main.sosasha_cd;
            -- 更新端末名称
            rec_f_tochihyoka_renkei.upd_tammatsu := 'SERVER';
            -- 削除フラグ
            rec_f_tochihyoka_renkei.del_flg := rec_main.del_flg::numeric;

            OPEN cur_lock;
               FETCH cur_lock INTO rec_lock;
            CLOSE cur_lock;

            IF rec_lock IS NULL THEN
               BEGIN
                  -- 登録処理
                  INSERT INTO f_tochihyoka_renkei(		
                     bukken_no
                     ,kazei_nendo
                     ,hyoka_nendo
                     ,hyokagaku
                     ,toroku_ymd
                     ,hyoka_rireki_no
                     ,ins_datetime
                     ,upd_datetime
                     ,upd_tantosha_cd
                     ,upd_tammatsu
                     ,del_flg
                  )
                  VALUES (
                     rec_f_tochihyoka_renkei.bukken_no
                     ,rec_f_tochihyoka_renkei.kazei_nendo
                     ,rec_f_tochihyoka_renkei.hyoka_nendo
                     ,rec_f_tochihyoka_renkei.hyokagaku
                     ,rec_f_tochihyoka_renkei.toroku_ymd
                     ,rec_f_tochihyoka_renkei.hyoka_rireki_no
                     ,rec_f_tochihyoka_renkei.ins_datetime
                     ,rec_f_tochihyoka_renkei.upd_datetime
                     ,rec_f_tochihyoka_renkei.upd_tantosha_cd
                     ,rec_f_tochihyoka_renkei.upd_tammatsu
                     ,rec_f_tochihyoka_renkei.del_flg
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
               -- 連携データの作成・更新
               BEGIN
                  UPDATE f_tochihyoka_renkei
                     SET kazei_nendo = rec_f_tochihyoka_renkei.kazei_nendo
                        ,hyoka_nendo = rec_f_tochihyoka_renkei.hyoka_nendo
                        ,hyokagaku = rec_f_tochihyoka_renkei.hyokagaku
                        ,toroku_ymd = rec_f_tochihyoka_renkei.toroku_ymd
                        ,hyoka_rireki_no = rec_f_tochihyoka_renkei.hyoka_rireki_no
                        ,ins_datetime = rec_f_tochihyoka_renkei.ins_datetime
                        ,upd_datetime = rec_f_tochihyoka_renkei.upd_datetime
                        ,upd_tantosha_cd = rec_f_tochihyoka_renkei.upd_tantosha_cd
                        ,upd_tammatsu = rec_f_tochihyoka_renkei.upd_tammatsu
                        ,del_flg = rec_f_tochihyoka_renkei.del_flg
                  WHERE bukken_no = rec_f_tochihyoka_renkei.bukken_no;

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

            -- 中間テーブルの「削除フラグ」が「1」のデータは「3：削除」を指定する
            IF rec_kojin.del_flg = 1 THEN
               ln_result_cd := ln_result_cd_del;
            END IF;

            -- 中間テーブル更新
            UPDATE i_r4g_tochi_hyoka
               SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
               WHERE  shikuchoson_cd = rec_main.shikuchoson_cd
            AND bukken_no = rec_main.bukken_no
            AND kazei_nendo = rec_main.kazei_nendo
            AND hyoka_nendo = rec_main.hyoka_nendo
            AND hyoka_rireki_no = rec_main.hyoka_rireki_no;
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
