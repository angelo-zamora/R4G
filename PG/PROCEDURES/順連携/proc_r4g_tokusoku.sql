--------------------------------------------------------
--  DDL for Procedure  proc_r4g_tokusoku
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_tokusoku ( 
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 機能概要 : 督促情報（統合収滞納）                                                                                     */
/* 入力 IN  : in_n_renkei_data_cd  連携データコード                                                                     */
/*            in_n_renkei_seq      連携SEQ（連携の一意性を保つSEQ）                                                      */
/*            in_n_shori_ymd       処理日 （連携の処理を行う日付）                                                       */
/* 出力 OUT : io_c_err_code        結果エラーが発生した場合のエラーコード                                                 */
/*            io_c_err_text        結果エラーが発生した場合のエラーメッセージ                                             */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/23  CRESS-INFO.Drexler     新規作成     036o007「督促情報（統合収滞納）」の取込を行う                */
/**********************************************************************************************************************/

DECLARE

   rec_tokusoku                   f_tokusoku_kibetsu%ROWTYPE;
   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;

   ln_shori_count                 numeric DEFAULT 0;             -- 処理件数用変数
   ln_ins_count                   numeric DEFAULT 0;             -- 追加件数用変数
   ln_upd_count                   numeric DEFAULT 0;             -- 更新件数用変数
   ln_del_count                   numeric DEFAULT 0;             -- 削除件数用変数
   ln_err_count                   numeric DEFAULT 0;             -- エラー件数用変数
   lc_err_cd                      character varying;             -- エラーコード用変数
   lc_err_text                    character varying(100):='';    -- エラー内容用変数
   ln_result_cd                   numeric DEFAULT 0;             -- 結果区分更新用変数

   ln_para01                      numeric DEFAULT 0;

   ln_result_cd_add               numeric DEFAULT 1;              -- 追加フラグ
   ln_result_cd_upd               numeric DEFAULT 2;              -- 更新フラグ
   ln_result_cd_del               numeric DEFAULT 3;              -- 削除フラグ
   ln_result_cd_warning           numeric DEFAULT 7;              -- 警告フラグ
   ln_result_cd_err               numeric DEFAULT 9;              -- エラーフラグ

   lc_err_cd_normal               character varying = '0';        -- 通常フラグ
   lc_err_cd_err                  character varying = '9';        -- エラーフラグ

   lc_sql                         character varying;              -- SQL文用変数

   -- メインカーソル
   cur_main CURSOR FOR
   SELECT *
   FROM dlgrenkei.i_r4g_tokusoku as main
   WHERE main.saishin_flg = '1'
   AND main.result_cd < 8
   AND main.rireki_no = (
    SELECT MAX(rireki_no)
    FROM dlgrenkei.i_r4g_tokusoku AS sub
    WHERE sub.fuka_nendo = main.fuka_nendo
      AND sub.soto_nendo = main.soto_nendo
      AND sub.tsuchisho_no = main.tsuchisho_no
      AND sub.zeimoku_cd = main.zeimoku_cd
      AND sub.tokucho_shitei_no = main.tokucho_shitei_no
      AND sub.kibetsu_cd = main.kibetsu_cd
      AND sub.shinkoku_rireki_no = main.shinkoku_rireki_no
      AND sub.jigyo_nendo_no = main.jigyo_nendo_no
      AND sub.jido_atena_no = main.jido_atena_no
      AND sub.atena_no = main.atena_no
  );

   rec_main                       dlgrenkei.i_r4g_tokusoku%ROWTYPE;

   -- パラメータ取得カーソル
   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;
   
   -- 行ロック用カーソル
   cur_lock CURSOR FOR
   SELECT *
   FROM f_tokusoku_kibetsu
   WHERE kibetsu_key = rec_tokusoku.kibetsu_key;

   rec_lock                       f_tokusoku_kibetsu%ROWTYPE;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   -- １．パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN 
            ln_para01 := rec_parameter.parameter_value; 
         END IF;

      END LOOP;
   CLOSE cur_parameter;

   -- ２．連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         lc_sql := 'TRUNCATE TABLE dlgmain.f_tokusoku_kibetsu';
         EXECUTE lc_sql;
      EXCEPTION
         WHEN OTHERS THEN
            io_c_err_code := SQLSTATE;
            io_c_err_text := SQLERRM;
            RETURN;
      END;
   END IF;

   -- ３．中間テーブルデータのエラーチェック
   CALL proc_error_check( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );

   IF io_c_err_code <> 0 THEN
      RETURN;
   END IF;

   -- ４．桁数設定情報取得
   -- r4gによる処理
   
   ln_shori_count := 0;

   -- ５．連携データの作成・更新
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

         ln_shori_count := ln_shori_count + 1;
         
         -- 期別明細KEY
         rec_tokusoku.kibetsu_key := get_kibetsu_key(rec_main.fuka_nendo, rec_main.soto_nendo, rec_main.zeimoku_cd, rec_main.kibetsu_cd, rec_main.tokucho_shitei_no);
         -- 督促状発行日
         rec_tokusoku.tokusoku_ymd := getdatetonum(to_date(rec_main.tokusoku_hakko_ymd, 'YYYY-MM-DD'));
         -- 督促状返戻日
         rec_tokusoku.tokusoku_henrei_ymd := getdatetonum(to_date(rec_main.tokusoku_henrei_ymd, 'YYYY-MM-DD'));
         -- 督促区分
         rec_tokusoku.tokusoku_kbn := rec_main.tokusoku_kbn::numeric;
         -- 引き抜き（削除）区分
         rec_tokusoku.hikinuki_kbn := rec_main.hikinuki_del_kbn::numeric;
         -- 引き抜き（削除）事由
         rec_tokusoku.hikinuki_jiyu_cd := rec_main.hikinuki_del_jiyu_cd::numeric;
         -- 履歴番号
         rec_tokusoku.rireki_no := rec_main.shinkoku_rireki_no::numeric;
         -- データ作成日時
         rec_tokusoku.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時
         rec_tokusoku.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_tokusoku.upd_tantosha_cd := rec_main.upd_tantosha_cd;
         -- 更新端末名称
         rec_tokusoku.upd_tammatsu := 'SERVER';
         -- 削除フラグ
         rec_tokusoku.del_flg := rec_main.del_flg::numeric;

         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         IF rec_lock IS NULL THEN
            BEGIN
               INSERT INTO f_tokusoku_kibetsu(
                  kibetsu_key
                  ,tokusoku_ymd
                  ,tokusoku_henrei_ymd
                  ,tokusoku_kbn
                  ,hikinuki_kbn
                  ,hikinuki_jiyu_cd
                  ,rireki_no
                  ,ins_datetime
                  ,upd_datetime
                  ,upd_tantosha_cd
                  ,upd_tammatsu
                  ,del_flg
               )
               VALUES (
                  rec_tokusoku.kibetsu_key
                  ,rec_tokusoku.tokusoku_ymd
                  ,rec_tokusoku.tokusoku_henrei_ymd
                  ,rec_tokusoku.tokusoku_kbn
                  ,rec_tokusoku.hikinuki_kbn
                  ,rec_tokusoku.hikinuki_jiyu_cd
                  ,rec_tokusoku.rireki_no
                  ,rec_tokusoku.ins_datetime
                  ,rec_tokusoku.upd_datetime
                  ,rec_tokusoku.upd_tantosha_cd
                  ,rec_tokusoku.upd_tammatsu
                  ,rec_tokusoku.del_flg
               );
                
               ln_ins_count := ln_ins_count + 1;
               lc_err_text := '';
               lc_err_cd := lc_err_cd_normal;
               ln_result_cd := ln_result_cd_add;

            EXCEPTION WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
               lc_err_cd := lc_err_cd_err;
               ln_result_cd := ln_result_cd_err;
            END;
         ELSE
            BEGIN
               UPDATE f_tokusoku_kibetsu
               SET tokusoku_ymd = rec_tokusoku.tokusoku_ymd
                  , tokusoku_henrei_ymd = rec_tokusoku.tokusoku_henrei_ymd
                  , tokusoku_kbn = rec_tokusoku.tokusoku_kbn
                  , hikinuki_kbn = rec_tokusoku.hikinuki_kbn
                  , hikinuki_jiyu_cd = rec_tokusoku.hikinuki_jiyu_cd
                  , rireki_no = rec_tokusoku.rireki_no
                  , upd_datetime = rec_tokusoku.upd_datetime
                  , upd_tantosha_cd = rec_tokusoku.upd_tantosha_cd
                  , upd_tammatsu = rec_tokusoku.upd_tammatsu
                  , del_flg = rec_tokusoku.del_flg
               WHERE kibetsu_key = rec_tokusoku.kibetsu_key;
               
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
         IF rec_main.del_flg::numeric = 1 THEN
            ln_del_count := ln_del_count + 1;
            ln_result_cd := ln_result_cd_del;
         END IF;

         -- 中間テーブル更新
         BEGIN
            UPDATE dlgrenkei.i_r4g_tokusoku
            SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
               , seq_no_renkei = in_n_renkei_seq
               , shori_ymd     = in_n_shori_ymd
            WHERE  fuka_nendo = rec_main.fuka_nendo
               AND soto_nendo = rec_main.soto_nendo
               AND tsuchisho_no = rec_main.tsuchisho_no
               AND zeimoku_cd = rec_main.zeimoku_cd
               AND tokucho_shitei_no = rec_main.tokucho_shitei_no
               AND kibetsu_cd = rec_main.kibetsu_cd
               AND shinkoku_rireki_no = rec_main.shinkoku_rireki_no
               AND jigyo_nendo_no = rec_main.jigyo_nendo_no
               AND jido_atena_no = rec_main.jido_atena_no
               AND atena_no = rec_main.atena_no
               AND rireki_no = rec_main.rireki_no;
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
   
   -- データ更新処理
   CALL dlgrenkei.proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

   EXCEPTION WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;
