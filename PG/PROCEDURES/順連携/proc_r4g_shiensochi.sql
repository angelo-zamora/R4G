--------------------------------------------------------
--  DDL for Procedure proc_r4g_shiensochi
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_shiensochi(
   in_n_renkei_data_cd numeric,
   in_n_renkei_seq numeric,
   in_n_shori_ymd numeric,
   INOUT io_c_err_code character varying,
   INOUT io_c_err_text character varying)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 支援措置対象者情報                                                                                      */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :  2025/01/24 CRESS-INFO.Angelo     新規作成     001o009「支援措置対象者情報」の取込を行う                */
/**********************************************************************************************************************/

DECLARE
   rec_f_shiensochi               f_shiensochi%ROWTYPE;
   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;
   ln_result_cd_add               numeric DEFAULT 1; -- 追加
   ln_result_cd_upd               numeric DEFAULT 2; -- 更新
   ln_result_cd_del               numeric DEFAULT 3; -- 削除
   ln_result_cd_warning           numeric DEFAULT 7; -- 警告
   ln_result_cd_err               numeric DEFAULT 9; -- エラー

   lc_err_cd_normal               character varying = '0'; -- 通常
   lc_err_cd_err                  character varying = '9'; -- エラー

   ln_para01 numeric DEFAULT 0;
   lc_kojin_no character varying;
   ln_kaishi_ymd numeric;
   lc_sql character varying;
   
   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter dlgrenkei.f_renkei_parameter%ROWTYPE;

   cur_main CURSOR FOR
   SELECT
      *
   FROM dlgrenkei.i_r4g_shiensochi AS tbl_shiensochi
   WHERE tbl_shiensochi.saishin_flg = '1' 
   AND tbl_shiensochi.rireki_no = (
      SELECT MAX(rireki_no)
      FROM dlgrenkei.i_r4g_shiensochi
      WHERE atena_no = tbl_shiensochi.atena_no
   )
   AND tbl_shiensochi.result_cd < 8;

   rec_main dlgrenkei.i_r4g_shiensochi%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM f_shiensochi
   WHERE kojin_no  = lc_kojin_no 
      AND kaishi_ymd = ln_kaishi_ymd; 

   rec_lock f_shiensochi%ROWTYPE;

   cur_busho CURSOR FOR
   SELECT *
   FROM t_busho
   ORDER BY busho_cd;

   rec_busho            t_busho%ROWTYPE;
   rec_log         dlgrenkei.f_renkei_log%ROWTYPE;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;
   
   -- パラメータ情報の取得
   OPEN cur_parameter;
   LOOP
      FETCH cur_parameter INTO rec_parameter;
      EXIT WHEN NOT FOUND;

      IF rec_parameter.parameter_no = 1 THEN
         ln_para01 := rec_parameter.parameter_value;
      END IF;
   END LOOP;
   CLOSE cur_parameter;

   -- 連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         lc_sql := 'TRUNCATE TABLE dlgmain.f_shiensochi';
         EXECUTE lc_sql;

      EXCEPTION
         WHEN OTHERS THEN
            io_c_err_code := SQLSTATE;
            io_c_err_text := SQLERRM;

            RETURN;
      END;
   END IF;

   -- 中間テーブルデータのエラーチェック
   CALL proc_error_check(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

   IF io_c_err_code <> '0' THEN
      RETURN;
   END IF;

   ln_shori_count := 0;
   -- メイン処理
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

         ln_shori_count                 := ln_shori_count + 1;
         lc_err_cd                      := lc_err_cd_normal;
         ln_result_cd                   := 0;
         rec_busho                      := NULL;
         rec_lock                       := NULL;

         lc_kojin_no := rec_main.atena_no::character varying;
         ln_kaishi_ymd := get_ymd_str_to_num(rec_main.shiensochi_kaishi_ymd);

         -- 個人番号
         rec_f_shiensochi.kojin_no := lc_kojin_no;
         -- 期間開始日
         rec_f_shiensochi.kaishi_ymd := ln_kaishi_ymd;
         -- 支援措置区分
         rec_f_shiensochi.shiensochi_kbn := rec_main.sochi_jkn;
         -- 期間終了日
         rec_f_shiensochi.shuryo_ymd := CASE WHEN rec_main.shiensochi_shuryo_ymd IS NULL OR rec_main.shiensochi_shuryo_ymd = '' THEN 99999999 ELSE get_ymd_str_to_num(rec_main.shiensochi_shuryo_ymd) END;
         -- 一時解除（照会）フラグ
         rec_f_shiensochi.kaijo_shokai_flg := 0;
         -- 一時解除（照会）開始日時
         rec_f_shiensochi.kaijo_shokai_kaishi_datetime := NULL;
         -- 一時解除（照会）終了日時
         rec_f_shiensochi.kaijo_shokai_shuryo_datetime := NULL;
         -- 一時解除（発行）フラグ
         rec_f_shiensochi.kaijo_hakko_flg := 0;
         -- 一時解除（発行）開始日時
         rec_f_shiensochi.kaijo_hakko_kaishi_datetime := NULL;
         -- 一時解除（発行）終了日時
         rec_f_shiensochi.kaijo_hakko_shuryo_datetime := NULL;
         -- 一時解除（発行）回数
         rec_f_shiensochi.kaijo_hakko_kaisu := 1;
         -- 異動フラグ
         rec_f_shiensochi.ido_flg := 0;
         -- 終了フラグ
         rec_f_shiensochi.shuryo_flg := 0;
         -- 備考
         rec_f_shiensochi.biko := NULL;
         -- 履歴番号
         rec_f_shiensochi.rireki_no := get_str_to_num(rec_main.rireki_no);
         -- データ作成日時
         rec_f_shiensochi.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時
         rec_f_shiensochi.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_f_shiensochi.upd_tantosha_cd := rec_main.sosasha_cd;
         -- 更新端末名称
         rec_f_shiensochi.upd_tammatsu := 'SERVER';
         -- 削除フラグ
         rec_f_shiensochi.del_flg := get_str_to_num(rec_main.del_flg);

         OPEN cur_lock;
               FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         IF rec_lock IS NULL THEN
               BEGIN
                  INSERT INTO f_shiensochi(
                        kojin_no
                     , kaishi_ymd
                     , shiensochi_kbn
                     , shuryo_ymd
                     , kaijo_shokai_flg
                     , kaijo_shokai_kaishi_datetime
                     , kaijo_shokai_shuryo_datetime
                     , kaijo_hakko_flg
                     , kaijo_hakko_kaishi_datetime
                     , kaijo_hakko_shuryo_datetime
                     , kaijo_hakko_kaisu
                     , ido_flg
                     , shuryo_flg
                     , biko
                     , rireki_no
                     , ins_datetime
                     , upd_datetime
                     , upd_tantosha_cd
                     , upd_tammatsu
                     , del_flg
                  ) VALUES (
                        rec_f_shiensochi.kojin_no
                     , rec_f_shiensochi.kaishi_ymd
                     , rec_f_shiensochi.shiensochi_kbn
                     , rec_f_shiensochi.shuryo_ymd
                     , rec_f_shiensochi.kaijo_shokai_flg
                     , rec_f_shiensochi.kaijo_shokai_kaishi_datetime
                     , rec_f_shiensochi.kaijo_shokai_shuryo_datetime
                     , rec_f_shiensochi.kaijo_hakko_flg
                     , rec_f_shiensochi.kaijo_hakko_kaishi_datetime
                     , rec_f_shiensochi.kaijo_hakko_shuryo_datetime
                     , rec_f_shiensochi.kaijo_hakko_kaisu
                     , rec_f_shiensochi.ido_flg
                     , rec_f_shiensochi.shuryo_flg
                     , rec_f_shiensochi.biko
                     , rec_f_shiensochi.rireki_no
                     , rec_f_shiensochi.ins_datetime
                     , rec_f_shiensochi.upd_datetime
                     , rec_f_shiensochi.upd_tantosha_cd
                     , rec_f_shiensochi.upd_tammatsu
                     , rec_f_shiensochi.del_flg
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
                  UPDATE f_shiensochi
                  SET shiensochi_kbn = rec_f_shiensochi.shiensochi_kbn
                     , shuryo_ymd = rec_f_shiensochi.shuryo_ymd
                     , rireki_no = rec_f_shiensochi.rireki_no
                     , upd_datetime = rec_f_shiensochi.upd_datetime
                     , upd_tantosha_cd = rec_f_shiensochi.upd_tantosha_cd
                     , upd_tammatsu = rec_f_shiensochi.upd_tammatsu
                     , del_flg = rec_f_shiensochi.del_flg
                  WHERE 
                     kojin_no  = lc_kojin_no 
                     AND kaishi_ymd = ln_kaishi_ymd;

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

         IF (rec_main.del_flg = '0' AND rec_main.sochi_kbn = '3') OR rec_main.del_flg = '1' THEN
            
            OPEN cur_busho;
               LOOP
                  FETCH cur_busho INTO rec_busho;
                  EXIT WHEN NOT FOUND;
                  
                     INSERT INTO f_kiji(
                        busho_cd
                        , seq_no_kiji
                        , kojin_no
                        , kodo_yotei_kbn
                        , kiji_ymd
                        , kiji_time
                        , kiji_bunrui_cd
                        , kiji_bunrui
                        , kiji_naiyo_cd
                        , kiji_naiyo
                        , kosho_hoho_cd
                        , kosho_hoho
                        , tainoseiri_kiroku_cd
                        , tainoseiri_kiroku
                        , midashi
                        , kiji_biko
                        , sessho_flg
                        , sessho_aite_cd
                        , sessho_aite
                        , sessho_basho_cd
                        , sessho_basho
                        , tantosha_cd
                        , tantosha
                        , busho
                        , nofu_yotei_ymd
                        , nofu_yotei_kingaku
                        , sashiosae_yotei_ymd
                        , kyocho_hyoji_flg
                        , ins_datetime
                        , upd_datetime
                        , upd_tantosha_cd
                        , upd_tammatsu
                        , del_flg
                     ) VALUES (
                        rec_busho.busho_cd
                        , nextval('dlgmain.seq_no_kiji')
                        , rec_main.atena_no
                        , 1
                        , getdatetonum(current_date)
                        , 0
                        , 2
                        , '事務処理用'
                        , NULL
                        , NULL
                        , NULL
                        , NULL
                        , 999
                        , 'その他'
                        , '支援措置終了'
                        , '支援措置終了情報が連携されました。
                     　必要に応じて注意喚起情報を更新してください。'
                        , 0
                        , 0
                        , NULL
                        , 0
                        , NULL
                        , NULL
                        , NULL
                        , rec_busho.busho
                        , 0
                        , 0
                        , 0
                        , 1
                        , current_timestamp
                        , current_timestamp
                        , rec_main.sosasha_cd
                        , 'SERVER'
                        , 0
                     );
               END LOOP;
            CLOSE cur_busho;
         END IF;

         -- 中間テーブルの「削除フラグ」が「1」のデータは「3：削除」を指定
         IF get_str_to_num(rec_main.del_flg) = 1 THEN
            ln_del_count := ln_del_count + 1;
            ln_result_cd := ln_result_cd_del;
         END IF;

         BEGIN
            -- 中間テーブル更新
            UPDATE dlgrenkei.i_r4g_shiensochi
            SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
               , seq_no_renkei = in_n_renkei_seq
               , shori_ymd = in_n_shori_ymd
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND atena_no = rec_main.atena_no
               AND rireki_no = rec_main.rireki_no
               AND shiensochi_kaishi_ymd = rec_main.shiensochi_kaishi_ymd;
         EXCEPTION
            WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
               lc_err_cd := lc_err_cd_err;
               ln_result_cd := ln_result_cd_err;
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
