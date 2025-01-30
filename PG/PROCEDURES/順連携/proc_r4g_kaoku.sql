--------------------------------------------------------
--  DDL for Procedure proc_r4g_kaoku
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_kaoku ( 
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 家屋基本情報                                                                                              */
/* 引数 IN  : in_n_renkei_data_cd  … 連携データコード                                                                   */
/*            in_n_renkei_seq       … 連携SEQ（処理単位で符番されるSEQ）                                                */
/*            in_n_shori_ymd        … 処理日 （処理単位で設定される処理日）                                              */
/*      OUT : io_c_err_code         …例外エラー発生時のエラーコード                                                      */
/*            io_c_err_text         … 例外エラー発生時のエラー内容                                                       */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/24  CRESS-INFO.Drexler     新規作成     012o005「家屋基本情報」の取込を行う                         */
/**********************************************************************************************************************/

DECLARE


   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;
   rec_f_kaokukihon_renkei        f_kaokukihon_renkei%ROWTYPE;
   
   ln_para01                      numeric DEFAULT 0;
   ln_para02                      numeric DEFAULT 0;

   ln_shori_count                 numeric DEFAULT 0;             -- 処理件数用変数
   ln_ins_count                   numeric DEFAULT 0;             -- 追加件数用変数
   ln_upd_count                   numeric DEFAULT 0;             -- 更新件数用変数
   ln_del_count                   numeric DEFAULT 0;             -- 削除件数用変数
   ln_err_count                   numeric DEFAULT 0;             -- エラー件数用変数
   lc_err_cd                      character varying;             -- エラーコード用変数
   lc_err_text                    character varying(100):='';    -- エラー内容用変数
   ln_result_cd                   numeric DEFAULT 0;             -- 結果区分更新用変数

   lc_sql                        character varying;             -- SQL文用変数

   ln_result_cd_add               numeric DEFAULT 1;             -- 追加フラグ
   ln_result_cd_upd               numeric DEFAULT 2;             -- 更新フラグ
   ln_result_cd_del               numeric DEFAULT 3;             -- 削除フラグ
   ln_result_cd_warning           numeric DEFAULT 7;             -- 警告フラグ
   ln_result_cd_err               numeric DEFAULT 9;             -- エラーフラグ

   lc_err_cd_normal               character varying = '0';       -- 通常フラグ
   lc_err_cd_err                  character varying = '9';       -- エラーフラグ

   -- メインカーソル
   cur_main CURSOR FOR
   SELECT *
   FROM dlgrenkei.i_r4g_kaoku
   WHERE saishin_flg = '1'
   AND result_cd < 8 ;

   rec_main                           dlgrenkei.i_r4g_kaoku%ROWTYPE;

   -- パラメータ取得カーソル
   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                      dlgrenkei.f_renkei_parameter%ROWTYPE;

   -- 行ロック用カーソル
   cur_lock CURSOR FOR
   SELECT *
   FROM f_kaokukihon_renkei
   WHERE 
   bukken_no = rec_main.bukken_no
   AND kazei_nendo = rec_main.kazei_nendo::numeric
   AND kaoku_kihon_rireki_no = rec_main.kaoku_kihon_rireki_no::numeric;

   rec_lock                           f_kaokukihon_renkei%ROWTYPE;

BEGIN
   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   --パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;

      END LOOP;
   CLOSE cur_parameter;

   --連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         lc_sql := 'TRUNCATE TABLE dlgmain.f_kaokukihon_renkei';
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

            ln_shori_count := ln_shori_count + 1;

            -- 物件番号
            rec_f_kaokukihon_renkei.bukken_no := rec_main.bukken_no;
            -- 課税年度
            rec_f_kaokukihon_renkei.kazei_nendo := rec_main.kazei_nendo::numeric;
            -- 家屋基本_履歴番号
            rec_f_kaokukihon_renkei.kaoku_kihon_rireki_no := kaoku_kihon_rireki_no::numeric;
            -- 家屋_登記所在地
            rec_f_kaokukihon_renkei.kaoku_toki_jusho := get_trimmed_space(rec_main.kaoku_toki_jusho);
            -- 家屋番号
            rec_f_kaokukihon_renkei.kaoku_no := get_trimmed_space(rec_main.kaoku_no);
            -- 登記種類区分
            rec_f_kaokukihon_renkei.toki_shurui_kbn := rec_main.toki_shurui_kbn;
            -- 床面積
            rec_f_kaokukihon_renkei.yuka_menseki := rec_main.yuka_menseki::numeric;
            -- 登記建築年月日
            rec_f_kaokukihon_renkei.toki_kenchiku_ymd := get_trimmed_date(rec_main.toki_kenchiku_ymd);
            -- 登記建築年月日_不詳表記
            rec_f_kaokukihon_renkei.toki_kenchiku_ymd_fusho := rec_main.toki_kenchiku_ymd_fusho;
            -- 義務者_宛名番号
            rec_f_kaokukihon_renkei.gimusha_atena_no := rec_main.gimusha_atena_no;
            -- 現況建築年月日
            rec_f_kaokukihon_renkei.genkyo_kenchiku_ymd :=get_trimmed_date(rec_main.genkyo_kenchiku_ymd);
            -- 現況種類区分
            rec_f_kaokukihon_renkei.genkyo_syurui_kbn := rec_main.genkyo_syurui_kbn;
            -- 主たる用途区分
            rec_f_kaokukihon_renkei.yoto_kbn := rec_main.yoto_kbn;
            -- 現況用途区分2
            rec_f_kaokukihon_renkei.genkyo_yoto_kbn2 := rec_main.genkyo_yoto_kbn2;
            -- 現況用途区分3
            rec_f_kaokukihon_renkei.genkyo_yoto_kbn3 := rec_main.genkyo_yoto_kbn3;
            -- 合計現況床面積
            rec_f_kaokukihon_renkei.gokei_genkyo_yuka_menseki := rec_main.gokei_genkyo_yuka_menseki::numeric;
            -- 家屋_現況所在地
            rec_f_kaokukihon_renkei.kaoku_genkyo_jusho := get_trimmed_space(rec_main.kaoku_genkyo_jusho);
            -- 分棟・合棟原因区分
            rec_f_kaokukihon_renkei.genin_kbn := rec_main.genin_kbn::numeric;
            -- 分棟元・合棟先_物件番号
            rec_f_kaokukihon_renkei.to_bukken_no := rec_main.buntomoto_bukken_no;
            -- 分棟元・合棟先_履歴番号
            rec_f_kaokukihon_renkei.to_bukken_rireki_no := rec_main.buntomoto_bukken_rireki_no;
            -- データ作成日時
            rec_f_kaokukihon_renkei.ins_datetime := CONCAT(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            -- データ更新日時
            rec_f_kaokukihon_renkei.upd_datetime := CONCAT(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            -- 更新担当者コード
            rec_f_kaokukihon_renkei.upd_tantosha_cd := rec_main.sosasha_cd;
            -- 更新端末名称
            rec_f_kaokukihon_renkei.upd_tammatsu := 'SERVER';
            -- 削除フラグ
            rec_f_kaokukihon_renkei.del_flg := rec_main.del_flg::numeric;

            OPEN cur_lock;
               FETCH cur_lock INTO rec_lock;
            CLOSE cur_lock;

            IF rec_lock IS NULL THEN
               BEGIN
                  -- 登録処理
                  INSERT INTO f_kaokukihon_renkei(
                     bukken_no
                     ,kazei_nendo
                     ,kaoku_kihon_rireki_no
                     ,kaoku_toki_jusho
                     ,kaoku_no
                     ,toki_shurui_kbn
                     ,yuka_menseki
                     ,toki_kenchiku_ymd
                     ,toki_kenchiku_ymd_fusho
                     ,gimusha_atena_no
                     ,genkyo_kenchiku_ymd
                     ,genkyo_syurui_kbn
                     ,yoto_kbn
                     ,genkyo_yoto_kbn2
                     ,genkyo_yoto_kbn3
                     ,gokei_genkyo_yuka_menseki
                     ,kaoku_genkyo_jusho
                     ,genin_kbn
                     ,to_bukken_no
                     ,to_bukken_rireki_no
                     ,ins_datetime
                     ,upd_datetime
                     ,upd_tantosha_cd
                     ,upd_tammatsu
                     ,del_flg
                  )
                  VALUES (
                     rec_f_kaokukihon_renkei.bukken_no
                     ,rec_f_kaokukihon_renkei.kazei_nendo
                     ,rec_f_kaokukihon_renkei.kaoku_kihon_rireki_no
                     ,rec_f_kaokukihon_renkei.kaoku_toki_jusho
                     ,rec_f_kaokukihon_renkei.kaoku_no
                     ,rec_f_kaokukihon_renkei.toki_shurui_kbn
                     ,rec_f_kaokukihon_renkei.yuka_menseki
                     ,rec_f_kaokukihon_renkei.toki_kenchiku_ymd
                     ,rec_f_kaokukihon_renkei.toki_kenchiku_ymd_fusho
                     ,rec_f_kaokukihon_renkei.gimusha_atena_no
                     ,rec_f_kaokukihon_renkei.genkyo_kenchiku_ymd
                     ,rec_f_kaokukihon_renkei.genkyo_syurui_kbn
                     ,rec_f_kaokukihon_renkei.yoto_kbn
                     ,rec_f_kaokukihon_renkei.genkyo_yoto_kbn2
                     ,rec_f_kaokukihon_renkei.genkyo_yoto_kbn3
                     ,rec_f_kaokukihon_renkei.gokei_genkyo_yuka_menseki
                     ,rec_f_kaokukihon_renkei.kaoku_genkyo_jusho
                     ,rec_f_kaokukihon_renkei.genin_kbn
                     ,rec_f_kaokukihon_renkei.to_bukken_no
                     ,rec_f_kaokukihon_renkei.to_bukken_rireki_no
                     ,rec_f_kaokukihon_renkei.ins_datetime
                     ,rec_f_kaokukihon_renkei.upd_datetime
                     ,rec_f_kaokukihon_renkei.upd_tantosha_cd
                     ,rec_f_kaokukihon_renkei.upd_tammatsu
                     ,rec_f_kaokukihon_renkei.del_flg
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
                  UPDATE f_kaokukihon_renkei
                     SET  kaoku_toki_jusho = rec_f_kaokukihon_renkei.kaoku_toki_jusho
                     ,kaoku_no = rec_f_kaokukihon_renkei.kaoku_no
                     ,toki_shurui_kbn = rec_f_kaokukihon_renkei.toki_shurui_kbn
                     ,yuka_menseki = rec_f_kaokukihon_renkei.yuka_menseki
                     ,toki_kenchiku_ymd = rec_f_kaokukihon_renkei.toki_kenchiku_ymd
                     ,toki_kenchiku_ymd_fusho = rec_f_kaokukihon_renkei.toki_kenchiku_ymd_fusho
                     ,gimusha_atena_no = rec_f_kaokukihon_renkei.gimusha_atena_no
                     ,genkyo_kenchiku_ymd = rec_f_kaokukihon_renkei.genkyo_kenchiku_ymd
                     ,genkyo_syurui_kbn = rec_f_kaokukihon_renkei.genkyo_syurui_kbn
                     ,yoto_kbn = rec_f_kaokukihon_renkei.yoto_kbn
                     ,genkyo_yoto_kbn2 = rec_f_kaokukihon_renkei.genkyo_yoto_kbn2
                     ,genkyo_yoto_kbn3 = rec_f_kaokukihon_renkei.genkyo_yoto_kbn3
                     ,gokei_genkyo_yuka_menseki = rec_f_kaokukihon_renkei.gokei_genkyo_yuka_menseki
                     ,kaoku_genkyo_jusho = rec_f_kaokukihon_renkei.kaoku_genkyo_jusho
                     ,genin_kbn = rec_f_kaokukihon_renkei.genin_kbn
                     ,to_bukken_no = rec_f_kaokukihon_renkei.to_bukken_no
                     ,to_bukken_rireki_no = rec_f_kaokukihon_renkei.to_bukken_rireki_no
                     ,upd_datetime = rec_f_kaokukihon_renkei.upd_datetime
                     ,upd_tantosha_cd = rec_f_kaokukihon_renkei.upd_tantosha_cd
                     ,upd_tammatsu = rec_f_kaokukihon_renkei.upd_tammatsu
                     ,del_flg = rec_f_kaokukihon_renkei.del_flg
                  WHERE 
                     bukken_no = rec_f_kaokukihon_renkei.bukken_no
                     AND kaoku_kihon_rireki_no = rec_f_kaokukihon_renkei.kaoku_kihon_rireki_no
                     AND kazei_nendo = rec_f_kaokukihon_renkei.kazei_nendo;

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
               UPDATE dlgrenkei.i_r4g_kaoku
                  SET result_cd = ln_result_cd
                  , error_cd = lc_err_cd
                  , error_text = lc_err_text
                  , seq_no_renkei = in_n_renkei_seq
                  , shori_ymd     = in_n_shori_ymd
               WHERE shikuchoson_cd = rec_main.shikuchoson_cd
                  AND bukken_no = rec_main.bukken_no
                  AND kazei_nendo = rec_main.kazei_nendo
                  AND kaoku_kihon_rireki_no = rec_main.kaoku_kihon_rireki_no;
            EXCEPTION
               WHEN OTHERS THEN NULL;
            END;
      END LOOP;
   CLOSE cur_main;

   -- dlgrenkeiプロシージャ：proc_r4g_kaoku_shokaiを実行する
   CALL dlgrenkei.proc_r4g_kaoku_shokai(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

   -- dlgrenkeiプロシージャ：proc_r4g_kaisoを実行する
   CALL dlgrenkei.proc_r4g_kaiso(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

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
