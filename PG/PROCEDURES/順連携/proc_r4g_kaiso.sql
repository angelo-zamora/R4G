--------------------------------------------------------
--  DDL for Procedure proc_r4g_kaiso
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_kaiso (
   in_n_renkei_data_cd IN numeric,
   in_n_renkei_seq IN numeric,
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : f_照会_不動産_階層（f_shokai_fudosan_kaiso）の追加／更新／削除を実施する                                */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code       …例外エラー発生時のエラーコード                                                    */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :  CRESS-INFO.Angelo     新規作成     012o005「家屋基本情報」の取込を行う                                 */
/**********************************************************************************************************************/

DECLARE
   rec_f_shokai_fudosan_kaiso          f_shokai_fudosan_kaiso%ROWTYPE;
   ln_para01                           numeric DEFAULT 0;
   ln_para02                           numeric DEFAULT 0;

   ln_shori_count                      numeric;
   ln_ins_count                        numeric;
   ln_upd_count                        numeric;
   ln_del_count                        numeric;
   ln_err_count                        numeric;
   lc_err_text                         character varying(100);
   ln_result_cd                        numeric DEFAULT 0;
   lc_err_cd                           character varying;
   lc_sql                              character varying(1000);

   ln_result_cd_add                    numeric DEFAULT 1; -- 追加
   ln_result_cd_upd                    numeric DEFAULT 2; -- 更新
   ln_result_cd_err                    numeric DEFAULT 9; -- エラー

   lc_err_cd_normal                    character varying = '0'; -- 通常
   lc_err_cd_err                       character varying = '9'; -- エラー

   rec_log                             dlgrenkei.f_renkei_log%ROWTYPE;

   cur_main CURSOR FOR
   SELECT
    *
   FROM
      dlgrenkei.i_r4g_kaoku
   WHERE
      saishin_flg = '1'
      AND kazei_nendo = (
         SELECT
               MAX(tax_year)
         FROM
               dlgrenkei.i_r4g_kaoku
      )
      AND kaoku_kihon_rireki_no = (
         SELECT
               MAX(kaoku_kihon_rireki_no)
         FROM
               dlgrenkei.i_r4g_kaoku
      )
      AND (
         yuka_menseki > 0
         OR gokei_genkyo_yuka_menseki > 0
      )
      AND result_cd < 8;

   rec_main                          dlgrenkei.i_r4g_kaoku%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                     dlgrenkei.f_renkei_parameter%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM f_shokai_fudosan_kaiso
   WHERE 
      kojin_no = rec_main.kojin_no
      AND seq_no_shokai = rec_main.seq_no_shokai
      AND kaiso_cd = rec_main.kaiso_cd;

   rec_lock                       f_shokai_fudosan_kaiso%ROWTYPE;

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
         SELECT COUNT(*) INTO ln_del_count FROM f_shokai_fudosan_kaiso;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_shokai_fudosan_kaiso';
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

            OPEN cur_lock;
               FETCH cur_lock INTO rec_lock;
            CLOSE cur_lock;

            -- 個人番号
            rec_f_shokai_fudosan_kaiso.kojin_no := rec_main.gimusha_atena_no;
            -- 照会SEQ
               rec_f_shokai_fudosan_kaiso.seq_no_shokai := rec_lock.seq_no_shokai;
            -- 物件番号
            rec_f_shokai_fudosan_kaiso.bukken_no := rec_main.bukken_no;
            -- 物件番号
            rec_f_shokai_fudosan_kaiso.kaiso_cd := 11;
            -- 階層コード
            rec_f_shokai_fudosan_kaiso.kaiso := get_kaiso(rec_main.kaiso_cd);
            -- 物件種類コード
            rec_f_shokai_fudosan_kaiso.bukken_shurui_cd := 2;
            -- 物件種類コード
            rec_f_shokai_fudosan_kaiso.yuka_menseki := CASE WHEN rec_main.yuka_menseki::numeric = 0 THEN rec_main.gokei_genkyo_yuka_menseki ELSE rec_main.yuka_menseki END;
            -- 床面積
            rec_f_shokai_fudosan_kaiso.fudosan_no := null;
            -- データ作成日時
            rec_f_shokai_fudosan_kaiso.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            -- データ更新日時
            rec_f_shokai_fudosan_kaiso.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            -- 更新担当者コード
            rec_f_shokai_fudosan_kaiso.upd_tantosha_cd := rec_main.sosasha_cd;
            -- 更新端末名称
            rec_f_shokai_fudosan_kaiso.upd_tammatsu := 'SERVER';
            -- 削除フラグ
            rec_f_shokai_fudosan_kaiso.del_flg := rec_main.del_flg::numeric;

            IF rec_lock IS NULL THEN
               BEGIN
                  -- 登録処理
                  INSERT INTO f_shokai_fudosan_kaiso(
                     kojin_no
                     , seq_no_shokai
                     , sbukken_no
                     , skaiso_cd
                     , skaiso
                     , sbukken_shurui_cd
                     , syuka_menseki
                     , sfudosan_no
                     , sins_datetime
                     , supd_datetime
                     , supd_tantosha_cd
                     , supd_tammatsu
                     , sdel_flg
                  )
                  VALUES (
                     rec_f_shokai_fudosan_kaiso.kojin_no
                     , rec_f_shokai_fudosan_kaiso.seq_no_shokai
                     , rec_f_shokai_fudosan_kaiso.bukken_no
                     , rec_f_shokai_fudosan_kaiso.kaiso_cd
                     , rec_f_shokai_fudosan_kaiso.kaiso
                     , rec_f_shokai_fudosan_kaiso.bukken_shurui_cd
                     , rec_f_shokai_fudosan_kaiso.yuka_menseki
                     , rec_f_shokai_fudosan_kaiso.fudosan_no
                     , rec_f_shokai_fudosan_kaiso.ins_datetime
                     , rec_f_shokai_fudosan_kaiso.upd_datetime
                     , rec_f_shokai_fudosan_kaiso.upd_tantosha_cd
                     , rec_f_shokai_fudosan_kaiso.upd_tammatsu
                     , rec_f_shokai_fudosan_kaiso.del_flg
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
                  UPDATE f_shokai_fudosan_kaiso
                     SET kaiso = rec_f_shokai_fudosan_kaiso.kaiso
                       , yuka_menseki = rec_f_shokai_fudosan_kaiso.yuka_menseki
                       , upd_datetime = concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                       , upd_tantosha_cd = rec_main.sosasha_cd
                       , upd_tammatsu = 'SERVER'
                       , del_flg = rec_main.del_flg
                  WHERE
                     kojin_no = rec_main.kojin_no
                  AND seq_no_shokai = rec_main.seq_no_shokai
                  AND kaiso_cd = rec_main.kaiso_cd;

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
            IF rec_main.del_flg::numeric = 1 THEN
               ln_result_cd = ln_result_cd_del;
            END IF;

            UPDATE dlgrenkei.i_r4g_kaoku
               SET result_cd     = ln_result_cd
               , error_cd      = ln_err_cd
               , error_text    = lc_err_text
               , seq_no_renkei = in_n_renkei_seq
               , shori_ymd     = in_n_shori_ymd
               WHERE shikuchoson_cd = rec_main.shikuchoson_cd
                  AND bukken_no = rec_main.bukken_no
                  AND kazei_nendo = rec_main.kazei_nendo
                  AND kaoku_kihon_rireki_no = rec_main.kaoku_kihon_rireki_no;

      END LOOP;
   CLOSE cur_main;

EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;