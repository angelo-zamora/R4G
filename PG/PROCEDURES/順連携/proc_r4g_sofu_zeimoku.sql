--------------------------------------------------------
--  DDL for Procedure proc_r4g_sofu_zeimoku
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_sofu_zeimoku(
   in_n_renkei_data_cd IN numeric,
   in_n_renkei_seq IN numeric,
   in_n_shori_ymd IN numeric,
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 送付先・連絡先情報（統合収滞納）                                                                        */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                     */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                      */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : 2025/02/03 CRESS-INFO.Angelo     新規作成     036o014「送付先・連絡先情報（統合収滞納）」の取込を行う   */
/**********************************************************************************************************************/

DECLARE
   rec_f_sofu_zeimoku             f_sofu_zeimoku%ROWTYPE;
   ln_para01                      numeric DEFAULT 0;
   ln_para06                      numeric DEFAULT 0;
   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;
   ln_rec_count                   numeric DEFAULT 0;
   lc_sql                         character varying;

   ln_result_cd_add               numeric DEFAULT 1; -- 追加
   ln_result_cd_upd               numeric DEFAULT 2; -- 更新
   ln_result_cd_err               numeric DEFAULT 9; -- エラー
   ln_yusen_flg                   numeric DEFAULT 0;

   lc_err_cd_normal               character varying = '0'; -- 通常
   lc_err_cd_err                  character varying = '9'; -- エラー
   
   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;

   cur_main CURSOR FOR
   WITH filtered AS (
      SELECT
         *,
         MAX(sofu_rireki_no) OVER (
               PARTITION BY shikuchoson_cd,
               atena_no,
               gyomu_id,
               zeimoku_cd,
               keiji_kanri_no
         ) AS max_sofu_rireki_no
      FROM
         dlgrenkei.i_r4g_sofu_renrakusaki
   )
   SELECT
      *
   FROM
      filtered
   WHERE
      saishin_flg = '1'
      AND zeimoku_cd <> '00'
      AND (
         COALESCE(yubin_no, '') <> ''
         OR COALESCE(jusho, '') <> ''
      )
      AND sofu_rireki_no = max_sofu_rireki_no
      AND result_cd < '8'
      AND NOT (
         COALESCE(yubin_no, '') = ''
         AND COALESCE(jusho, '') = ''
      );

   rec_main                      dlgrenkei.i_r4g_sofu_renrakusaki%ROWTYPE;
    
   cur_lock CURSOR FOR
   SELECT
      *
   FROM
      f_sofu_zeimoku
   WHERE
      kojin_no = rec_f_sofu_zeimoku.kojin_no
      AND gyomu_cd = rec_f_sofu_zeimoku.gyomu_cd
      AND zeimoku_cd = ln_zeimoku_cd
      AND keiji_kanri_no = rec_f_sofu_zeimoku.keiji_kanri_no;
    
   rec_lock                      f_sofu_zeimoku%ROWTYPE;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;
   
   OPEN cur_parameter;
   LOOP
      FETCH cur_parameter INTO rec_parameter;
      EXIT WHEN NOT FOUND;

      IF rec_parameter.parameter_no = 1 THEN
         ln_para01 := rec_parameter.parameter_value;
      END IF;
      IF rec_parameter.parameter_no = 6 THEN 
         ln_para06 := rec_parameter.parameter_value;
      END IF;
   END LOOP;
   CLOSE cur_parameter;

   IF ln_para01 = 1 THEN
      BEGIN
         lc_sql := 'TRUNCATE TABLE dlgmain.f_sofu_zeimoku';
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
		 
         ln_shori_count                 := ln_shori_count + 1;
         lc_err_cd                      := ln_err_cd_normal;
         ln_result_cd                   := 0;
         lc_err_text                    := NULL;
         rec_f_sofu_zeimoku             := NULL;

         -- 個人番号
         rec_f_sofu_zeimoku.kojin_no := rec_main.atena_no;
         -- 業務コード
         rec_f_sofu_zeimoku.gyomu_cd := get_str_to_num(rec_main.gyomu_id);
         -- 税目コード
         rec_f_sofu_zeimoku.zeimoku_cd := get_str_to_num(get_r4g_code_conv(0, 3, rec_main.zeimoku_cd, NULL));
         -- 軽自管理番号
         rec_f_sofu_zeimoku.keiji_kanri_no := get_str_to_num(rec_main.keiji_kanri_no);

         IF ln_para06 = 1 THEN
            ln_yusen_flg := 1;
         ELSE
            BEGIN
               SELECT yusen_flg
               INTO ln_yusen_flg
               FROM f_sofu
               WHERE kojin_no = rec_f_sofu_zeimoku.kojin_no
                  AND zeimoku_cd = rec_f_sofu_zeimoku.zeimoku_cd 
                  AND keiji_kanri_no = rec_f_sofu_zeimoku.keiji_kanri_no
                  AND del_flg = 0;
            EXCEPTION
               WHEN OTHERS THEN
               ln_yusen_flg := 0;
            END;
         END IF;

         -- 優先フラグ
         rec_f_sofu_zeimoku.yusen_flg := ln_yusen_flg;
         -- 送付先氏名カナ
         rec_f_sofu_zeimoku.sofu_shimei_kana := get_trimmed_space(rec_main.simei_meisho_katakana);
         -- 送付先氏名
         rec_f_sofu_zeimoku.sofu_shimei := get_trimmed_space(rec_main.simei_meisho);
         -- 送付先郵便番号
         rec_f_sofu_zeimoku.sofu_yubin_no := rec_main.yubin_no;
         -- 住所
         rec_f_sofu_zeimoku.sofu_jusho := CASE WHEN (rec_main.jusho = '' OR rec_main.jusho IS NULL)
                                                THEN CONCAT(get_trimmed_space(rec_main.ken), get_trimmed_space(rec_main.shikuchoson), get_trimmed_space(rec_main.jusho_machi_cd), get_trimmed_space(rec_main.banchi))
                                                ELSE get_trimmed_space(rec_main.jusho)
                                                END;
         -- 住所方書
         rec_f_sofu_zeimoku.sofu_jusho_katagaki := get_trimmed_space(rec_main.jusho_katagaki);
         -- 送付先入力区分
         rec_f_sofu_zeimoku.sofu_nyuryoku_kbn := CASE WHEN (rec_main.jusho = '' OR rec_main.jusho IS NULL)
                                                   THEN 0
                                                   ELSE 4
                                                   END;
         -- 送付先市区町村コード
         rec_f_sofu_zeimoku.sofu_shikuchoson_cd := rec_main.jusho_shikuchoson_cd;
         -- 送付先町字コード
         rec_f_sofu_zeimoku.sofu_machiaza_cd := rec_main.jusho_machi_cd;
         -- 送付先都道府県
         rec_f_sofu_zeimoku.sofu_todofuken := get_trimmed_space(rec_main.ken);
         -- 送付先市区郡町村名
         rec_f_sofu_zeimoku.sofu_shikugunchoson := get_trimmed_space(rec_main.shikuchoson);
         -- 送付先町字
         rec_f_sofu_zeimoku.sofu_machiaza := get_trimmed_space(rec_main.machi);
         -- 送付先番地号表記
         rec_f_sofu_zeimoku.sofu_banchigohyoki := get_trimmed_space(rec_main.banchi);
         -- 送付先国名コード
         rec_f_sofu_zeimoku.sofu_kokumei_cd := NULL;
         -- 送付先国名等
         rec_f_sofu_zeimoku.sofu_kokumeito := NULL;
         -- 送付先国外住所
         rec_f_sofu_zeimoku.sofu_kokugai_jusho := NULL;
         -- 送付先区分
         rec_f_sofu_zeimoku.sofu_kbn := rec_main.sofu_kbn;
         -- 送付先を設定する理由
         rec_f_sofu_zeimoku.sofu_setti_riyu := rec_main.sofu_setti_riyu;
         -- 連絡先区分
         rec_f_sofu_zeimoku.renrakusaki_kbn := rec_main.renrakusaki_kbn;
         -- 電話番号
         rec_f_sofu_zeimoku.denwa_no := rec_main.tel_no;
         -- 有効期限（開始年月日）
         rec_f_sofu_zeimoku.yukokigen_kaishi_ymd := get_ymd_str_to_num(rec_main.toroku_ymd);
         -- 有効期限（終了年月日）
         rec_f_sofu_zeimoku.yukokigen_shuryo_ymd := get_ymd_str_to_num(rec_main.riyou_haishi_ymd);
         --連携フラグ
         rec_f_sofu_zeimoku.renkei_flg := 1;
         -- 送付先履歴番号
         rec_f_sofu_zeimoku.sofurireki_no := rec_main.sofu_rireki_no;
         -- データ作成日時
         rec_f_sofu_zeimoku.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時
         rec_f_sofu_zeimoku.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_f_sofu_zeimoku.upd_tantosha_cd := rec_main.sosasha_cd;
         -- 更新端末名称
         rec_f_sofu_zeimoku.upd_tammatsu := 'SERVER';
         -- 削除フラグ
         rec_f_sofu_zeimoku.del_flg := get_str_to_num(rec_main.del_flg);
         
         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         BEGIN
            SELECT
               COUNT(*) INTO ln_rec_count
            FROM
               f_sofu_zeimoku
            WHERE
               kojin_no = rec_f_sofu_zeimoku.kojin_no
               AND zeimoku_cd = rec_f_sofu_zeimoku.zeimoku_cd
               AND (
                  sofu_shimei <> rec_f_sofu_zeimoku.sofu_shimei
                  OR sofi_yubin_no <> rec_f_sofu_zeimoku.sofi_yubin_no
                  OR sofu_jusho <> rec_f_sofu_zeimoku.sofu_jusho
                  OR sofu_jusho_katagaki <> rec_f_sofu_zeimoku.sofu_jusho_katagaki
               );
         EXCEPTION
            WHEN OTHERS THEN NULL;
         END;

         IF ln_rec_count = 0 THEN
            BEGIN
               INSERT INTO f_sofu_zeimoku(
                     kojin_no
                  , gyomu_cd
                  , zeimoku_cd
                  , keiji_kanri_no
                  , yusen_flg
                  , sofu_shimei_kana
                  , sofu_shimei
                  , sofu_yubin_no
                  , sofu_jusho
                  , sofu_jusho_katagaki
                  , sofu_nyuryoku_kbn
                  , sofu_shikuchoson_cd
                  , sofu_machiaza_cd
                  , sofu_todofuken
                  , sofu_shikugunchoson
                  , sofu_machiaza
                  , sofu_banchigohyoki
                  , sofu_kokumei_cd
                  , sofu_kokumeito
                  , sofu_kokugai_jusho
                  , sofu_kbn
                  , sofu_setti_riyu
                  , renrakusaki_kbn
                  , denwa_no
                  , yukokigen_kaishi_ymd
                  , yukokigen_shuryo_ymd
                  , renkei_flg
                  , sofurireki_no
                  , ins_datetime
                  , upd_datetime
                  , upd_tantosha_cd
                  , upd_tammatsu
                  , del_flg
               ) VALUES (
                     rec_f_zeimoku.kojin_no
                  , rec_f_zeimoku.gyomu_cd
                  , rec_f_zeimoku.zeimoku_cd
                  , rec_f_zeimoku.keiji_kanri_no
                  , rec_f_zeimoku.yusen_flg
                  , rec_f_zeimoku.sofu_shimei_kana
                  , rec_f_zeimoku.sofu_shimei
                  , rec_f_zeimoku.sofu_yubin_no
                  , rec_f_zeimoku.sofu_jusho
                  , rec_f_zeimoku.sofu_jusho_katagaki
                  , rec_f_zeimoku.sofu_nyuryoku_kbn
                  , rec_f_zeimoku.sofu_shikuchoson_cd
                  , rec_f_zeimoku.sofu_machiaza_cd
                  , rec_f_zeimoku.sofu_todofuken
                  , rec_f_zeimoku.sofu_shikugunchoson
                  , rec_f_zeimoku.sofu_machiaza
                  , rec_f_zeimoku.sofu_banchigohyoki
                  , rec_f_zeimoku.sofu_kokumei_cd
                  , rec_f_zeimoku.sofu_kokumeito
                  , rec_f_zeimoku.sofu_kokugai_jusho
                  , rec_f_zeimoku.sofu_kbn
                  , rec_f_zeimoku.sofu_setti_riyu
                  , rec_f_zeimoku.renrakusaki_kbn
                  , rec_f_zeimoku.denwa_no
                  , rec_f_zeimoku.yukokigen_kaishi_ymd
                  , rec_f_zeimoku.yukokigen_shuryo_ymd
                  , rec_f_zeimoku.renkei_flg
                  , rec_f_zeimoku.sofurireki_no
                  , rec_f_zeimoku.ins_datetime
                  , rec_f_zeimoku.upd_datetime
                  , rec_f_zeimoku.upd_tantosha_cd
                  , rec_f_zeimoku.upd_tammatsu
                  , rec_f_zeimoku.del_flg
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
               UPDATE f_sofu_zeimoku
               SET yusen_flg = rec_f_sofu_zeimoku.yusen_flg
                  , sofu_shimei_kana = rec_f_sofu_zeimoku.sofu_shimei_kana
                  , sofu_shimei = rec_f_sofu_zeimoku.sofu_shimei
                  , sofu_yubin_no = rec_f_sofu_zeimoku.sofu_yubin_no
                  , sofu_jusho = rec_f_sofu_zeimoku.sofu_jusho
                  , sofu_jusho_katagaki = rec_f_sofu_zeimoku.sofu_jusho_katagaki
                  , sofu_nyuryoku_kbn = rec_f_sofu_zeimoku.sofu_nyuryoku_kbn
                  , sofu_shikuchoson_cd = rec_f_sofu_zeimoku.sofu_shikuchoson_cd
                  , sofu_machiaza_cd = rec_f_sofu_zeimoku.sofu_machiaza_cd
                  , sofu_todofuken = rec_f_sofu_zeimoku.sofu_todofuken
                  , sofu_shikugunchoson = rec_f_sofu_zeimoku.sofu_shikugunchoson
                  , sofu_machiaza = rec_f_sofu_zeimoku.sofu_machiaza
                  , sofu_banchigohyoki = rec_f_sofu_zeimoku.sofu_banchigohyoki
                  , sofu_kbn = rec_f_sofu_zeimoku.sofu_kbn
                  , sofu_setti_riyu = rec_f_sofu_zeimoku.sofu_setti_riyu
                  , renrakusaki_kbn = rec_f_sofu_zeimoku.renrakusaki_kbn
                  , denwa_no = rec_f_sofu_zeimoku.denwa_no
                  , yukokigen_kaishi_ymd = rec_f_sofu_zeimoku.yukokigen_kaishi_ymd
                  , yukokigen_shuryo_ymd = rec_f_sofu_zeimoku.yukokigen_shuryo_ymd
                  , sofurireki_no = rec_f_sofu_zeimoku.sofurireki_no
                  , upd_datetime = rec_f_sofu_zeimoku.upd_datetime
                  , upd_tantosha_cd = rec_f_sofu_zeimoku.upd_tantosha_cd
                  , upd_tammatsu = rec_f_sofu_zeimoku.upd_tammatsu
                  , del_flg = rec_f_sofu_zeimoku.del_flg
               WHERE
                  kojin_no = rec_f_sofu_zeimoku.kojin_no
                  AND gyomu_cd = rec_f_sofu_zeimoku.gyomu_cd
                  AND zeimoku_cd = rec_f_sofu_zeimoku.zeimoku_cd 
                  AND keiji_kanri_no = rec_f_sofu_zeimoku.keiji_kanri_no;

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
      END LOOP;
   CLOSE cur_main;
      
   rec_log.seq_no_renkei := in_n_renkei_seq;
   rec_log.proc_shuryo_datetime := CURRENT_TIMESTAMP;
   rec_log.proc_shori_count := ln_shori_count;
   rec_log.proc_ins_count := ln_ins_count;
   rec_log.proc_upd_count := ln_upd_count;
   rec_log.proc_del_count := ln_del_count;
   rec_log.proc_err_count := ln_err_count;
         
   CALL dlgrenkei.proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;

END;
$$; 