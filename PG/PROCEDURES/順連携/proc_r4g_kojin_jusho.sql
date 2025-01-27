--------------------------------------------------------
--  DDL for Procedure proc_r4g_kojin_jusho
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_kojin_jusho (
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : f_個人_住所（f_kojin_jusho）の追加／更新／削除を実施する                                                */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                     */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                      */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/22  CRESS-INFO.Angelo     新規作成     001o006「住民情報（個人番号あり）」の取込を行う          */
/**********************************************************************************************************************/

DECLARE
   rec_f_kojin_jusho              f_kojin_jusho%ROWTYPE;
   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_del_diag_count              numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;
   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;
   lc_seq_no_tokusokuteishi       numeric DEFAULT 0;
   ln_para01                      numeric DEFAULT 0;
   lc_kojin_no                    character varying;
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
   SELECT
      *
   FROM dlgrenkei.i_r4g_atena AS tbl_atena
   WHERE tbl_atena.saishin_flg = '1' 
   AND tbl_atena.rireki_no = (
      SELECT MAX(rireki_no)
      FROM dlgrenkei.i_r4g_atena
      WHERE atena_no = tbl_atena.atena_no
   )
   AND tbl_atena.rireki_no_eda = (
      SELECT MAX(rireki_no_eda)
      FROM dlgrenkei.i_r4g_atena
      WHERE atena_no = tbl_atena.atena_no
        AND rireki_no = tbl_atena.rireki_no
   )
   AND tbl_atena.result_cd < 8;

   rec_main                       dlgrenkei.i_r4g_kojin_jusho%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM f_kojin_jusho
   WHERE kojin_no = lc_kojin_no;
   rec_lock             f_kojin_jusho%ROWTYPE;
   
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
         SELECT COUNT(*) INTO ln_del_count FROM f_kojin_jusho;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_kojin_jusho';
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

         lc_seq_no_tokusokuteishi := rec_main.tokusoku_teishi_kanri_no;
         lc_kojin_no := rec_main.atena_no;

          -- 個人番号
         rec_f_kojin_jusho.kojin_no := rec_main.atena_no;
          -- 住所_市区町村コード
         rec_f_kojin_jusho.adr_shikuchoson_cd := rec_main.jusho_shikuchoson_cd;
          -- 住所_町字コード
         rec_f_kojin_jusho.adr_machiaza_cd := rec_main.jusho_machiaza_cd;
          -- 住所_都道府県
         rec_f_kojin_jusho.adr_todofuken := get_trimmed_space(rec_main.jusho_todofuken);
          -- 住所_市区郡町村名
         rec_f_kojin_jusho.adr_shikugunchoson := get_trimmed_space(rec_main.jusho_shikugunchoson);
          -- 住所_町字
         rec_f_kojin_jusho.adr_machiaza := get_trimmed_space(rec_main.jusho_machiaza);
          -- 住所_番地号表記
         rec_f_kojin_jusho.adr_banchigohyoki := get_trimmed_space(rec_main.jusho_banchigohyoki);
          -- 住所_番地枝番数値
         rec_f_kojin_jusho.adr_banchi_eda := get_trimmed_space(rec_main.jusho_banchi_eda);
          -- 住所_方書コード
         rec_f_kojin_jusho.adr_jusho_katagaki_cd := rec_main.jusho_katagaki_cd;
          -- 住所_方書
         rec_f_kojin_jusho.adr_jusho_katagaki := get_trimmed_space(rec_main.jusho_katagaki);
          -- 住所_方書_フリガナ
         rec_f_kojin_jusho.adr_jusho_katagaki_kana := get_trimmed_space(rec_main.jusho_katagaki_kana);
          -- 住所_郵便番号
         rec_f_kojin_jusho.adr_yubin_no := rec_main.jusho_yubin_no;
          -- 住所_確定住所
         rec_f_kojin_jusho.adr_kakutei_jusho := CONCAT(
                        rec_main.jusho_todofuken
                        , rec_main.jusho_shikugunchoson
                        , rec_main.jusho_machiaza
                        , rec_main.jusho_banchigohyoki
                     );
          -- 住所_国名コード            
         rec_f_kojin_jusho.adr_kokumei_cd := NULL;
          -- 住所_国名等
         rec_f_kojin_jusho.adr_kokumeito := NULL;
          -- 住所_国外住所
         rec_f_kojin_jusho.adr_kokugai_jusho := NULL;
          -- 転入前住所_市区町村コード
         rec_f_kojin_jusho.tennyumae_shikuchoson_cd := rec_main.tennyumae_shikuchoson_cd;
          -- 転入前住所_町字コード
         rec_f_kojin_jusho.tennyumae_machiaza_cd := rec_main.tennyumae_machiaza_cd;
          --転入前住所_都道府県
         rec_f_kojin_jusho.tennyumae_todofuken := get_trimmed_space(rec_main.tennyumae_todofuken);
          -- 転入前住所_市区郡町村名
         rec_f_kojin_jusho.tennyumae_shikugunchoson := get_trimmed_space(rec_main.tennyumae_shikugunchoson);
          -- 転入前住所_町字
         rec_f_kojin_jusho.tennyumae_machiaza := get_trimmed_space(rec_main.tennyumae_machiaza);
          -- 転入前住所_番地号表記
         rec_f_kojin_jusho.tennyumae_banchigohyoki := get_trimmed_space(rec_main.tennyumae_banchigohyoki);
          -- 転入前住所_方書
         rec_f_kojin_jusho.tennyumae_jusho_katagaki := get_trimmed_space(rec_main.tennyumae_jusho_katagaki);
          -- 転入前住所_郵便番号
         rec_f_kojin_jusho.tennyumae_yubin_no := rec_main.tennyumae_yubin_no;
          -- 転入前住所_確定住所
         rec_f_kojin_jusho.tennyumae_kakutei_jusho := CONCAT(
                        rec_main.tennyumae_todofuken
                        , rec_main.tennyumae_shikugunchoson
                        , rec_main.tennyumae_machiaza
                        , rec_main.tennyumae_banchigohyoki
                     );
          -- 転入前住所_国名コード
         rec_f_kojin_jusho.tennyumae_kokumei_cd := rec_main.tennyumae_kokumei_cd;
          -- 転入前住所_国名等
         rec_f_kojin_jusho.tennyumae_kokumeito := get_trimmed_space(rec_main.tennyumae_kokumeito);
          -- 転入前住所_国外住所
         rec_f_kojin_jusho.tennyumae_kokugai_jusho := get_trimmed_space(rec_main.tennyumae_kokugai_jusho);
          -- 最終登録住所_市区町村コード
         rec_f_kojin_jusho.saishu_shikuchoson_cd := rec_main.saishu_shikuchoson_cd;
          -- 最終登録住所_町字コード
         rec_f_kojin_jusho.saishu_machiaza_cd := rec_main.saishu_machiaza_cd;
          -- 最終登録住所_都道府県
         rec_f_kojin_jusho.saishu_todofuken := get_trimmed_space(rec_main.saishu_todofuken);
          -- 最終登録住所_市区郡町村名
         rec_f_kojin_jusho.saishu_shikugunchoson := get_trimmed_space(rec_main.saishu_shikugunchoson);
          -- 最終登録住所_町字
         rec_f_kojin_jusho.saishu_machiaza := get_trimmed_space(rec_main.saishu_machiaza);
          -- 最終登録住所_番地号表記
         rec_f_kojin_jusho.saishu_banchigohyoki := get_trimmed_space(rec_main.saishu_banchigohyoki);
          -- 最終登録住所_方書
         rec_f_kojin_jusho.saishu_jusho_katagaki := get_trimmed_space(rec_main.saishu_jusho_katagaki);
          -- 最終登録住所_郵便番号
         rec_f_kojin_jusho.saishu_yubin_no := rec_main.saishu_yubin_no;
          -- 最終登録住所_確定住
         rec_f_kojin_jusho.saishu_kakutei_jusho := CONCAT(
                        rec_main.saishu_todofuken
                        , rec_main.saishu_shikugunchoson
                        , rec_main.saishu_machiaza
                        , rec_main.saishu_banchigohyoki
                     );
          -- 転居前住所_市区町村コード
         rec_f_kojin_jusho.tenkyomae_shikuchoson_cd := rec_main.tenkyomae_shikuchoson_cd;
          -- 転居前住所_町字コード
         rec_f_kojin_jusho.tenkyomae_machiaza_cd := rec_main.tenkyomae_machiaza_cd;
          -- 転居前住所_都道府県
         rec_f_kojin_jusho.tenkyomae_todofuken := get_trimmed_space(rec_main.tenkyomae_todofuken);
          -- 転居前住所_市区郡町村名
         rec_f_kojin_jusho.tenkyomae_shikugunchoson := get_trimmed_space(rec_main.tenkyomae_shikugunchoson);
          -- 転居前住所_町字
         rec_f_kojin_jusho.tenkyomae_machiaza := get_trimmed_space(rec_main.tenkyomae_machiaza);
          -- 転居前住所_番地号表記
         rec_f_kojin_jusho.tenkyomae_banchigohyoki := get_trimmed_space(rec_main.tenkyomae_banchigohyoki);
          -- 転居前住所_方書コード
         rec_f_kojin_jusho.tenkyomae_jusho_katagaki_cd := rec_main.tenkyomae_jusho_katagaki_cd;
          -- 転居前住所_方書
         rec_f_kojin_jusho.tenkyomae_jusho_katagaki := get_trimmed_space(rec_main.tenkyomae_jusho_katagaki);
          -- 転居前住所_方書_フリガナ
         rec_f_kojin_jusho.tenkyomae_jusho_katagaki_kana := get_trimmed_space(rec_main.tenkyomae_jusho_katagaki_kana);
          -- 転居前住所_郵便番号
         rec_f_kojin_jusho.tenkyomae_yubin_no := rec_main.tenkyomae_yubin_no;
          -- 転居前住所_確定住所
         rec_f_kojin_jusho.tenkyomae_kakutei_jusho := CONCAT(
                        rec_main.tenkyomae_todofuken
                        , rec_main.tenkyomae_shikugunchoson
                        , rec_main.tenkyomae_machiaza
                        , rec_main.tenkyomae_banchigohyoki
                     );
          -- 転出先住所（予定）_市区町村コード
         rec_f_kojin_jusho.tenshutsu_yotei_shikuchoson_cd := rec_main.tenshutsusaki_yotei_shikuchoson_cd;
          -- 転出先住所（予定）_町字コード
         rec_f_kojin_jusho.tenshutsu_yotei_machiaza_cd := rec_main.tenshutsusaki_yotei_machiaza_cd;
          -- 転出先住所（予定）_都道府県
         rec_f_kojin_jusho.tenshutsu_yotei_todofuken := get_trimmed_space(rec_main.tenshutsusaki_yotei_todofuken);
          -- 転出先住所（予定）_市区郡町村名
         rec_f_kojin_jusho.tenshutsu_yotei_shikugunchoson := get_trimmed_space(rec_main.tenshutsusaki_yotei_shikugunchoson);
          -- 転出先住所（予定）_町字
         rec_f_kojin_jusho.tenshutsu_yotei_machiaza := get_trimmed_space(rec_main.tenshutsusaki_yotei_machiaza);
          -- 転出先住所（予定）_番地号表記
         rec_f_kojin_jusho.tenshutsu_yotei_banchigohyoki := get_trimmed_space(rec_main.tenshutsusaki_yotei_banchigohyoki);
          -- 転出先住所（予定）_方書
         rec_f_kojin_jusho.tenshutsu_yotei_jusho_katagaki := get_trimmed_space(rec_main.tenshutsusaki_yotei_jusho_katagaki);
          -- 転出先住所（予定）_郵便番号
         rec_f_kojin_jusho.tenshutsu_yotei_yubin_no := rec_main.tenshutsusaki_yotei_yubin_no;
          -- 転出先住所（予定）_確定住所
         rec_f_kojin_jusho.tenshutsu_yotei_kakutei_jusho := CONCAT(
                        rec_main.tenshutsusaki_yotei_todofuken
                        , rec_main.tenshutsusaki_yotei_shikugunchoson
                        , rec_main.tenshutsusaki_yotei_machiaza
                        , rec_main.tenshutsusaki_yotei_banchigohyoki
                     );
          -- 転出先住所（予定）_国名コード
         rec_f_kojin_jusho.tenshutsu_yotei_kokumei_cd := rec_main.tenshutsusaki_yotei_kokumei_cd;
          -- 転出先住所（予定）_国名等
         rec_f_kojin_jusho.tenshutsu_yotei_kokumei := get_trimmed_space(rec_main.tenshutsusaki_yotei_kokumei);
          -- 転出先住所（予定）_国外住所
         rec_f_kojin_jusho.tenshutsu_yotei_kokugai_jusho := get_trimmed_space(rec_main.tenshutsusaki_yotei_kokugai_jusho);
          -- 転出先住所（確定）_市区町村コード
         rec_f_kojin_jusho.tenshutsusaki_shikuchoson_cd := rec_main.tenshutsusaki_shikuchoson_cd;
          -- 転出先住所（確定）_町字コード
         rec_f_kojin_jusho.tenshutsusaki_machiaza_cd := rec_main.tenshutsusaki_machiaza_cd;
          -- 転出先住所（確定）_都道府県
         rec_f_kojin_jusho.tenshutsusaki_todofuken := get_trimmed_space(rec_main.tenshutsusaki_todofuken);
          -- 転出先住所（確定）_市区郡町村名
         rec_f_kojin_jusho.tenshutsusaki_shikugunchoson := get_trimmed_space(rec_main.tenshutsusaki_shikugunchoson);
          -- 転出先住所（確定）_町字
         rec_f_kojin_jusho.tenshutsusaki_machiaza := get_trimmed_space(rec_main.tenshutsusaki_machiaza);
          -- 転出先住所（確定）_番地号表記
         rec_f_kojin_jusho.tenshutsusaki_banchigohyoki := get_trimmed_space(rec_main.tenshutsusaki_banchigohyoki);
          -- 転出先住所（確定）_方書
         rec_f_kojin_jusho.tenshutsusaki_jusho_katagaki := get_trimmed_space(rec_main.tenshutsusaki_jusho_katagaki);
          -- 転出先住所（確定）_郵便番号
         rec_f_kojin_jusho.tenshutsusaki_yubin_no := rec_main.tenshutsusaki_yubin_no;
          -- 転出先住所（確定）_確定住所
         rec_f_kojin_jusho.tenshutsusaki_kakutei_jusho := CONCAT(
                        rec_main.tenshutsusaki_todofuken
                        , rec_main.tenshutsusaki_shikugunchoson
                        , rec_main.tenshutsusaki_machiaza
                        , rec_main.tenshutsusaki_banchigohyoki
                     );
          -- データ作成日時
         rec_f_kojin_jusho.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
          -- データ更新日時
         rec_f_kojin_jusho.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
          -- 更新担当者コード
         rec_f_kojin_jusho.upd_tantosha_cd := rec_main.upd_tantosha_cd;
         -- 更新端末名称
         rec_f_kojin_jusho.upd_tammatsu := 'SERVER';
          -- 削除フラグ
         rec_f_kojin_jusho.del_flg := 0;

         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         IF rec_main.del_flg = '1' THEN
            BEGIN
               DELETE FROM f_kojin_jusho
               WHERE kojin_no = lc_kojin_no;

               GET DIAGNOSTICS ln_del_diag_count := ROW_COUNT;
               ln_del_count = ln_del_diag_count + ln_del_count;
            EXCEPTION
               WHEN OTHERS THEN
                  io_c_err_code := SQLSTATE;
                  io_c_err_text    := SQLERRM;

                  RETURN;
            END;
         ELSE

            IF rec_lock IS NULL THEN
               BEGIN
                  INSERT INTO f_kojin_jusho (
                     kojin_no
                     , adr_shikuchoson_cd
                     , adr_machiaza_cd
                     , adr_todofuken
                     , adr_shikugunchoson
                     , adr_machiaza
                     , adr_banchigohyoki
                     , adr_banchi_eda
                     , adr_jusho_katagaki_cd
                     , adr_jusho_katagaki
                     , adr_jusho_katagaki_kana
                     , adr_yubin_no
                     , adr_kakutei_jusho
                     , adr_kokumei_cd
                     , adr_kokumeito
                     , adr_kokugai_jusho
                     , tennyumae_shikuchoson_cd
                     , tennyumae_machiaza_cd
                     , tennyumae_todofuken
                     , tennyumae_shikugunchoson
                     , tennyumae_machiaza
                     , tennyumae_banchigohyoki
                     , tennyumae_jusho_katagaki
                     , tennyumae_yubin_no
                     , tennyumae_kakutei_jusho
                     , tennyumae_kokumei_cd
                     , tennyumae_kokumeito
                     , tennyumae_kokugai_jusho
                     , saishu_shikuchoson_cd
                     , saishu_machiaza_cd
                     , saishu_todofuken
                     , saishu_shikugunchoson
                     , saishu_machiaza
                     , saishu_banchigohyoki
                     , saishu_jusho_katagaki
                     , saishu_yubin_no
                     , saishu_kakutei_jusho
                     , tenkyomae_shikuchoson_cd
                     , tenkyomae_machiaza_cd
                     , tenkyomae_todofuken
                     , tenkyomae_shikugunchoson
                     , tenkyomae_machiaza
                     , tenkyomae_banchigohyoki
                     , tenkyomae_jusho_katagaki_cd
                     , tenkyomae_jusho_katagaki
                     , tenkyomae_jusho_katagaki_kana
                     , tenkyomae_yubin_no
                     , tenkyomae_kakutei_jusho
                     , tenshutsu_yotei_shikuchoson_cd
                     , tenshutsu_yotei_machiaza_cd
                     , tenshutsu_yotei_todofuken
                     , tenshutsu_yotei_shikugunchoson
                     , tenshutsu_yotei_machiaza
                     , tenshutsu_yotei_banchigohyoki
                     , tenshutsu_yotei_jusho_katagaki
                     , tenshutsu_yotei_yubin_no
                     , tenshutsu_yotei_kakutei_jusho
                     , tenshutsu_yotei_kokumei_cd
                     , tenshutsu_yotei_kokumei
                     , tenshutsu_yotei_kokugai_jusho
                     , tenshutsusaki_shikuchoson_cd
                     , tenshutsusaki_machiaza_cd
                     , tenshutsusaki_todofuken
                     , tenshutsusaki_shikugunchoson
                     , tenshutsusaki_machiaza
                     , tenshutsusaki_banchigohyoki
                     , tenshutsusaki_jusho_katagaki
                     , tenshutsusaki_yubin_no
                     , tenshutsusaki_kakutei_jusho
                     , ins_datetime
                     , upd_datetime
                     , upd_tantosha_cd
                     , upd_tammatsu
                     , del_flg
                  )
                  VALUES (
                     rec_f_kojin_jusho.kojin_no
                     , rec_f_kojin_jusho.adr_shikuchoson_cd
                     , rec_f_kojin_jusho.adr_machiaza_cd
                     , rec_f_kojin_jusho.adr_todofuken
                     , rec_f_kojin_jusho.adr_shikugunchoson
                     , rec_f_kojin_jusho.adr_machiaza
                     , rec_f_kojin_jusho.adr_banchigohyoki
                     , rec_f_kojin_jusho.adr_banchi_eda
                     , rec_f_kojin_jusho.adr_jusho_katagaki_cd
                     , rec_f_kojin_jusho.adr_jusho_katagaki
                     , rec_f_kojin_jusho.adr_jusho_katagaki_kana
                     , rec_f_kojin_jusho.adr_yubin_no
                     , rec_f_kojin_jusho.adr_kakutei_jusho
                     , rec_f_kojin_jusho.adr_kokumei_cd
                     , rec_f_kojin_jusho.adr_kokumeito
                     , rec_f_kojin_jusho.adr_kokugai_jusho
                     , rec_f_kojin_jusho.tennyumae_shikuchoson_cd
                     , rec_f_kojin_jusho.tennyumae_machiaza_cd
                     , rec_f_kojin_jusho.tennyumae_todofuken
                     , rec_f_kojin_jusho.tennyumae_shikugunchoson
                     , rec_f_kojin_jusho.tennyumae_machiaza
                     , rec_f_kojin_jusho.tennyumae_banchigohyoki
                     , rec_f_kojin_jusho.tennyumae_jusho_katagaki
                     , rec_f_kojin_jusho.tennyumae_yubin_no
                     , rec_f_kojin_jusho.tennyumae_kakutei_jusho
                     , rec_f_kojin_jusho.tennyumae_kokumei_cd
                     , rec_f_kojin_jusho.tennyumae_kokumeito
                     , rec_f_kojin_jusho.tennyumae_kokugai_jusho
                     , rec_f_kojin_jusho.saishu_shikuchoson_cd
                     , rec_f_kojin_jusho.saishu_machiaza_cd
                     , rec_f_kojin_jusho.saishu_todofuken
                     , rec_f_kojin_jusho.saishu_shikugunchoson
                     , rec_f_kojin_jusho.saishu_machiaza
                     , rec_f_kojin_jusho.saishu_banchigohyoki
                     , rec_f_kojin_jusho.saishu_jusho_katagaki
                     , rec_f_kojin_jusho.saishu_yubin_no
                     , rec_f_kojin_jusho.saishu_kakutei_jusho
                     , rec_f_kojin_jusho.tenkyomae_shikuchoson_cd
                     , rec_f_kojin_jusho.tenkyomae_machiaza_cd
                     , rec_f_kojin_jusho.tenkyomae_todofuken
                     , rec_f_kojin_jusho.tenkyomae_shikugunchoson
                     , rec_f_kojin_jusho.tenkyomae_machiaza
                     , rec_f_kojin_jusho.tenkyomae_banchigohyoki
                     , rec_f_kojin_jusho.tenkyomae_jusho_katagaki_cd
                     , rec_f_kojin_jusho.tenkyomae_jusho_katagaki
                     , rec_f_kojin_jusho.tenkyomae_jusho_katagaki_kana
                     , rec_f_kojin_jusho.tenkyomae_yubin_no
                     , rec_f_kojin_jusho.tenkyomae_kakutei_jusho
                     , rec_f_kojin_jusho.tenshutsu_yotei_shikuchoson_cd
                     , rec_f_kojin_jusho.tenshutsu_yotei_machiaza_cd
                     , rec_f_kojin_jusho.tenshutsu_yotei_todofuken
                     , rec_f_kojin_jusho.tenshutsu_yotei_shikugunchoson
                     , rec_f_kojin_jusho.tenshutsu_yotei_machiaza
                     , rec_f_kojin_jusho.tenshutsu_yotei_banchigohyoki
                     , rec_f_kojin_jusho.tenshutsu_yotei_jusho_katagaki
                     , rec_f_kojin_jusho.tenshutsu_yotei_yubin_no
                     , rec_f_kojin_jusho.tenshutsu_yotei_kakutei_jusho
                     , rec_f_kojin_jusho.tenshutsu_yotei_kokumei_cd
                     , rec_f_kojin_jusho.tenshutsu_yotei_kokumei
                     , rec_f_kojin_jusho.tenshutsu_yotei_kokugai_jusho
                     , rec_f_kojin_jusho.tenshutsusaki_shikuchoson_cd
                     , rec_f_kojin_jusho.tenshutsusaki_machiaza_cd
                     , rec_f_kojin_jusho.tenshutsusaki_todofuken
                     , rec_f_kojin_jusho.tenshutsusaki_shikugunchoson
                     , rec_f_kojin_jusho.tenshutsusaki_machiaza
                     , rec_f_kojin_jusho.tenshutsusaki_banchigohyoki
                     , rec_f_kojin_jusho.tenshutsusaki_jusho_katagaki
                     , rec_f_kojin_jusho.tenshutsusaki_yubin_no
                     , rec_f_kojin_jusho.tenshutsusaki_kakutei_jusho
                     , rec_f_kojin_jusho.ins_datetime
                     , rec_f_kojin_jusho.upd_datetime
                     , rec_f_kojin_jusho.upd_tantosha_cd
                     , rec_f_kojin_jusho.upd_tammatsu
                     , rec_f_kojin_jusho.del_flg
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
                  UPDATE f_kojin_jusho
                  SET adr_shikuchoson_cd = rec_f_kojin_jusho.adr_shikuchoson_cd
                     , adr_machiaza_cd = rec_f_kojin_jusho.adr_machiaza_cd
                     , adr_todofuken = rec_f_kojin_jusho.adr_todofuken
                     , adr_shikugunchoson = rec_f_kojin_jusho.adr_shikugunchoson
                     , adr_machiaza = rec_f_kojin_jusho.adr_machiaza
                     , adr_banchigohyoki = rec_f_kojin_jusho.adr_banchigohyoki
                     , adr_banchi_eda = rec_f_kojin_jusho.adr_banchi_eda
                     , adr_jusho_katagaki_cd = rec_f_kojin_jusho.adr_jusho_katagaki_cd
                     , adr_jusho_katagaki = rec_f_kojin_jusho.adr_jusho_katagaki
                     , adr_jusho_katagaki_kana = rec_f_kojin_jusho.adr_jusho_katagaki_kana
                     , adr_yubin_no = rec_f_kojin_jusho.adr_yubin_no
                     , adr_kakutei_jusho = rec_f_kojin_jusho.adr_kakutei_jusho
                     , tennyumae_shikuchoson_cd = rec_f_kojin_jusho.tennyumae_shikuchoson_cd
                     , tennyumae_machiaza_cd = rec_f_kojin_jusho.tennyumae_machiaza_cd
                     , tennyumae_todofuken = rec_f_kojin_jusho.tennyumae_todofuken
                     , tennyumae_shikugunchoson = rec_f_kojin_jusho.tennyumae_shikugunchoson
                     , tennyumae_machiaza = rec_f_kojin_jusho.tennyumae_machiaza
                     , tennyumae_banchigohyoki = rec_f_kojin_jusho.tennyumae_banchigohyoki
                     , tennyumae_jusho_katagaki = rec_f_kojin_jusho.tennyumae_jusho_katagaki
                     , tennyumae_yubin_no = rec_f_kojin_jusho.tennyumae_yubin_no
                     , tennyumae_kakutei_jusho = rec_f_kojin_jusho.tennyumae_kakutei_jusho
                     , tennyumae_kokumei_cd = rec_f_kojin_jusho.tennyumae_kokumei_cd
                     , tennyumae_kokumeito = rec_f_kojin_jusho.tennyumae_kokumeito
                     , tennyumae_kokugai_jusho = rec_f_kojin_jusho.tennyumae_kokugai_jusho
                     , saishu_shikuchoson_cd = rec_f_kojin_jusho.saishu_shikuchoson_cd
                     , saishu_machiaza_cd = rec_f_kojin_jusho.saishu_machiaza_cd
                     , saishu_todofuken = rec_f_kojin_jusho.saishu_todofuken
                     , saishu_shikugunchoson = rec_f_kojin_jusho.saishu_shikugunchoson
                     , saishu_machiaza = rec_f_kojin_jusho.saishu_machiaza
                     , saishu_banchigohyoki = rec_f_kojin_jusho.saishu_banchigohyoki
                     , saishu_jusho_katagaki = rec_f_kojin_jusho.saishu_jusho_katagaki
                     , saishu_yubin_no = rec_f_kojin_jusho.saishu_yubin_no
                     , saishu_kakutei_jusho = rec_f_kojin_jusho.saishu_kakutei_jusho
                     , tenkyomae_shikuchoson_cd = rec_f_kojin_jusho.tenkyomae_shikuchoson_cd
                     , tenkyomae_machiaza_cd = rec_f_kojin_jusho.tenkyomae_machiaza_cd
                     , tenkyomae_todofuken = rec_f_kojin_jusho.tenkyomae_todofuken
                     , tenkyomae_shikugunchoson = rec_f_kojin_jusho.tenkyomae_shikugunchoson
                     , tenkyomae_machiaza = rec_f_kojin_jusho.tenkyomae_machiaza
                     , tenkyomae_banchigohyoki = rec_f_kojin_jusho.tenkyomae_banchigohyoki
                     , tenkyomae_jusho_katagaki_cd = rec_f_kojin_jusho.tenkyomae_jusho_katagaki_cd
                     , tenkyomae_jusho_katagaki = rec_f_kojin_jusho.tenkyomae_jusho_katagaki
                     , tenkyomae_jusho_katagaki_kana = rec_f_kojin_jusho.tenkyomae_jusho_katagaki_kana
                     , tenkyomae_yubin_no = rec_f_kojin_jusho.tenkyomae_yubin_no
                     , tenkyomae_kakutei_jusho = rec_f_kojin_jusho.tenkyomae_kakutei_jusho
                     , tenshutsu_yotei_shikuchoson_cd = rec_f_kojin_jusho.tenshutsu_yotei_shikuchoson_cd
                     , tenshutsu_yotei_machiaza_cd = rec_f_kojin_jusho.tenshutsu_yotei_machiaza_cd
                     , tenshutsu_yotei_todofuken = rec_f_kojin_jusho.tenshutsu_yotei_todofuken
                     , tenshutsu_yotei_shikugunchoson = rec_f_kojin_jusho.tenshutsu_yotei_shikugunchoson
                     , tenshutsu_yotei_machiaza = rec_f_kojin_jusho.tenshutsu_yotei_machiaza
                     , tenshutsu_yotei_banchigohyoki = rec_f_kojin_jusho.tenshutsu_yotei_banchigohyoki
                     , tenshutsu_yotei_jusho_katagaki = rec_f_kojin_jusho.tenshutsu_yotei_jusho_katagaki
                     , tenshutsu_yotei_yubin_no = rec_f_kojin_jusho.tenshutsu_yotei_yubin_no
                     , tenshutsu_yotei_kakutei_jusho = rec_f_kojin_jusho.tenshutsu_yotei_kakutei_jusho
                     , tenshutsu_yotei_kokumei_cd = rec_f_kojin_jusho.tenshutsu_yotei_kokumei_cd
                     , tenshutsu_yotei_kokumei = rec_f_kojin_jusho.tenshutsu_yotei_kokumei
                     , tenshutsu_yotei_kokugai_jusho = rec_f_kojin_jusho.tenshutsu_yotei_kokugai_jusho
                     , tenshutsusaki_shikuchoson_cd = rec_f_kojin_jusho.tenshutsusaki_shikuchoson_cd
                     , tenshutsusaki_machiaza_cd = rec_f_kojin_jusho.tenshutsusaki_machiaza_cd
                     , tenshutsusaki_todofuken = rec_f_kojin_jusho.tenshutsusaki_todofuken
                     , tenshutsusaki_shikugunchoson = rec_f_kojin_jusho.tenshutsusaki_shikugunchoson
                     , tenshutsusaki_machiaza = rec_f_kojin_jusho.tenshutsusaki_machiaza
                     , tenshutsusaki_banchigohyoki = rec_f_kojin_jusho.tenshutsusaki_banchigohyoki
                     , tenshutsusaki_jusho_katagaki = rec_f_kojin_jusho.tenshutsusaki_jusho_katagaki
                     , tenshutsusaki_yubin_no = rec_f_kojin_jusho.tenshutsusaki_yubin_no
                     , tenshutsusaki_kakutei_jusho = rec_f_kojin_jusho.tenshutsusaki_kakutei_jusho
                     , upd_datetime = rec_f_kojin_jusho.upd_datetime
                     , upd_tantosha_cd = rec_f_kojin_jusho.upd_tantosha_cd
                     , upd_tammatsu = rec_f_kojin_jusho.upd_tammatsu
                  WHERE kojin_no = rec_f_kojin_jusho.lc_kojin_no;

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
         END IF;

         -- 中間テーブル更新
         UPDATE dlgrenkei.i_r4g_atena 
         SET result_cd = ln_result_cd
            , error_cd = lc_err_cd
            , error_text = lc_err_text
         WHERE shikuchoson_cd = rec_main.shikuchoson_cd
            AND atena_no = rec_main.atena_no
            AND rireki_no = rec_main.rireki_no
            AND rireki_no_eda = rec_main.rireki_no_eda;

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

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;

EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;