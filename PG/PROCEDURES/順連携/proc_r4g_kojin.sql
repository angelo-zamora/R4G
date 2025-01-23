--------------------------------------------------------
--  DDL for Procedure  proc_r4g_kojin
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_kojin ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, io_c_err_code INOUT character varying, io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 住民情報（個人番号あり）                                                                              */
/* 引数 IN  : in_n_renkei_data_cd  … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/

DECLARE
   rec_kojin                      f_kojin%ROWTYPE;
   rec_log                        f_renkei_log%ROWTYPE;

   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;

   ln_para01                      numeric DEFAULT 0;
   ln_para02                      numeric DEFAULT 0;
   ln_para09                      numeric DEFAULT 0;
   ln_para12                      numeric DEFAULT 0;

   lc_sql                         character varying(1000);

   ln_tsuchisho_no_length         type_tsuchisho_no_length[];
   ln_kojin_no_length             numeric DEFAULT 15;

   
   
   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_atena AS atena1
      LEFT JOIN(
         SELECT
            shikuchoson_cd,
            atena_no,
            MAX(rireki_no) AS max_rireki_no,
            MAX(rireki_no_eda) AS max_rireki_no_eda
         FROM i_r4g_atena
         GROUP BY
            atena_no,
         rireki_no
      ) AS atena2
      ON atena1.shikuchoson_cd = atena2.shikuchoson_cd
         AND atena1.atena_no = atena2.atena_no
         AND atena1.rireki_no = atena2.max_rireki_no
         AND atena1.rireki_no_eda = atena2.max_rireki_no_eda
   WHERE saishin_flg = '1'
      AND result_cd < 8;
   
   rec_main                       i_r4g_atena%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;
   
   cur_lock CURSOR FOR
   SELECT *
   FROM f_kojin
   WHERE kojin_no = rec_kojin.kojin_no;

   rec_lock                       f_kojin%ROWTYPE;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   -- １．パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 9 THEN ln_para09 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 12 THEN ln_para12 := rec_parameter.parameter_value; END IF;
      END LOOP;
   CLOSE cur_parameter;

   -- ２．連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_kojin;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_kojin';
         EXECUTE lc_sql;
      EXCEPTION WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text := SQLERRM;
         RETURN;
      END;
   END IF;


   -- ３．中間テーブルデータのエラーチェック
   CALL proc_error_check( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );

   IF io_c_err_code <> '0' THEN
      RETURN;
   END IF;
   
   -- ４．桁数設定情報取得
   -- r4gでは不要

   -- ５．連携データの作成・更新
   ln_shori_count := 0;
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

            ln_shori_count                 := ln_shori_count + 1;
            lc_err_cd                      := '0';
            lc_err_text                    := '';
            ln_result_cd                   := 0;
            rec_kojin                      := NULL;
            rec_lock                       := NULL;

            rec_kojin.kojin_no := rec_main.atena_no;
            rec_kojin.setai_no := '000000000000000';
            rec_kojin.shimei_kbn := '0';
            rec_kojin.kannai_kbn := 2;
            rec_kojin.jumin_kbn := 2;
            rec_kojin.jumin_shubetsu_cd := 2;--todo
            rec_kojin.jumin_jotai_cd := ;
            rec_kojin.renkei_shimei := rec_main.shimei;
            rec_kojin.renkei_shimei_kana := rec_main.shimei_kana;
            rec_kojin.renkei_tsushomei := get_trimmed_space(rec_main.tsushomei);
            rec_kojin.renkei_tsushomei_kana := get_trimmed_space(rec_main.tsushomei_kana);
            rec_kojin.renkei_tsushomei_kana_flg := CASE WHEN rec_main.tsushomei_kana_flg IS NULL OR rec_main.tsushomei_kana_flg = '' THEN 0 ELSE rec_main.tsushomei_kana_flg::numeric END;
            rec_kojin.shimei := get_trimmed_space(rec_main.shimei);
            rec_kojin.nihonjin_uji := get_trimmed_space(rec_main.uji_nihonjin);
            rec_kojin.nihonjin_mei := get_trimmed_space(rec_main.mei_nihonjin);
            rec_kojin.shimei_kana := get_trimmed_space(rec_main.shimei_kana);
            rec_kojin.shimei_kana_kosho_kakunin_kbn := 9;
            rec_kojin.shimei_kensaku_kana := get_trimmed_space(rec_main.tsushomei_kana_flg);--todo
            rec_kojin.nihonjin_uji_kana := get_trimmed_space(rec_main.uji_nihonjin_kana);
            rec_kojin.nihonjin_mei_kana := get_trimmed_space(rec_main.mei_nihonjin_kana);
            rec_kojin.shimei_gaikokujin_romaji := get_trimmed_space(rec_main.shimei_gaikokujin_romaji);
            rec_kojin.shimei_gaikokujin_kanji := get_trimmed_space(rec_main.shimei_gaikokujin_kanji);
            rec_kojin.old_uji := NULL;
            rec_kojin.old_uji_kana := NULL;
            rec_kojin.old_uji_kana_flg := 0;
            rec_kojin.yubin_no := get_trimmed_space(rec_main.tsushomei_kana_flg);--todo
            rec_kojin.jusho := get_trimmed_space(rec_main.tsushomei_kana_flg);--todo
            rec_kojin.jusho_cd := get_trimmed_space(rec_main.tsushomei_kana_flg);--todo
            rec_kojin.shikuchoson_cd := rec_main.shikuchoson_cd;
            rec_kojin.machiaza_cd := rec_main.jusho_machiaza_cd;--todo
            rec_kojin.todofuken := get_trimmed_space(rec_main.jusho_todofuken);
            rec_kojin.shikugunchoson := get_trimmed_space(rec_main.jusho_shikugunchoson);
            rec_kojin.machiaza := get_trimmed_space(rec_main.jusho_machiaza);
            rec_kojin.banchigohyoki := get_trimmed_space(rec_main.jusho_banchigohyoki);
            rec_kojin.banchi_edaban := NULL;
            rec_kojin.jusho_katagaki_cd := NULL;
            rec_kojin.jusho_katagaki := get_trimmed_space(rec_main.jusho_katagaki);
            rec_kojin.jusho_katagaki_kana := get_trimmed_space(rec_main.jusho_katagaki_kana);
            rec_kojin.kokumei_cd := get_trimmed_space(rec_main.kokumei_cd);--todo
            rec_kojin.kokumeito := get_trimmed_space(rec_main.kokumeito);--todo
            rec_kojin.kokugai_jusho := get_trimmed_space(rec_main.kokugai_jusho);--todo
            rec_kojin.koseki_honseki := NULL;
            rec_kojin.honseki_shikuchoson_cd := NULL;
            rec_kojin.honseki_machiaza_cd := NULL;
            rec_kojin.honseki_todofuken := NULL;
            rec_kojin.honseki_shikugunchoson := NULL;
            rec_kojin.honseki_machiaza := NULL;
            rec_kojin.honseki_banchigohyoki := NULL;
            rec_kojin.koseki_hittosha := NULL;
            rec_kojin.koseki_hittosha_uji := NULL;
            rec_kojin.koseki_hittosha_mei := NULL;
            rec_kojin.birth_ymd := CASE WHEN rec_main.birth_ymd IS NULL OR rec_main.birth_ymd = '' THEN 0 ELSE rec_main.birth_ymd::numeric END;
            rec_kojin.birth_fusho_flg := rec_main.birth_fusho_flg::numeric;
            rec_kojin.birth_fusho := rec_main.birth_fusho;
            rec_kojin.zokugara_cd1 := 0;
            rec_kojin.zokugara_cd2 := 0;
            rec_kojin.zokugara_cd3 := 0;
            rec_kojin.zokugara_cd4 := 0;
            rec_kojin.zokugara := NULL;
            rec_kojin.seibetsu_cd := rec_main.seibetsu_cd::numeric;
            rec_kojin.seibetsu := get_trimmed_space(rec_main.seibetsu);--todo
            rec_kojin.chiku_cd := get_trimmed_space(rec_main.chiku_cd);--todo
            rec_kojin.gyoseiku_cd := NULL;
            rec_kojin.jichitai_cd := rec_main.jusho_shikuchoson_cd;
            rec_kojin.shori_ymd := 0;
            rec_kojin.ido_ymd := 0;
            rec_kojin.ido_ymd_fusho_flg := 0;
            rec_kojin.ido_ymd_fusho := 0;
            rec_kojin.ido_todoke_ymd := 0;
            rec_kojin.ido_jiyu_cd := 0;
            rec_kojin.shibo_ymd := 0;
            rec_kojin.jumin_ymd := 0;
            rec_kojin.jumin_ymd_fusho_flg := 0;
            rec_kojin.jumin_ymd_fusho := NULL;
            rec_kojin.gaikokujin_jumin_ymd := 0;
            rec_kojin.gaikokujin_jumin_ymd_fusho_flg := 0;
            rec_kojin.gaikokujin_jumin_ymd_fusho := NULL;
            rec_kojin.jutei_ymd := 0;
            rec_kojin.jutei_ymd_fusho_flg := 0;
            rec_kojin.jutei_ymd_fusho := NULL;
            rec_kojin.tennyu_tsuchi_ymd := 0;
            rec_kojin.tenshutsu_todoke_ymd := 0;
            rec_kojin.tenshutsu_yotei_ymd := 0;
            rec_kojin.tenshutsu_ymd := 0;
            rec_kojin.tsushomei_kensaku_kana := get_trimmed_space(rec_main.tsushomei_kensaku_kana);--todo
            rec_kojin.old_uji_kensaku_kana := NULL;
            rec_kojin.zairyu_card_no := NULL;
            rec_kojin.zairyu_card_no_kbn_cd := 0;
            rec_kojin.kokuseki_cd := 0;
            rec_kojin.kokusekimeito := NULL;
            rec_kojin.zairyu_kitei_kbn_cd := 0;
            rec_kojin.zairyu_shikaku_cd := NULL;
            rec_kojin.zairyu_shikaku := NULL;
            rec_kojin.zairyu_kikan_nen_cd := 0;
            rec_kojin.zairyu_kikan_tsuki_cd := 0;
            rec_kojin.zairyu_kikan_hi_cd := 0;
            rec_kojin.zairyu_manryo_ymd := 0;
            rec_kojin.kisai_juni := 0;
            rec_kojin.zairyu_todoke_kbn_cd := 0;
            rec_kojin.gyomu_cd := 0;--todo
            rec_kojin.yobi_komoku1 := NULL;
            rec_kojin.yobi_komoku2 := NULL;
            rec_kojin.yobi_komoku3 := NULL;
            rec_kojin.yobi_komoku4 := NULL;
            rec_kojin.yobi_komoku5 := NULL;
            rec_kojin.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;;
            rec_kojin.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;;
            rec_kojin.upd_tantosha_cd := rec_main.upd_tantosha_cd;
            rec_kojin.upd_tammatsu := rec_main.upd_tammatsu;
            rec_kojin.del_flg := rec_main.del_flg::numeric;
            
         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;
         
         IF rec_kojin.del_flg = 1 THEN
            BEGIN DELETE FROM f_kojin
               WHERE kojin_no = rec_kojin.kojin_no;

               GET DIAGNOSTICS ln_del_count := ln_del_count + ROW_COUNT;
               lc_err_text := '';
               lc_err_cd := '0';
               ln_result_cd := 3;

            EXCEPTION WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
               lc_err_cd := '9';
               ln_result_cd := 9;
            END;
         ELSE
            IF rec_lock IS NULL THEN
               BEGIN
                  INSERT INTO f_kojin(
                     kojin_no
                     , setai_no
                     , shimei_kbn
                     , kannai_kbn
                     , jumin_kbn
                     , jumin_shubetsu_cd
                     , jumin_jotai_cd
                     , renkei_shimei
                     , renkei_shimei_kana
                     , renkei_tsushomei
                     , renkei_tsushomei_kana
                     , renkei_tsushomei_kana_flg
                     , shimei
                     , nihonjin_uji
                     , nihonjin_mei
                     , shimei_kana
                     , shimei_kana_kosho_kakunin_kbn
                     , shimei_kensaku_kana
                     , nihonjin_uji_kana
                     , nihonjin_mei_kana
                     , shimei_gaikokujin_romaji
                     , shimei_gaikokujin_kanji
                     , old_uji
                     , old_uji_kana
                     , old_uji_kana_flg
                     , yubin_no
                     , jusho
                     , jusho_cd
                     , shikuchoson_cd
                     , machiaza_cd
                     , todofuken
                     , shikugunchoson
                     , machiaza
                     , banchigohyoki
                     , banchi_edaban
                     , jusho_katagaki_cd
                     , jusho_katagaki
                     , jusho_katagaki_kana
                     , kokumei_cd
                     , kokumeito
                     , kokugai_jusho
                     , koseki_honseki
                     , honseki_shikuchoson_cd
                     , honseki_machiaza_cd
                     , honseki_todofuken
                     , honseki_shikugunchoson
                     , honseki_machiaza
                     , honseki_banchigohyoki
                     , koseki_hittosha
                     , koseki_hittosha_uji
                     , koseki_hittosha_mei
                     , birth_ymd
                     , birth_fusho_flg
                     , birth_fusho
                     , zokugara_cd1
                     , zokugara_cd2
                     , zokugara_cd3
                     , zokugara_cd4
                     , zokugara
                     , seibetsu_cd
                     , seibetsu
                     , chiku_cd
                     , gyoseiku_cd
                     , jichitai_cd
                     , shori_ymd
                     , ido_ymd
                     , ido_ymd_fusho_flg
                     , ido_ymd_fusho
                     , ido_todoke_ymd
                     , ido_jiyu_cd
                     , shibo_ymd
                     , jumin_ymd
                     , jumin_ymd_fusho_flg
                     , jumin_ymd_fusho
                     , gaikokujin_jumin_ymd
                     , gaikokujin_jumin_ymd_fusho_flg
                     , gaikokujin_jumin_ymd_fusho
                     , jutei_ymd
                     , jutei_ymd_fusho_flg
                     , jutei_ymd_fusho
                     , tennyu_tsuchi_ymd
                     , tenshutsu_todoke_ymd
                     , tenshutsu_yotei_ymd
                     , tenshutsu_ymd
                     , tsushomei_kensaku_kana
                     , old_uji_kensaku_kana
                     , zairyu_card_no
                     , zairyu_card_no_kbn_cd
                     , kokuseki_cd
                     , kokusekimeito
                     , zairyu_kitei_kbn_cd
                     , zairyu_shikaku_cd
                     , zairyu_shikaku
                     , zairyu_kikan_nen_cd
                     , zairyu_kikan_tsuki_cd
                     , zairyu_kikan_hi_cd
                     , zairyu_manryo_ymd
                     , kisai_juni
                     , zairyu_todoke_kbn_cd
                     , gyomu_cd
                     , yobi_komoku1
                     , yobi_komoku2
                     , yobi_komoku3
                     , yobi_komoku4
                     , yobi_komoku5
                     , ins_datetime
                     , upd_datetime
                     , upd_tantosha_cd
                     , upd_tammatsu
                     , del_flg
                     )
                  VALUES (
                     rec_kojin.kojin_no
                     , rec_kojin.setai_no
                     , rec_kojin.shimei_kbn
                     , rec_kojin.kannai_kbn
                     , rec_kojin.jumin_kbn
                     , rec_kojin.jumin_shubetsu_cd
                     , rec_kojin.jumin_jotai_cd
                     , rec_kojin.renkei_shimei
                     , rec_kojin.renkei_shimei_kana
                     , rec_kojin.renkei_tsushomei
                     , rec_kojin.renkei_tsushomei_kana
                     , rec_kojin.renkei_tsushomei_kana_flg
                     , rec_kojin.shimei
                     , rec_kojin.nihonjin_uji
                     , rec_kojin.nihonjin_mei
                     , rec_kojin.shimei_kana
                     , rec_kojin.shimei_kana_kosho_kakunin_kbn
                     , rec_kojin.shimei_kensaku_kana
                     , rec_kojin.nihonjin_uji_kana
                     , rec_kojin.nihonjin_mei_kana
                     , rec_kojin.shimei_gaikokujin_romaji
                     , rec_kojin.shimei_gaikokujin_kanji
                     , rec_kojin.old_uji
                     , rec_kojin.old_uji_kana
                     , rec_kojin.old_uji_kana_flg
                     , rec_kojin.yubin_no
                     , rec_kojin.jusho
                     , rec_kojin.jusho_cd
                     , rec_kojin.shikuchoson_cd
                     , rec_kojin.machiaza_cd
                     , rec_kojin.todofuken
                     , rec_kojin.shikugunchoson
                     , rec_kojin.machiaza
                     , rec_kojin.banchigohyoki
                     , rec_kojin.banchi_edaban
                     , rec_kojin.jusho_katagaki_cd
                     , rec_kojin.jusho_katagaki
                     , rec_kojin.jusho_katagaki_kana
                     , rec_kojin.kokumei_cd
                     , rec_kojin.kokumeito
                     , rec_kojin.kokugai_jusho
                     , rec_kojin.koseki_honseki
                     , rec_kojin.honseki_shikuchoson_cd
                     , rec_kojin.honseki_machiaza_cd
                     , rec_kojin.honseki_todofuken
                     , rec_kojin.honseki_shikugunchoson
                     , rec_kojin.honseki_machiaza
                     , rec_kojin.honseki_banchigohyoki
                     , rec_kojin.koseki_hittosha
                     , rec_kojin.koseki_hittosha_uji
                     , rec_kojin.koseki_hittosha_mei
                     , rec_kojin.birth_ymd
                     , rec_kojin.birth_fusho_flg
                     , rec_kojin.birth_fusho
                     , rec_kojin.zokugara_cd1
                     , rec_kojin.zokugara_cd2
                     , rec_kojin.zokugara_cd3
                     , rec_kojin.zokugara_cd4
                     , rec_kojin.zokugara
                     , rec_kojin.seibetsu_cd
                     , rec_kojin.seibetsu
                     , rec_kojin.chiku_cd
                     , rec_kojin.gyoseiku_cd
                     , rec_kojin.jichitai_cd
                     , rec_kojin.shori_ymd
                     , rec_kojin.ido_ymd
                     , rec_kojin.ido_ymd_fusho_flg
                     , rec_kojin.ido_ymd_fusho
                     , rec_kojin.ido_todoke_ymd
                     , rec_kojin.ido_jiyu_cd
                     , rec_kojin.shibo_ymd
                     , rec_kojin.jumin_ymd
                     , rec_kojin.jumin_ymd_fusho_flg
                     , rec_kojin.jumin_ymd_fusho
                     , rec_kojin.gaikokujin_jumin_ymd
                     , rec_kojin.gaikokujin_jumin_ymd_fusho_flg
                     , rec_kojin.gaikokujin_jumin_ymd_fusho
                     , rec_kojin.jutei_ymd
                     , rec_kojin.jutei_ymd_fusho_flg
                     , rec_kojin.jutei_ymd_fusho
                     , rec_kojin.tennyu_tsuchi_ymd
                     , rec_kojin.tenshutsu_todoke_ymd
                     , rec_kojin.tenshutsu_yotei_ymd
                     , rec_kojin.tenshutsu_ymd
                     , rec_kojin.tsushomei_kensaku_kana
                     , rec_kojin.old_uji_kensaku_kana
                     , rec_kojin.zairyu_card_no
                     , rec_kojin.zairyu_card_no_kbn_cd
                     , rec_kojin.kokuseki_cd
                     , rec_kojin.kokusekimeito
                     , rec_kojin.zairyu_kitei_kbn_cd
                     , rec_kojin.zairyu_shikaku_cd
                     , rec_kojin.zairyu_shikaku
                     , rec_kojin.zairyu_kikan_nen_cd
                     , rec_kojin.zairyu_kikan_tsuki_cd
                     , rec_kojin.zairyu_kikan_hi_cd
                     , rec_kojin.zairyu_manryo_ymd
                     , rec_kojin.kisai_juni
                     , rec_kojin.zairyu_todoke_kbn_cd
                     , rec_kojin.gyomu_cd
                     , rec_kojin.yobi_komoku1
                     , rec_kojin.yobi_komoku2
                     , rec_kojin.yobi_komoku3
                     , rec_kojin.yobi_komoku4
                     , rec_kojin.yobi_komoku5
                     , rec_kojin.ins_datetime
                     , rec_kojin.upd_datetime
                     , rec_kojin.upd_tantosha_cd
                     , rec_kojin.upd_tammatsu
                     , rec_kojin.del_flg
                     );

                     ln_ins_count := ln_ins_count + 1;
                     lc_err_text := '';
                     lc_err_cd := '0';
                     ln_result_cd := 1;

                  EXCEPTION WHEN OTHERS THEN
                     ln_err_count := ln_err_count + 1;
                     lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                     lc_err_cd := '9';
                     ln_result_cd := 9;
               END;
            ELSE
               BEGIN
                  UPDATE f_taino
                  SET 
                     jumin_shubetsu_cd = rec_kojin.jumin_shubetsu_cd
                     , renkei_shimei = rec_kojin.renkei_shimei
                     , renkei_shimei_kana = rec_kojin.renkei_shimei_kana
                     , renkei_tsushomei = rec_kojin.renkei_tsushomei
                     , renkei_tsushomei_kana = rec_kojin.renkei_tsushomei_kana
                     , renkei_tsushomei_kana_flg = rec_kojin.renkei_tsushomei_kana_flg
                     , shimei = rec_kojin.shimei
                     , nihonjin_uji = rec_kojin.nihonjin_uji
                     , nihonjin_mei = rec_kojin.nihonjin_mei
                     , shimei_kana = rec_kojin.shimei_kana
                     , shimei_kensaku_kana = rec_kojin.shimei_kensaku_kana
                     , nihonjin_uji_kana = rec_kojin.nihonjin_uji_kana
                     , nihonjin_mei_kana = rec_kojin.nihonjin_mei_kana
                     , shimei_gaikokujin_romaji = rec_kojin.shimei_gaikokujin_romaji
                     , shimei_gaikokujin_kanji = rec_kojin.shimei_gaikokujin_kanji
                     , yubin_no = rec_kojin.yubin_no
                     , jusho = rec_kojin.jusho
                     , jusho_cd = rec_kojin.jusho_cd
                     , shikuchoson_cd = rec_kojin.shikuchoson_cd
                     , machiaza_cd = rec_kojin.machiaza_cd
                     , todofuken = rec_kojin.todofuken
                     , shikugunchoson = rec_kojin.shikugunchoson
                     , machiaza = rec_kojin.machiaza
                     , banchigohyoki = rec_kojin.banchigohyoki
                     , jusho_katagaki = rec_kojin.jusho_katagaki
                     , jusho_katagaki_kana = rec_kojin.jusho_katagaki_kana
                     , kokumei_cd = rec_kojin.kokumei_cd
                     , kokumeito = rec_kojin.kokumeito
                     , kokugai_jusho = rec_kojin.kokugai_jusho
                     , birth_ymd = rec_kojin.birth_ymd
                     , birth_fusho_flg = rec_kojin.birth_fusho_flg
                     , birth_fusho = rec_kojin.birth_fusho
                     , seibetsu_cd = rec_kojin.seibetsu_cd
                     , seibetsu = rec_kojin.seibetsu
                     , chiku_cd = rec_kojin.chiku_cd
                     , jichitai_cd = rec_kojin.jichitai_cd
                     , tsushomei_kensaku_kana = rec_kojin.tsushomei_kensaku_kana
                     , gyomu_cd = rec_kojin.gyomu_cd
                     , upd_datetime = rec_kojin.upd_datetime
                     , upd_tantosha_cd = rec_kojin.upd_tantosha_cd
                     , upd_tammatsu = rec_kojin.upd_tammatsu
                  WHERE kibetsu_key = rec_kojin.kibetsu_key;

                  ln_upd_count := ln_upd_count + 1;
                  lc_err_text := '';
                  lc_err_cd := '0';
                  ln_result_cd := 2;

               EXCEPTION WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
                  lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                  lc_err_cd := '9';
                  ln_result_cd := 9;
               END;
            END IF;
         END IF;
         END IF;
         
       -- 中間テーブル更新
         UPDATE i_r4g_atena 
         SET result_cd = ln_result_cd
            , error_cd = lc_err_cd
            , error_text = lc_err_text
         WHERE shikuchoson_cd = rec_main.shikuchoson_cd
            AND atena_no = rec_main.atena_no
            AND rireki_no = rec_main.rireki_no
            AND rireki_no_eda = rec_main.rireki_no_eda;
         
      END LOOP;
   CLOSE cur_main;
   
   CALL proc_r4g_kojin_doitsunin_upd( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );
   
   CALL proc_r4g_kojin_jusho( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );
  
   CALL proc_r4g_mynumbr( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );
   
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
         ROLLBACK;
         RETURN;
END;
$$;
