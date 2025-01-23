--------------------------------------------------------
--  DDL for Procedure  proc_r4g_taino
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_taino ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, io_c_err_code INOUT character varying, io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 調定収納情報（統合収滞納）                                                                              */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/

DECLARE
   rec_f_taino                    f_taino%ROWTYPE;
   rec_log                        f_renkei_log%ROWTYPE;

   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;
   ln_hotei_noki_to_ymd           numeric DEFAULT 0;
   ln_hotei_noki_ymd              numeric DEFAULT 0;

   ln_para01                      numeric DEFAULT 0;
   ln_para02                      numeric DEFAULT 0;
   ln_para15                      numeric DEFAULT 0;
   ln_para16                      numeric DEFAULT 0;
   
   row_cnt                      numeric DEFAULT 0;

   lc_sql                         character varying(1000);

   ln_tsuchisho_no_length         type_tsuchisho_no_length[];
   ln_kojin_no_length             numeric DEFAULT 15;

   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_shuno
   WHERE result_cd < 8;

   rec_main                       i_r4g_shuno%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;

   cur_data_kanri_kibetsu CURSOR FOR
   SELECT *
   FROM f_data_kanri_kibetsu;

   rec_data_kanri_kibetsu         f_data_kanri_kibetsu%ROWTYPE;
   
   cur_lock CURSOR FOR
   SELECT *
   FROM f_taino
   WHERE kibetsu_key = rec_f_taino.kibetsu_key;

   rec_lock                       f_taino%ROWTYPE;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   -- １．パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 15 THEN ln_para15 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 16 THEN ln_para16 := rec_parameter.parameter_value; END IF;
      END LOOP;
   CLOSE cur_parameter;

   -- ２．連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_taino;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_taino';
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
   
   IF ln_para02 = 1 THEN
      CALL proc_taino_drop_index();
   END IF;
   
   -- ４．桁数設定情報取得
   -- r4gでは不要
   /*
   BEGIN
      SELECT kojin_no_length
      INTO ln_kojin_no_length
      FROM f_data_kanri_kojin
      WHERE data_kanri_no = 1;

      IF ln_kojin_no_length IS NULL OR ln_kojin_no_length = 0 OR ln_kojin_no_length > 15 THEN
         ln_kojin_no_length := 15;
      END IF;

   EXCEPTION
      WHEN OTHERS THEN
         ln_kojin_no_length := 15;
   END;

   BEGIN
      OPEN cur_data_kanri_kibetsu;
         LOOP
            FETCH cur_data_kanri_kibetsu INTO rec_data_kanri_kibetsu;
            EXIT WHEN NOT FOUND;

            ln_tsuchisho_no_length[rec_data_kanri_kibetsu.zeimoku_cd] := rec_data_kanri_kibetsu.tsuchisho_no_length;
         END LOOP;
      CLOSE cur_data_kanri_kibetsu;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;
   */

   -- ５．連携データの作成・更新
   ln_shori_count := 0;
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;
		 
         IF rec_main.zeimoku_cd <> '04' AND rec_f_taino.zeigaku > 0 AND (rec_f_taino.noki_ymd IS NULL OR rec_f_taino.noki_ymd = 0) THEN
            ln_err_count := ln_err_count + 1;
            lc_err_text := '納期エラー';
            lc_err_cd := '9';
            ln_result_cd := 9;
         ELSE
            ln_shori_count                 := ln_shori_count + 1;
            lc_err_cd                      := '0';
            lc_err_text                    := '';
            ln_result_cd                   := 0;
            rec_f_taino                    := NULL;
            rec_lock                       := NULL;

            CALL proc_taino_key_columns(rec_main, rec_f_taino);
            rec_f_taino.jigyo_kaishi_ymd := CASE WHEN rec_main.jigyo_kaishi_ymd IS NOT NULL THEN get_date_to_num(rec_main.jigyo_kaishi_ymd::date) ELSE 0 END;
            rec_f_taino.jigyo_shuryo_ymd := CASE WHEN rec_main.jigyo_shuryo_ymd IS NOT NULL THEN get_date_to_num(rec_main.jigyo_shuryo_ymd::date) ELSE 0 END;
			rec_f_taino.shinkoku_cd := CASE WHEN rec_main.shinkoku_cd IS NOT NULL OR rec_main.shinkoku_cd <> '' THEN rec_main.shinkoku_cd::numeric ELSE 0 END;
            rec_f_taino.shusei_kaisu := 0;
            rec_f_taino.kasankin_cd := 0;
            rec_f_taino.tokucho_shitei_no := rec_main.tokucho_shitei_no;
            rec_f_taino.shinkoku_rireki_no := rec_main.shinkoku_rireki_no;
            rec_f_taino.jigyo_nendo_no := rec_main.jigyo_nendo_no;
            rec_f_taino.jido_kojin_no := rec_main.jido_atena_no;
			rec_f_taino.noki_ymd := CASE WHEN rec_main.noki_ymd IS NOT NULL THEN get_date_to_num(rec_main.noki_ymd::date) ELSE 0 END;
			rec_f_taino.noki_kuriage_ymd := 0;
            rec_f_taino.shitei_noki_ymd := CASE WHEN rec_main.shitei_noki_ymd IS NOT NULL THEN get_date_to_num(rec_main.shitei_noki_ymd::date) ELSE 0 END;
			rec_f_taino.tokusoku_ymd := 0;
            rec_f_taino.tokusoku_henrei_ymd := 0;
            rec_f_taino.tokusoku_koji_ymd := 0;
            rec_f_taino.tokusoku_noki_ymd := 0;
            rec_f_taino.saikoku_ymd := 0;
            rec_f_taino.saikoku_noki_ymd := 0;
			ln_hotei_noki_to_ymd := CASE WHEN rec_main.hotei_noki_to_ymd IS NOT NULL THEN get_date_to_num(rec_main.hotei_noki_to_ymd::date) ELSE 0 END;
            ln_hotei_noki_ymd := CASE WHEN rec_main.hotei_noki_ymd IS NOT NULL THEN get_date_to_num(rec_main.hotei_noki_ymd::date) ELSE 0 END;
			rec_f_taino.kisan_ymd := 0;
            rec_f_taino.kazei_kosei_ymd := CASE WHEN rec_main.kazei_kosei_ymd IS NOT NULL THEN get_date_to_num(rec_main.kazei_kosei_ymd::date) ELSE 0 END;
			rec_f_taino.kosei_jiyu_cd := CASE WHEN rec_main.kosei_jiyu_cd IS NOT NULL OR rec_main.kosei_jiyu_cd <> '' THEN rec_main.kosei_jiyu_cd::numeric ELSE 0 END;
            rec_f_taino.shinkoku_ymd := CASE WHEN rec_main.shinkoku_ymd IS NOT NULL THEN get_date_to_num(rec_main.shinkoku_ymd::date) ELSE 0 END;
			rec_f_taino.shusei_shinkoku_ymd := CASE WHEN rec_main.shusei_shinkoku_ymd IS NOT NULL THEN get_date_to_num(rec_main.shusei_shinkoku_ymd::date) ELSE 0 END;
			rec_f_taino.kakutei_shinkoku_ymd := CASE WHEN rec_main.kakutei_shinkoku_ymd IS NOT NULL THEN get_date_to_num(rec_main.kakutei_shinkoku_ymd::date) ELSE 0 END;
			rec_f_taino.kosei_kettei_tsuchi_ymd := CASE WHEN rec_main.kosei_kettei_tsuchi_ymd IS NOT NULL THEN get_date_to_num(rec_main.kosei_kettei_tsuchi_ymd::date) ELSE 0 END;
            rec_f_taino.encho_tsuki := CASE WHEN rec_main.shinkoku_kigen_encho IS NOT NULL THEN rec_main.shinkoku_kigen_encho::numeric ELSE 0 END;
            rec_f_taino.shinkoku_kigen_ymd := CASE WHEN rec_main.shinkoku_kigen IS NOT NULL THEN get_date_to_num(rec_main.shinkoku_kigen::date) ELSE 0 END;
            rec_f_taino.encho_kigen_ymd := CASE WHEN rec_main.encho_shinkoku_kigen IS NOT NULL THEN get_date_to_num(rec_main.encho_shinkoku_kigen::date) ELSE 0 END;
            rec_f_taino.kosei_seikyu_ymd := CASE WHEN rec_main.kosei_seikyu_ymd IS NOT NULL THEN get_date_to_num(rec_main.kosei_seikyu_ymd::date) ELSE 0 END;
            rec_f_taino.kokuzei_shinkoku_kiso_kbn := CASE WHEN rec_main.kokuzei_shinkoku_kbn IS NOT NULL OR rec_main.kokuzei_shinkoku_kbn <> '' THEN rec_main.kokuzei_shinkoku_kbn::numeric ELSE 0 END;
            rec_f_taino.kokuzei_shinkoku_ymd := CASE WHEN rec_main.kokuzei_shinkoku_ymd IS NOT NULL THEN get_date_to_num(rec_main.kokuzei_shinkoku_ymd::date) ELSE 0 END;
            rec_f_taino.kosei_shinkoku_ymd := LEAST(rec_f_taino.kosei_seikyu_ymd, rec_f_taino.kokuzei_shinkoku_ymd);
            rec_f_taino.jiko_yotei_ymd := 0;
            rec_f_taino.shometsu_yotei_ymd := 0;
            rec_f_taino.zeigaku := CASE WHEN rec_main.zeigaku IS NOT NULL OR rec_main.zeigaku <> '' THEN rec_main.zeigaku::numeric ELSE 0 END;
            rec_f_taino.tokusoku := CASE WHEN rec_main.tokusoku IS NOT NULL OR rec_main.tokusoku <> '' THEN rec_main.tokusoku::numeric ELSE 0 END;
            rec_f_taino.entaikin := CASE WHEN rec_main.entaikin IS NOT NULL OR rec_main.entaikin <> '' THEN rec_main.entaikin::numeric ELSE 0 END;
            rec_f_taino.entaikin_kakutei_cd := CASE WHEN rec_f_taino.entaikin = 0 THEN 0 ELSE 1 END;
            rec_f_taino.entaikin_kyosei_kbn := CASE WHEN rec_main.entaikin_kyosei_kbn IS NOT NULL OR rec_main.entaikin_kyosei_kbn <> '' THEN rec_main.entaikin_kyosei_kbn::numeric ELSE 0 END;
            rec_f_taino.entaikin_kyosei_ymd := CASE WHEN rec_main.entaikin_kyosei_ymd IS NOT NULL THEN get_date_to_num(rec_main.entaikin_kyosei_ymd::date) ELSE 0 END;
            rec_f_taino.zeigaku_kintowari := CASE WHEN rec_main.zeigaku_kintowari IS NOT NULL OR rec_main.zeigaku_kintowari <> '' THEN rec_main.zeigaku_kintowari::numeric ELSE 0 END;
            rec_f_taino.zeigaku_hojinwari := CASE WHEN rec_main.zeigaku_hojinwari IS NOT NULL OR rec_main.zeigaku_hojinwari <> '' THEN rec_main.zeigaku_hojinwari::numeric ELSE 0 END;
            rec_f_taino.zeigaku_iryo_ippan := CASE WHEN rec_main.zeigaku_iryo_ippan IS NOT NULL OR rec_main.zeigaku_iryo_ippan <> '' THEN rec_main.zeigaku_iryo_ippan::numeric ELSE 0 END;
            rec_f_taino.zeigaku_iryo_taisyoku := CASE WHEN rec_main.zeigaku_iryo_taisyoku IS NOT NULL OR rec_main.zeigaku_iryo_taisyoku <> '' THEN rec_main.zeigaku_iryo_taisyoku::numeric ELSE 0 END;
            rec_f_taino.zeigaku_kaigo_ippan := CASE WHEN rec_main.zeigaku_kaigo_ippan IS NOT NULL OR rec_main.zeigaku_kaigo_ippan <> '' THEN rec_main.zeigaku_kaigo_ippan::numeric ELSE 0 END;
            rec_f_taino.zeigaku_kaigo_taisyoku := CASE WHEN rec_main.zeigaku_kaigo_taisyoku IS NOT NULL OR rec_main.zeigaku_kaigo_taisyoku <> '' THEN rec_main.zeigaku_kaigo_taisyoku::numeric ELSE 0 END;
            rec_f_taino.zeigaku_shien_ippan := CASE WHEN rec_main.zeigaku_shien_ippan IS NOT NULL OR rec_main.zeigaku_shien_ippan <> '' THEN rec_main.zeigaku_shien_ippan::numeric ELSE 0 END;
            rec_f_taino.zeigaku_shien_taisyoku := CASE WHEN rec_main.zeigaku_shien_taisyoku IS NOT NULL OR rec_main.zeigaku_shien_taisyoku <> '' THEN rec_main.zeigaku_shien_taisyoku::numeric ELSE 0 END;
            rec_f_taino.zeigaku_shuno := CASE WHEN rec_main.zeigaku_shuno IS NOT NULL OR rec_main.zeigaku_shuno <> '' THEN rec_main.zeigaku_shuno::numeric ELSE 0 END + 
			                                CASE WHEN rec_main.zeigaku_karikeshi IS NOT NULL OR rec_main.zeigaku_karikeshi <> '' THEN rec_main.zeigaku_karikeshi::numeric ELSE 0 END;
            rec_f_taino.tokusoku_shuno := CASE WHEN rec_main.tokusoku_shuno IS NOT NULL OR rec_main.tokusoku_shuno <> '' THEN rec_main.tokusoku_shuno::numeric ELSE 0 END + 
			                                CASE WHEN rec_main.tokusoku_karikeshi IS NOT NULL OR rec_main.tokusoku_karikeshi <> '' THEN rec_main.tokusoku_karikeshi::numeric ELSE 0 END;
            rec_f_taino.entaikin_shuno := CASE WHEN rec_main.entaikin_shuno IS NOT NULL OR rec_main.entaikin_shuno <> '' THEN rec_main.entaikin_shuno::numeric ELSE 0 END + 
			                                CASE WHEN rec_main.entaikin_karikeshi IS NOT NULL OR rec_main.entaikin_karikeshi <> '' THEN rec_main.entaikin_karikeshi::numeric ELSE 0 END;
            rec_f_taino.zeigaku_kintowari_shuno := CASE WHEN rec_main.zeigaku_kintowari_shuno IS NOT NULL OR rec_main.zeigaku_kintowari_shuno <> '' THEN rec_main.zeigaku_kintowari_shuno::numeric ELSE 0 END;
            rec_f_taino.zeigaku_hojinwari_shuno := CASE WHEN rec_main.zeigaku_hojinwari_shuno IS NOT NULL OR rec_main.zeigaku_hojinwari_shuno <> '' THEN rec_main.zeigaku_hojinwari_shuno::numeric ELSE 0 END;
            rec_f_taino.saishu_nikkei_ymd := CASE WHEN rec_main.shunyu_ymd IS NOT NULL THEN get_date_to_num(rec_main.shunyu_ymd::date) ELSE 0 END;
            rec_f_taino.saishu_shuno_ymd := CASE WHEN rec_main.ryoshu_ymd IS NOT NULL THEN get_date_to_num(rec_main.ryoshu_ymd::date) ELSE 0 END;
            rec_f_taino.saishu_shuno_kingaku := 0;
            rec_f_taino.kanno_cd :=  get_taino_kanno_cd(rec_f_taino);
            rec_f_taino.kanno_ymd := 0;
            rec_f_taino.zeigaku_mino := rec_f_taino.zeigaku - rec_f_taino.zeigaku_shuno;
            rec_f_taino.tokusoku_mino := rec_f_taino.tokusoku - rec_f_taino.tokusoku_shuno;
            rec_f_taino.entaikin_mino := CASE WHEN rec_f_taino.entaikin_kakutei_cd = 0 THEN 0 ELSE rec_f_taino.entaikin - rec_f_taino.entaikin_shuno END;
            rec_f_taino.shotokuwari := 0;
            rec_f_taino.fukakachiwari := 0;
            rec_f_taino.shihonwari := 0;
            rec_f_taino.shunyuwari := 0;
            rec_f_taino.tosho_kazeigaku := 0;
            rec_f_taino.jukasankin_taisho_zeigaku := 0;
			
            CALL proc_taino_kanrinin(rec_f_taino);

            rec_f_taino.shobun_kano_ymd := 0;
            rec_f_taino.noki_torai_handan_ymd := 0;
            rec_f_taino.kaikei_nendo := 0;
            rec_f_taino.kobetsu_komoku1 := get_kobetsu_komoku1(rec_main);
            rec_f_taino.kobetsu_komoku2 := null;
            rec_f_taino.kobetsu_komoku3 := null;
            rec_f_taino.yobi_komoku1 := null;
            rec_f_taino.yobi_komoku2 := null;
            rec_f_taino.yobi_komoku3 := null;
            rec_f_taino.hihokensha_no := rec_main.hihokensha_no;
            rec_f_taino.kokuhokigo_no := rec_main.kokuhokigo_no;
            rec_f_taino.kyoyu_shisan_no := rec_main.kyoyu_shisan_no;
            rec_f_taino.shizei_jimusho_cd := rec_main.shizei_jimusho_cd;
            rec_f_taino.tsuchi_ymd := 0;
            rec_f_taino.koseiin_tokusoku_flg := rec_main.koseiin_tokusoku_flg;
            rec_f_taino.zeigaku_kotei_tochikaoku := rec_main.zeigaku_kotei_tochikaoku;
            rec_f_taino.zeigaku_kotei_shokyaku := rec_main.zeigaku_kotei_shokyaku;
            rec_f_taino.zeigaku_shinrin := rec_main.zeigaku_shinrin;
            rec_f_taino.shotokuwari_kojo := rec_main.haitowari_shotokuwari_kojo;
            rec_f_taino.shotokuwari_kanpu := rec_main.haitowari_shotokuwari_kanpu;
            rec_f_taino.kojo_fusoku := rec_main.kojo_fusoku;
            rec_f_taino.kojo_fusoku_nofu := rec_main.juto_itaku_nofu;
            rec_f_taino.noki_tokurei_flg := CASE WHEN rec_main.noki_tokurei_kbn IS NOT NULL OR rec_main.noki_tokurei_kbn <> '' THEN rec_main.noki_tokurei_kbn::numeric ELSE 0 END;
            rec_f_taino.noki_tokurei_ym := rec_main.noki_tokurei_ym;
            rec_f_taino.kazei_kbn := rec_main.kazei_kbn;
            rec_f_taino.keiji_kanri_no := rec_main.keiji_kanri_no;
            rec_f_taino.shadai_no := rec_main.shadai_no;
            rec_f_taino.keiji_shubetsu_cd := rec_main.keiji_shubetsu_cd;
            rec_f_taino.sharyo_no1 := rec_main.sharyo_no1;
            rec_f_taino.sharyo_no2 := rec_main.sharyo_no2;
            rec_f_taino.sharyo_no3 := rec_main.sharyo_no3;
            rec_f_taino.sharyo_no4 := rec_main.sharyo_no4;
            rec_f_taino.shomeisho_yuko_kigen := rec_main.shomeisho_yuko_kigen;
            rec_f_taino.jukazei_flg := rec_main.jukasanzei_flg;
            rec_f_taino.kesson_ymd := CASE WHEN rec_main.kesson_ymd IS NOT NULL THEN get_date_to_num(rec_main.kesson_ymd::date) ELSE 0 END;
            rec_f_taino.kesson_jiyu_cd := rec_main.kesson_jiyu_cd;
            rec_f_taino.zeigaku_kesson := rec_main.zeigaku_kesson;
            rec_f_taino.entaikin_kesson := rec_main.entaikin_kesson;
            rec_f_taino.tokusoku_kesson := rec_main.tokusoku_kesson;
            rec_f_taino.kodomo_jigyosho_no := rec_main.kodomo_jigyosho_no;
            rec_f_taino.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            rec_f_taino.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            rec_f_taino.upd_tantosha_cd := rec_main.sosasha_cd;
            rec_f_taino.upd_tammatsu := 'SERVER';
            rec_f_taino.del_flg := CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> '' THEN rec_main.del_flg::numeric ELSE 0 END;

			OPEN cur_lock;
               FETCH cur_lock INTO rec_lock;
			CLOSE cur_lock;
			
            IF rec_f_taino.del_flg = 1 THEN
               BEGIN
               DELETE FROM f_taino
                     WHERE kibetsu_key = rec_f_taino.kibetsu_key;

                     GET DIAGNOSTICS row_cnt = ROW_COUNT;
					 ln_del_count := ln_del_count + row_cnt;
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
                  INSERT INTO f_taino(
                           kibetsu_key
                           , fuka_nendo
                           , soto_nendo
                           , zeimoku_cd
                           , kibetsu_cd
                           , kibetsu
                           , kojin_no
                           , tsuchisho_no
                           , jigyo_kaishi_ymd
                           , jigyo_shuryo_ymd
                           , shinkoku_cd
                           , shusei_kaisu
                           , nendo_kbn
                           , kankatsu_cd
                           , kasankin_cd
                           , tokucho_shitei_no
                           , shinkoku_rireki_no
                           , jigyo_nendo_no
                           , jido_kojin_no
                           , noki_ymd
                           , noki_kuriage_ymd
                           , shitei_noki_ymd
                           , tokusoku_ymd
                           , tokusoku_henrei_ymd
                           , tokusoku_koji_ymd
                           , tokusoku_noki_ymd
                           , saikoku_ymd
                           , saikoku_noki_ymd
                           , hotei_noki_to_ymd
                           , hotei_noki_ymd
                           , kisan_ymd
                           , kazei_kosei_ymd
                           , kosei_jiyu_cd
                           , shinkoku_ymd
                           , shusei_shinkoku_ymd
                           , kakutei_shinkoku_ymd
                           , kosei_kettei_tsuchi_ymd
                           , encho_tsuki
                           , shinkoku_kigen_ymd
                           , encho_kigen_ymd
                           , kosei_seikyu_ymd
                           , kokuzei_shinkoku_kiso_kbn
                           , kokuzei_shinkoku_ymd
                           , kosei_shinkoku_ymd
                           , jiko_yotei_ymd
                           , shometsu_yotei_ymd
                           , zeigaku
                           , tokusoku
                           , entaikin
                           , entaikin_kakutei_cd
                           , entaikin_kyosei_kbn
                           , entaikin_kyosei_ymd
                           , zeigaku_kintowari
                           , zeigaku_hojinwari
                           , zeigaku_iryo_ippan
                           , zeigaku_iryo_taisyoku
                           , zeigaku_kaigo_ippan
                           , zeigaku_kaigo_taisyoku
                           , zeigaku_shien_ippan
                           , zeigaku_shien_taisyoku
                           , zeigaku_shuno
                           , tokusoku_shuno
                           , entaikin_shuno
                           , zeigaku_kintowari_shuno
                           , zeigaku_hojinwari_shuno
                           , saishu_nikkei_ymd
                           , saishu_shuno_ymd
                           , saishu_shuno_kingaku
                           , kanno_cd
                           , kanno_ymd
                           , zeigaku_mino
                           , tokusoku_mino
                           , entaikin_mino
                           , shotokuwari
                           , fukakachiwari
                           , shihonwari
                           , shunyuwari
                           , tosho_kazeigaku
                           , jukasankin_taisho_zeigaku
                           , kanrinin_cd
                           , kanrinin_kojin_no
                           , shobun_kano_ymd
                           , noki_torai_handan_ymd
                           , kaikei_nendo
                           , kobetsu_komoku1
                           , kobetsu_komoku2
                           , kobetsu_komoku3
                           , yobi_komoku1
                           , yobi_komoku2
                           , yobi_komoku3
                           , hihokensha_no
                           , kokuhokigo_no
                           , kyoyu_shisan_no
                           , shizei_jimusho_cd
                           , tsuchi_ymd
                           , koseiin_tokusoku_flg
                           , zeigaku_kotei_tochikaoku
                           , zeigaku_kotei_shokyaku
                           , zeigaku_shinrin
                           , shotokuwari_kojo
                           , shotokuwari_kanpu
                           , kojo_fusoku
                           , kojo_fusoku_nofu
                           , noki_tokurei_flg
                           , noki_tokurei_ym
                           , kazei_kbn
                           , keiji_kanri_no
                           , shadai_no
                           , keiji_shubetsu_cd
                           , sharyo_no1
                           , sharyo_no2
                           , sharyo_no3
                           , sharyo_no4
                           , shomeisho_yuko_kigen
                           , jukazei_flg
                           , kesson_ymd
                           , kesson_jiyu_cd
                           , zeigaku_kesson
                           , entaikin_kesson
                           , tokusoku_kesson
                           , kodomo_jigyosho_no
                           , ins_datetime
                           , upd_datetime
                           , upd_tantosha_cd
                           , upd_tammatsu
                           , del_flg
                           )
                        VALUES (
                           rec_f_taino.kibetsu_key
                           , rec_f_taino.fuka_nendo
                           , rec_f_taino.soto_nendo
                           , rec_f_taino.zeimoku_cd
                           , rec_f_taino.kibetsu_cd
                           , rec_f_taino.kibetsu
                           , rec_f_taino.kojin_no
                           , rec_f_taino.tsuchisho_no
                           , rec_f_taino.jigyo_kaishi_ymd
                           , rec_f_taino.jigyo_shuryo_ymd
                           , rec_f_taino.shinkoku_cd
                           , rec_f_taino.shusei_kaisu
                           , rec_f_taino.nendo_kbn
                           , rec_f_taino.kankatsu_cd
                           , rec_f_taino.kasankin_cd
                           , rec_f_taino.tokucho_shitei_no
                           , rec_f_taino.shinkoku_rireki_no
                           , rec_f_taino.jigyo_nendo_no
                           , rec_f_taino.jido_kojin_no
                           , rec_f_taino.noki_ymd
                           , rec_f_taino.noki_kuriage_ymd
                           , rec_f_taino.shitei_noki_ymd
                           , rec_f_taino.tokusoku_ymd
                           , rec_f_taino.tokusoku_henrei_ymd
                           , rec_f_taino.tokusoku_koji_ymd
                           , rec_f_taino.tokusoku_noki_ymd
                           , rec_f_taino.saikoku_ymd
                           , rec_f_taino.saikoku_noki_ymd
                           , CASE WHEN ln_para15 = 0 OR ln_para15 = 2 THEN ln_hotei_noki_to_ymd ELSE 0 END
                           , CASE WHEN ln_para16 = 0 OR ln_para16 = 2 THEN ln_hotei_noki_ymd ELSE 0 END
                           , rec_f_taino.kisan_ymd
                           , rec_f_taino.kazei_kosei_ymd
                           , rec_f_taino.kosei_jiyu_cd
                           , rec_f_taino.shinkoku_ymd
                           , rec_f_taino.shusei_shinkoku_ymd
                           , rec_f_taino.kakutei_shinkoku_ymd
                           , rec_f_taino.kosei_kettei_tsuchi_ymd
                           , rec_f_taino.encho_tsuki
                           , rec_f_taino.shinkoku_kigen_ymd
                           , rec_f_taino.encho_kigen_ymd
                           , rec_f_taino.kosei_seikyu_ymd
                           , rec_f_taino.kokuzei_shinkoku_kiso_kbn
                           , rec_f_taino.kokuzei_shinkoku_ymd
                           , rec_f_taino.kosei_shinkoku_ymd
                           , rec_f_taino.jiko_yotei_ymd
                           , rec_f_taino.shometsu_yotei_ymd
                           , rec_f_taino.zeigaku
                           , rec_f_taino.tokusoku
                           , rec_f_taino.entaikin
                           , rec_f_taino.entaikin_kakutei_cd
                           , rec_f_taino.entaikin_kyosei_kbn
                           , rec_f_taino.entaikin_kyosei_ymd
                           , rec_f_taino.zeigaku_kintowari
                           , rec_f_taino.zeigaku_hojinwari
                           , rec_f_taino.zeigaku_iryo_ippan
                           , rec_f_taino.zeigaku_iryo_taisyoku
                           , rec_f_taino.zeigaku_kaigo_ippan
                           , rec_f_taino.zeigaku_kaigo_taisyoku
                           , rec_f_taino.zeigaku_shien_ippan
                           , rec_f_taino.zeigaku_shien_taisyoku
                           , rec_f_taino.zeigaku_shuno
                           , rec_f_taino.tokusoku_shuno
                           , rec_f_taino.entaikin_shuno
                           , rec_f_taino.zeigaku_kintowari_shuno
                           , rec_f_taino.zeigaku_hojinwari_shuno
                           , rec_f_taino.saishu_nikkei_ymd
                           , rec_f_taino.saishu_shuno_ymd
                           , rec_f_taino.saishu_shuno_kingaku
                           , rec_f_taino.kanno_cd
                           , rec_f_taino.kanno_ymd
                           , rec_f_taino.zeigaku_mino
                           , rec_f_taino.tokusoku_mino
                           , rec_f_taino.entaikin_mino
                           , rec_f_taino.shotokuwari
                           , rec_f_taino.fukakachiwari
                           , rec_f_taino.shihonwari
                           , rec_f_taino.shunyuwari
                           , rec_f_taino.tosho_kazeigaku
                           , rec_f_taino.jukasankin_taisho_zeigaku
                           , rec_f_taino.kanrinin_cd
                           , rec_f_taino.kanrinin_kojin_no
                           , rec_f_taino.shobun_kano_ymd
                           , rec_f_taino.noki_torai_handan_ymd
                           , rec_f_taino.kaikei_nendo
                           , rec_f_taino.kobetsu_komoku1
                           , rec_f_taino.kobetsu_komoku2
                           , rec_f_taino.kobetsu_komoku3
                           , rec_f_taino.yobi_komoku1
                           , rec_f_taino.yobi_komoku2
                           , rec_f_taino.yobi_komoku3
                           , rec_f_taino.hihokensha_no
                           , rec_f_taino.kokuhokigo_no
                           , rec_f_taino.kyoyu_shisan_no
                           , rec_f_taino.shizei_jimusho_cd
                           , rec_f_taino.tsuchi_ymd
                           , rec_f_taino.koseiin_tokusoku_flg
                           , rec_f_taino.zeigaku_kotei_tochikaoku
                           , rec_f_taino.zeigaku_kotei_shokyaku
                           , rec_f_taino.zeigaku_shinrin
                           , rec_f_taino.shotokuwari_kojo
                           , rec_f_taino.shotokuwari_kanpu
                           , rec_f_taino.kojo_fusoku
                           , rec_f_taino.kojo_fusoku_nofu
                           , rec_f_taino.noki_tokurei_flg
                           , rec_f_taino.noki_tokurei_ym
                           , rec_f_taino.kazei_kbn
                           , rec_f_taino.keiji_kanri_no
                           , rec_f_taino.shadai_no
                           , rec_f_taino.keiji_shubetsu_cd
                           , rec_f_taino.sharyo_no1
                           , rec_f_taino.sharyo_no2
                           , rec_f_taino.sharyo_no3
                           , rec_f_taino.sharyo_no4
                           , rec_f_taino.shomeisho_yuko_kigen
                           , rec_f_taino.jukazei_flg
                           , rec_f_taino.kesson_ymd
                           , rec_f_taino.kesson_jiyu_cd
                           , rec_f_taino.zeigaku_kesson
                           , rec_f_taino.entaikin_kesson
                           , rec_f_taino.tokusoku_kesson
                           , rec_f_taino.kodomo_jigyosho_no
                           , rec_f_taino.ins_datetime
                           , rec_f_taino.upd_datetime
                           , rec_f_taino.upd_tantosha_cd
                           , rec_f_taino.upd_tammatsu
                           , rec_f_taino.del_flg
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
                        SET kibetsu = rec_f_taino.kibetsu
                           , jigyo_shuryo_ymd = rec_f_taino.jigyo_shuryo_ymd
                           , noki_ymd = rec_f_taino.noki_ymd
                           , shitei_noki_ymd = rec_f_taino.shitei_noki_ymd
                           , hotei_noki_to_ymd = CASE ln_para15 WHEN 0 THEN ln_hotei_noki_to_ymd ELSE 0 END
                           , hotei_noki_ymd = CASE ln_para16 WHEN 0 THEN ln_hotei_noki_ymd END
                           , kazei_kosei_ymd = rec_f_taino.kazei_kosei_ymd
                           , kosei_jiyu_cd = rec_f_taino.kosei_jiyu_cd
                           , shinkoku_ymd = rec_f_taino.shinkoku_ymd
                           , shusei_shinkoku_ymd = rec_f_taino.shusei_shinkoku_ymd
                           , kakutei_shinkoku_ymd = rec_f_taino.kakutei_shinkoku_ymd
                           , kosei_kettei_tsuchi_ymd = rec_f_taino.kosei_kettei_tsuchi_ymd
                           , encho_tsuki = rec_f_taino.encho_tsuki
                           , shinkoku_kigen_ymd = rec_f_taino.shinkoku_kigen_ymd
                           , encho_kigen_ymd = rec_f_taino.encho_kigen_ymd
                           , kosei_seikyu_ymd = rec_f_taino.kosei_seikyu_ymd
                           , kokuzei_shinkoku_kiso_kbn = rec_f_taino.kokuzei_shinkoku_kiso_kbn
                           , kokuzei_shinkoku_ymd = rec_f_taino.kokuzei_shinkoku_ymd
                           , kosei_shinkoku_ymd = rec_f_taino.kosei_shinkoku_ymd
                           , zeigaku = rec_f_taino.zeigaku
                           , tokusoku = rec_f_taino.tokusoku
                           , entaikin = rec_f_taino.entaikin
                           , entaikin_kakutei_cd = rec_f_taino.entaikin_kakutei_cd
                           , entaikin_kyosei_kbn = rec_f_taino.entaikin_kyosei_kbn
                           , entaikin_kyosei_ymd = rec_f_taino.entaikin_kyosei_ymd
                           , zeigaku_kintowari = rec_f_taino.zeigaku_kintowari
                           , zeigaku_hojinwari = rec_f_taino.zeigaku_hojinwari
                           , zeigaku_iryo_ippan = rec_f_taino.zeigaku_iryo_ippan
                           , zeigaku_iryo_taisyoku = rec_f_taino.zeigaku_iryo_taisyoku
                           , zeigaku_kaigo_ippan = rec_f_taino.zeigaku_kaigo_ippan
                           , zeigaku_kaigo_taisyoku = rec_f_taino.zeigaku_kaigo_taisyoku
                           , zeigaku_shien_ippan = rec_f_taino.zeigaku_shien_ippan
                           , zeigaku_shien_taisyoku = rec_f_taino.zeigaku_shien_taisyoku
                           , zeigaku_shuno = rec_f_taino.zeigaku_shuno
                           , tokusoku_shuno = rec_f_taino.tokusoku_shuno
                           , entaikin_shuno = rec_f_taino.entaikin_shuno
                           , zeigaku_kintowari_shuno = rec_f_taino.zeigaku_kintowari_shuno
                           , zeigaku_hojinwari_shuno = rec_f_taino.zeigaku_hojinwari_shuno
                           , saishu_nikkei_ymd = rec_f_taino.saishu_nikkei_ymd
                           , saishu_shuno_ymd = rec_f_taino.saishu_shuno_ymd
                           , kanno_cd = rec_f_taino.kanno_cd
                           , zeigaku_mino = rec_f_taino.zeigaku_mino
                           , tokusoku_mino = rec_f_taino.tokusoku_mino
                           , entaikin_mino = rec_f_taino.entaikin_mino
                           , kanrinin_cd = rec_f_taino.kanrinin_cd
                           , kanrinin_kojin_no = rec_f_taino.kanrinin_kojin_no
                           , kobetsu_komoku1 = rec_f_taino.kobetsu_komoku1
                           , hihokensha_no = rec_f_taino.hihokensha_no
                           , kokuhokigo_no = rec_f_taino.kokuhokigo_no
                           , kyoyu_shisan_no = rec_f_taino.kyoyu_shisan_no
                           , shizei_jimusho_cd = rec_f_taino.shizei_jimusho_cd
                           , koseiin_tokusoku_flg = rec_f_taino.koseiin_tokusoku_flg
                           , zeigaku_kotei_tochikaoku = rec_f_taino.zeigaku_kotei_tochikaoku
                           , zeigaku_kotei_shokyaku = rec_f_taino.zeigaku_kotei_shokyaku
                           , zeigaku_shinrin = rec_f_taino.zeigaku_shinrin
                           , shotokuwari_kojo = rec_f_taino.shotokuwari_kojo
                           , shotokuwari_kanpu = rec_f_taino.shotokuwari_kanpu
                           , kojo_fusoku = rec_f_taino.kojo_fusoku
                           , kojo_fusoku_nofu = rec_f_taino.kojo_fusoku_nofu
                           , noki_tokurei_flg = rec_f_taino.noki_tokurei_flg
                           , noki_tokurei_ym = rec_f_taino.noki_tokurei_ym
                           , kazei_kbn = rec_f_taino.kazei_kbn
                           , keiji_kanri_no = rec_f_taino.keiji_kanri_no
                           , shadai_no = rec_f_taino.shadai_no
                           , keiji_shubetsu_cd = rec_f_taino.keiji_shubetsu_cd
                           , sharyo_no1 = rec_f_taino.sharyo_no1
                           , sharyo_no2 = rec_f_taino.sharyo_no2
                           , sharyo_no3 = rec_f_taino.sharyo_no3
                           , sharyo_no4 = rec_f_taino.sharyo_no4
                           , shomeisho_yuko_kigen = rec_f_taino.shomeisho_yuko_kigen
                           , jukazei_flg = rec_f_taino.jukazei_flg
                           , kesson_ymd = rec_f_taino.kesson_ymd
                           , kesson_jiyu_cd = rec_f_taino.kesson_jiyu_cd
                           , zeigaku_kesson = rec_f_taino.zeigaku_kesson
                           , entaikin_kesson = rec_f_taino.entaikin_kesson
                           , tokusoku_kesson = rec_f_taino.tokusoku_kesson
                           , kodomo_jigyosho_no = rec_f_taino.kodomo_jigyosho_no
                           , upd_datetime = rec_f_taino.upd_datetime
                           , upd_tantosha_cd = rec_f_taino.upd_tantosha_cd
                           , upd_tammatsu = rec_f_taino.upd_tammatsu
                        WHERE kibetsu_key = rec_f_taino.kibetsu_key;

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
         
		 BEGIN
		    -- 中間テーブル更新
            UPDATE i_r4g_shuno 
            SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND fuka_nendo = rec_main.fuka_nendo
               AND soto_nendo = rec_main.soto_nendo
               AND tsuchisho_no = rec_main.tsuchisho_no
               AND zeimoku_cd = rec_main.zeimoku_cd
               AND tokucho_shitei_no = rec_main.tokucho_shitei_no
               AND kibetsu_cd = rec_main.kibetsu_cd
               AND shinkoku_rireki_no = rec_main.shinkoku_rireki_no
               AND jigyo_nendo_no = rec_main.jigyo_nendo_no
               AND jido_atena_no = rec_main.jido_atena_no;
         EXCEPTION
            WHEN OTHERS THEN
                ln_err_count := ln_err_count + 1;
                lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                lc_err_cd := '9';
                ln_result_cd := 9;
         END;
      END LOOP;
   CLOSE cur_main;
   
   IF ln_para02 = 1 THEN
      CALL proc_taino_create_index();
   END IF;
   
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
