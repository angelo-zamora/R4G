CREATE OR REPLACE PROCEDURE proc_r4g_sofu_zeimoku(
   in_n_renkei_data_cd IN numeric,
   in_n_renkei_seq IN numeric,
   in_n_shori_ymd IN numeric,
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 税目毎送付先情報連携                                                                                    */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                  */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                      */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/

DECLARE
   rec_f_sofu_zeimoku f_sofu_zeimoku%ROWTYPE;
   
   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;
   rec_log                        f_renkei_log%ROWTYPE;

   ln_para01                      numeric DEFAULT 0;
   lc_kojin_no                    character varying;
   ln_gyomu_cd                    numeric;
   ln_zeimoku_cd                  numeric;
   lc_keiji_kanri_no              character varying;
   lc_sql                         character varying;
   ld_riyou_haishi_ymd            date;

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;

   cur_main CURSOR FOR
   SELECT DISTINCT ON (atena_no, gyomu_id, zeimoku_cd, keiji_kanri_no) *
   FROM i_r4g_sofu_renrakusaki
   WHERE 
      saishin_flg = '1'
      AND zeimoku_cd <> '00'
      AND (yubin_no IS NOT NULL AND yubin_no <> '' 
            OR jusho IS NOT NULL AND jusho <> '')
      AND result_cd < 8
   ORDER BY 
      atena_no, gyomu_id, zeimoku_cd, keiji_kanri_no, sofu_rireki_no DESC;

   rec_main                      i_r4g_sofu_renrakusaki%ROWTYPE;
    
   cur_lock CURSOR FOR
   SELECT *
   FROM f_sofu_zeimoku
   WHERE kojin_no = rec_f_sofu_zeimoku.kojin_no
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
   END LOOP;
   CLOSE cur_parameter;

   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_sofu_zeimoku;
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
         lc_err_cd                      := '0';
         ln_result_cd                   := 0;
         lc_err_text                    := NULL;
         rec_f_sofu_zeimoku             := NULL;

         rec_f_sofu_zeimoku.kojin_no := rec_main.atena_no;
         rec_f_sofu_zeimoku.gyomu_cd := CASE WHEN rec_main.gyomu_id IS NOT NULL OR rec_main.gyomu_id <> ''  THEN rec_main.gyomu_id::numeric ELSE 0 END;
         rec_f_sofu_zeimoku.zeimoku_cd := get_r4g_cd(rec_main.zeimoku_cd, '3');
         rec_f_sofu_zeimoku.keiji_kanri_no := CASE WHEN rec_main.keiji_kanri_no IS NULL OR rec_main.keiji_kanri_no = '' THEN '0' ELSE rec_main.keiji_kanri_no END;
         rec_f_sofu_zeimoku.sofu_shimei_kana := get_trimmed_space(rec_main.simei_meisho_katakana);
         rec_f_sofu_zeimoku.sofu_shimei := get_trimmed_space(rec_main.simei_meisho);
         rec_f_sofu_zeimoku.sofu_yubin_no := rec_main.yubin_no; -- TODO: get_hyoji_yubin_no(rec_main.yubin_no);
         rec_f_sofu_zeimoku.sofu_jusho := CASE WHEN (rec_main.jusho = '' OR rec_main.jusho IS NULL)
                                                THEN CONCAT(get_trimmed_space(rec_main.ken), get_trimmed_space(rec_main.shikuchoson), get_trimmed_space(rec_main.jusho_machi_cd), get_trimmed_space(rec_main.banchi))
                                                ELSE rec_main.jusho
                                                END;
         rec_f_sofu_zeimoku.sofu_jusho_katagaki := get_trimmed_space(rec_main.jusho_katagaki);
         rec_f_sofu_zeimoku.sofu_nyuryoku_kbn := CASE WHEN (rec_main.jusho = '' OR rec_main.jusho IS NULL)
                                                   THEN 0
                                                   ELSE 4
                                                   END;
         rec_f_sofu_zeimoku.sofu_shikuchoson_cd := rec_main.jusho_shikuchoson_cd;
         rec_f_sofu_zeimoku.sofu_machiaza_cd := rec_main.jusho_machi_cd;
         rec_f_sofu_zeimoku.sofu_todofuken := get_trimmed_space(rec_main.ken);
         rec_f_sofu_zeimoku.sofu_shikugunchoson := get_trimmed_space(rec_main.shikuchoson);
         rec_f_sofu_zeimoku.sofu_machiaza := get_trimmed_space(rec_main.machi);
         rec_f_sofu_zeimoku.sofu_banchigohyoki := get_trimmed_space(rec_main.banchi);
         rec_f_sofu_zeimoku.sofu_kokumei_cd := NULL;
         rec_f_sofu_zeimoku.sofu_kokumeito := NULL;
         rec_f_sofu_zeimoku.sofu_kokugai_jusho := NULL;
         rec_f_sofu_zeimoku.sofu_kbn := CASE WHEN rec_main.sofu_kbn IS NOT NULL OR rec_main.sofu_kbn <> ''  THEN rec_main.sofu_kbn::numeric ELSE 0 END;
         rec_f_sofu_zeimoku.sofu_setti_riyu := rec_main.sofu_setti_riyu;
         rec_f_sofu_zeimoku.renrakusaki_kbn := rec_main.renrakusaki_kbn;
         rec_f_sofu_zeimoku.denwa_no := rec_main.tel_no;
         rec_f_sofu_zeimoku.yukokigen_kaishi_ymd := get_date_to_num(to_date(rec_main.toroku_ymd, 'yyyy-mm-dd'));
         rec_f_sofu_zeimoku.yukokigen_shuryo_ymd := get_date_to_num(to_date(rec_main.riyou_haishi_ymd, 'yyyy-mm-dd'));
         rec_f_sofu_zeimoku.renkei_flg := 1;
         rec_f_sofu_zeimoku.sofurireki_no := rec_main.sofu_rireki_no;
         rec_f_sofu_zeimoku.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_f_sofu_zeimoku.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_f_sofu_zeimoku.upd_tantosha_cd := rec_main.sosasha_cd;
         rec_f_sofu_zeimoku.upd_tammatsu := 'SERVER';
         rec_f_sofu_zeimoku.del_flg := CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> ''  THEN rec_main.del_flg::numeric ELSE 0 END;

         ld_riyou_haishi_ymd := to_date(rec_main.riyou_haishi_ymd, 'yyyy-mm-dd');

         lc_kojin_no := rec_f_sofu_zeimoku.kojin_no;
         ln_gyomu_cd := rec_f_sofu_zeimoku.gyomu_cd;
         ln_zeimoku_cd  := rec_f_sofu_zeimoku.zeimoku_cd;
         lc_keiji_kanri_no := rec_f_sofu_zeimoku.keiji_kanri_no;
         
         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         IF rec_lock IS NULL THEN
            BEGIN
               INSERT INTO f_sofu_zeimoku(
                  kojin_no,
                  gyomu_cd,
                  zeimoku_cd,
                  keiji_kanri_no,
                  sofu_shimei_kana,
                  sofu_shimei,
                  sofu_yubin_no,
                  sofu_jusho,
                  sofu_jusho_katagaki,
                  sofu_nyuryoku_kbn,
                  sofu_shikuchoson_cd,
                  sofu_machiaza_cd,
                  sofu_todofuken,
                  sofu_shikugunchoson,
                  sofu_machiaza,
                  sofu_banchigohyoki,
                  sofu_kokumei_cd,
                  sofu_kokumeito,
                  sofu_kokugai_jusho,
                  sofu_kbn,
                  sofu_setti_riyu,
                  renrakusaki_kbn,
                  denwa_no,
                  yukokigen_kaishi_ymd,
                  yukokigen_shuryo_ymd,
                  renkei_flg,
                  sofurireki_no,
                  ins_datetime,
                  upd_datetime,
                  upd_tantosha_cd,
                  upd_tammatsu,
                  del_flg
               ) VALUES (
                  rec_f_sofu_zeimoku.kojin_no,
                  rec_f_sofu_zeimoku.gyomu_cd,
                  rec_f_sofu_zeimoku.zeimoku_cd,
                  rec_f_sofu_zeimoku.keiji_kanri_no,
                  rec_f_sofu_zeimoku.sofu_shimei_kana,
                  rec_f_sofu_zeimoku.sofu_shimei,
                  rec_f_sofu_zeimoku.sofu_yubin_no,
                  rec_f_sofu_zeimoku.sofu_jusho,
                  rec_f_sofu_zeimoku.sofu_jusho_katagaki,
                  rec_f_sofu_zeimoku.sofu_nyuryoku_kbn,
                  rec_f_sofu_zeimoku.sofu_shikuchoson_cd,
                  rec_f_sofu_zeimoku.sofu_machiaza_cd,
                  rec_f_sofu_zeimoku.sofu_todofuken,
                  rec_f_sofu_zeimoku.sofu_shikugunchoson,
                  rec_f_sofu_zeimoku.sofu_machiaza,
                  rec_f_sofu_zeimoku.sofu_banchigohyoki,
                  rec_f_sofu_zeimoku.sofu_kokumei_cd,
                  rec_f_sofu_zeimoku.sofu_kokumeito,
                  rec_f_sofu_zeimoku.sofu_kokugai_jusho,
                  rec_f_sofu_zeimoku.sofu_kbn,
                  rec_f_sofu_zeimoku.sofu_setti_riyu,
                  rec_f_sofu_zeimoku.renrakusaki_kbn,
                  rec_f_sofu_zeimoku.denwa_no,
                  rec_f_sofu_zeimoku.yukokigen_kaishi_ymd,
                  rec_f_sofu_zeimoku.yukokigen_shuryo_ymd,
                  rec_f_sofu_zeimoku.renkei_flg,
                  rec_f_sofu_zeimoku.sofurireki_no,
                  rec_f_sofu_zeimoku.ins_datetime,
                  rec_f_sofu_zeimoku.upd_datetime,
                  rec_f_sofu_zeimoku.upd_tantosha_cd,
                  rec_f_sofu_zeimoku.upd_tammatsu,
                  rec_f_sofu_zeimoku.del_flg
               );

               ln_ins_count := ln_ins_count + 1;
               lc_err_text := '';
               lc_err_cd := 0;
               ln_result_cd := 1;

               EXCEPTION
                  WHEN OTHERS THEN
                     ln_err_count := ln_err_count + 1;
                     lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                     lc_err_cd := 9;
                     ln_result_cd := 9;
            END;
         ELSE
            BEGIN
               UPDATE f_sofu_zeimoku
               SET 
                  sofu_shimei_kana = rec_f_sofu_zeimoku.sofu_shimei_kana,
                  sofu_shimei = rec_f_sofu_zeimoku.sofu_shimei,
                  sofu_yubin_no = rec_f_sofu_zeimoku.sofu_yubin_no,
                  sofu_jusho_katagaki = rec_f_sofu_zeimoku.sofu_jusho_katagaki,
                  sofu_nyuryoku_kbn = rec_f_sofu_zeimoku.sofu_nyuryoku_kbn,
                  sofu_shikuchoson_cd = rec_f_sofu_zeimoku.sofu_shikuchoson_cd,
                  sofu_machiaza_cd = rec_f_sofu_zeimoku.sofu_machiaza_cd,
                  sofu_todofuken = rec_f_sofu_zeimoku.sofu_todofuken,
                  sofu_shikugunchoson = rec_f_sofu_zeimoku.sofu_shikugunchoson,
                  sofu_machiaza = rec_f_sofu_zeimoku.sofu_machiaza,
                  sofu_banchigohyoki = rec_f_sofu_zeimoku.sofu_banchigohyoki,
                  sofu_kokumei_cd = rec_f_sofu_zeimoku.sofu_kokumei_cd,
                  sofu_kokumeito = rec_f_sofu_zeimoku.sofu_kokumeito,
                  sofu_kokugai_jusho = rec_f_sofu_zeimoku.sofu_kokugai_jusho,
                  sofu_kbn = rec_f_sofu_zeimoku.sofu_kbn,
                  sofu_setti_riyu = rec_f_sofu_zeimoku.sofu_setti_riyu,
                  renrakusaki_kbn = rec_f_sofu_zeimoku.renrakusaki_kbn,
                  denwa_no = rec_f_sofu_zeimoku.denwa_no,
                  yukokigen_kaishi_ymd = rec_f_sofu_zeimoku.yukokigen_kaishi_ymd,
                  yukokigen_shuryo_ymd = rec_f_sofu_zeimoku.yukokigen_shuryo_ymd,
                  renkei_flg = rec_f_sofu_zeimoku.renkei_flg,
                  sofurireki_no = rec_f_sofu_zeimoku.sofurireki_no,
                  upd_datetime = rec_f_sofu_zeimoku.upd_datetime,
                  upd_tantosha_cd = rec_f_sofu_zeimoku.upd_tantosha_cd,
                  upd_tammatsu = rec_f_sofu_zeimoku.upd_tammatsu,
                  del_flg = CASE WHEN (rec_f_sofu_zeimoku.del_flg = 1 AND ld_riyou_haishi_ymd < current_date) THEN 1 ELSE rec_f_sofu_zeimoku.del_flg END
               WHERE
                  kojin_no = rec_f_sofu_zeimoku.kojin_no
                  AND gyomu_cd = rec_f_sofu_zeimoku.gyomu_cd
                  AND zeimoku_cd = rec_f_sofu_zeimoku.zeimoku_cd 
                  AND keiji_kanri_no = rec_f_sofu_zeimoku.keiji_kanri_no;

               ln_upd_count := ln_upd_count + 1;
               lc_err_text := '';
               lc_err_cd := '0';
               ln_result_cd := 2;

            EXCEPTION
               WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
                  lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                  lc_err_cd := '9';
                  ln_result_cd := 9;
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
         
   CALL proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;

EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;

END;
$$; 