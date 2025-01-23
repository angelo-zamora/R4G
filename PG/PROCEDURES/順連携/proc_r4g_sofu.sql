CREATE OR REPLACE PROCEDURE proc_r4g_sofu(
   in_n_renkei_data_cd IN numeric,
   in_n_renkei_seq IN numeric,
   in_n_shori_ymd IN numeric,
   io_c_result_code INOUT character varying,
   io_c_err_text INOUT character varying
)
LANGUAGE plpgsql
AS $$
DECLARE
   rec_f_sofu RECORD;

   rec_busho RECORD;
   rec_busho_list RECORD[];

   ln_shori_count numeric DEFAULT 0;
   ln_rec_count numeric DEFAULT 0;
   ln_renkei_rec_count numeric DEFAULT 0;
   ln_i numeric DEFAULT 0;
   ln_yusen_flg numeric DEFAULT 0;

   rec_main RECORD;

   rec_remban RECORD;
   
   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   cur_main CURSOR FOR
   SELECT DISTINCT ON (atena_no, gyomu_id) *
   FROM i_r4g_sofu_renrakusaki
   WHERE saishin_flg = '1'
      AND zeimoku_cd = '00'
      AND result_cd < 8
      AND (
            (yubin_no IS NOT NULL AND yubin_no != '') 
            OR (jusho IS NOT NULL AND jusho != '')
      )
      ORDER BY atena_no, gyomu_id, sofu_rireki_no DESC;

   rec_main            i_r4g_sofu_renrakusaki%ROWTYPE;

   cur_busho CURSOR FOR
     SELECT busho_cd,
         busho,
         denwa_no,
         shori_kengen_cd,
         zeimoku_kengen_flg,
         busho_etsuran_kengen_flg,
         schema,
         sort_no,
         ins_datetime,
         upd_datetime,
         upd_tantosha_cd,
         upd_tammatsu,
         del_flg
     FROM t_busho
     WHERE del_flg = 0;

   cur_remban CURSOR (p_busho_cd character varying, p_kojin_no character varying) IS
     SELECT COALESCE(SUM(CASE WHEN del_flg = 0 THEN 1 ELSE 0 END), 0) AS yuko_count,
         COALESCE(SUM(renkei_flg), 0) AS renkei_count,
         COALESCE(CASE WHEN SUM(renkei_flg) > 0 THEN MAX(renkei_flg * remban) ELSE MAX(remban) + 1 END, 1) AS renkei_remban,
         COALESCE(MIN(CASE 
                  WHEN del_flg = 0 AND yusen_flg = 1 AND renkei_flg = 0 THEN 1
                  WHEN del_flg = 0 AND yusen_flg = 1 AND renkei_flg = 1 THEN 2
                  ELSE 9
                END), 9) AS yusen_kbn
     FROM f_sofu
     WHERE busho_cd = p_busho_cd
      AND kojin_no = p_kojin_no;
		  
BEGIN
   OPEN cur_parameter;
   LOOP
      FETCH cur_parameter INTO rec_parameter;
      EXIT WHEN NOT FOUND;
         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 6 THEN ln_para06 := rec_parameter.parameter_value; END IF;
   END LOOP;
   CLOSE cur_parameter;

   IF ln_para01 = 1 THEN
      lc_sql := 'TRUNCATE TABLE dlgmain.f_sofu;';
      EXECUTE lc_sql;
      RETURN;
   END IF;

   CALL proc_error_check(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_result_code, io_c_err_text);
   
   IF io_c_result_code <> '0' THEN
      RETURN;
   END IF;

   rec_busho_list := ARRAY(SELECT * FROM cur_busho);

   OPEN cur_main;
   LOOP
      FETCH cur_main INTO rec_main;
      EXIT WHEN NOT FOUND;
      ln_shori_count := ln_shori_count + 1;

      FOREACH rec_busho IN ARRAY rec_busho_list LOOP
       ln_yusen_flg := 0;
       rec_f_sofu := NULL;

       rec_f_sofu.busho_cd := rec_busho.busho_cd;
       rec_f_sofu.kojin_no := rec_main.kojin_no;

       OPEN cur_remban(rec_f_sofu.busho_cd, rec_f_sofu.kojin_no);
       FETCH cur_remban INTO rec_remban;
       CLOSE cur_remban;

       IF ln_para06 = 1 THEN
         ln_yusen_flg := 1;
       ELSIF rec_remban.yuko_count = 0 OR rec_remban.yusen_kbn = 9 THEN
         ln_yusen_flg := 1;
       ELSE
         BEGIN
            SELECT yusen_flg
            INTO ln_yusen_flg
            FROM f_sofu
            WHERE busho_cd = rec_f_sofu.busho_cd
             AND kojin_no = rec_f_sofu.kojin_no
             AND remban = rec_remban.renkei_remban
             AND del_flg = 0;
         EXCEPTION
            WHEN NOT FOUND THEN
              ln_yusen_flg := 0;
         END;
       END IF;

       rec_f_sofu.yusen_flg := ln_yusen_flg;

       IF ln_para06 = 1 OR rec_f_sofu.yusen_flg = 1 THEN
         UPDATE f_sofu
         SET yusen_flg = 0
         WHERE busho_cd = rec_f_sofu.busho_cd
           AND kojin_no = rec_f_sofu.kojin_no;
       END IF;

       rec_f_sofu.remban := rec_remban.renkei_remban;
       rec_f_sofu.sofu_sansho_kojin_no := NULL;
       rec_f_sofu.chosa_ymd := to_number(to_char(CURRENT_DATE, 'YYYYMMDD'));
       rec_f_sofu.sofu_chiku_cd := NULL;
       rec_f_sofu.sofu_jusho_cd := NULL;
       rec_f_sofu.sofu_yubin_no := rec_main.gen_yubin_no;
       rec_f_sofu.sofu_jusho := rec_main.gen_jusho;
       rec_f_sofu.sofu_jusho_mojisu := LENGTH(rec_f_sofu.sofu_jusho);
       rec_f_sofu.sofu_jusho_gaiji_flg := 0;
       rec_f_sofu.sofu_jusho_katagaki := rec_main.gen_jusho_katagaki;
       rec_f_sofu.sofu_jusho_katagaki_mojisu := LENGTH(rec_f_sofu.sofu_jusho_katagaki);
       rec_f_sofu.sofu_jusho_katagaki_gaiji_flg := 0;
       rec_f_sofu.sofu_shimei := rec_main.shimei;
       rec_f_sofu.sofu_shimei_mojisu := LENGTH(rec_f_sofu.sofu_shimei);
       rec_f_sofu.sofu_shimei_gaiji_flg := 0;
       rec_f_sofu.sofu_kaishi_ymd := 0;
       rec_f_sofu.sofu_shuryo_ymd := 99999999;
       rec_f_sofu.biko_sofu := '連携データ';
       rec_f_sofu.renkei_flg := 1;
       rec_f_sofu.ins_datetime := CURRENT_TIMESTAMP;
       rec_f_sofu.upd_datetime := CURRENT_TIMESTAMP;
       rec_f_sofu.upd_tantosha_cd := 'RENKEI';
       rec_f_sofu.upd_tammatsu := 'SERVER';
       rec_f_sofu.del_flg := 0;

       INSERT INTO f_sofu (
		 busho_cd
		 , kojin_no
		 , gyomu_cd
		 , remban
		 , sofu_sansho_kojin_no
		 , yusen_flg
		 , chosa_ymd
		 , sofu_chiku_cd
		 , sofu_jusho_cd
		 , sofu_yubin_no
		 , sofu_jusho
		 , sofu_jusho_katagaki
		 , nyuryoku_kbn
		 , shikuchoson_cd
		 , machiaza_cd
		 , todofuken
		 , shikugunchoson
		 , machiaza
		 , banchigohyoki
		 , kokumei_cd
		 , kokumeito
		 , kokugai_jusho
		 , sofu_shimei
		 , sofu_shimei_kana
		 , sofu_kbn
		 , sofu_setti_riyu
		 , renrakusaki_kbn
		 , denwa_no
		 , sofu_kaishi_ymd
		 , sofu_shuryo_ymd
		 , biko_sofu
		 , renkei_flg
		 , sofurireki_no
		 , ins_datetime
		 , upd_datetime
		 , upd_tantosha_cd
		 , upd_tammatsu
		 , del_flg

       )
       VALUES (
         rec_f_sofu.busho_cd
		 , rec_f_sofu.kojin_no
		 , rec_f_sofu.gyomu_cd
		 , rec_f_sofu.remban
		 , rec_f_sofu.sofu_sansho_kojin_no
		 , rec_f_sofu.yusen_flg
		 , rec_f_sofu.chosa_ymd
		 , rec_f_sofu.sofu_chiku_cd
		 , rec_f_sofu.sofu_jusho_cd
		 , rec_f_sofu.sofu_yubin_no
		 , rec_f_sofu.sofu_jusho
		 , rec_f_sofu.sofu_jusho_katagaki
		 , rec_f_sofu.nyuryoku_kbn
		 , rec_f_sofu.shikuchoson_cd
		 , rec_f_sofu.machiaza_cd
		 , rec_f_sofu.todofuken
		 , rec_f_sofu.shikugunchoson
		 , rec_f_sofu.machiaza
		 , rec_f_sofu.banchigohyoki
		 , rec_f_sofu.kokumei_cd
		 , rec_f_sofu.kokumeito
		 , rec_f_sofu.kokugai_jusho
		 , rec_f_sofu.sofu_shimei
		 , rec_f_sofu.sofu_shimei_kana
		 , rec_f_sofu.sofu_kbn
		 , rec_f_sofu.sofu_setti_riyu
		 , rec_f_sofu.renrakusaki_kbn
		 , rec_f_sofu.denwa_no
		 , rec_f_sofu.sofu_kaishi_ymd
		 , rec_f_sofu.sofu_shuryo_ymd
		 , rec_f_sofu.biko_sofu
		 , rec_f_sofu.renkei_flg
		 , rec_f_sofu.sofurireki_no
		 , rec_f_sofu.ins_datetime
		 , rec_f_sofu.upd_datetime
		 , rec_f_sofu.upd_tantosha_cd
		 , rec_f_sofu.upd_tammatsu
		 , rec_f_sofu.del_flg
       )
       ON CONFLICT (busho_cd, kojin_no, remban) DO UPDATE
       SET
         sofu_yubin_no = EXCLUDED.sofu_yubin_no,
         sofu_jusho = EXCLUDED.sofu_jusho,
         sofu_jusho_mojisu = LENGTH(EXCLUDED.sofu_jusho),
         sofu_jusho_katagaki = EXCLUDED.sofu_jusho_katagaki,
         sofu_jusho_katagaki_mojisu = LENGTH(EXCLUDED.sofu_jusho_katagaki),
         sofu_shimei = EXCLUDED.sofu_shimei,
         sofu_shimei_mojisu = LENGTH(EXCLUDED.sofu_shimei),
         upd_datetime = CURRENT_TIMESTAMP,
         upd_tantosha_cd = 'RENKEI',
         upd_tammatsu = 'SERVER',
         del_flg = 0,
         yusen_flg = EXCLUDED.yusen_flg;

       IF ln_shori_count % 10000 = 0 THEN
         COMMIT;
       END IF;
     END LOOP;
   END LOOP;

   CLOSE cur_main;

   CALL proc_kojin_sofu_chofuku_upd(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_result_code, io_c_err_text);
   CALL proc_r4g_sofu_zeimoku(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_result_code, io_c_err_text);
   CALL proc_r4g_denwa(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_result_code, io_c_err_text);

EXCEPTION
   WHEN OTHERS THEN
   io_c_result_code := SQLSTATE;
   io_c_err_text := SQLERRM;
   ROLLBACK;
   RETURN;
END;
$$;
