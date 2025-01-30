--------------------------------------------------------
--  DDL for Procedure proc_r4g_dairinin
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_dairinin ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, io_c_err_code INOUT character varying, io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 納税管理人情報連携                                                                                        */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                   */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                  */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                               */
/*      OUT : io_c_err_code   … 例外エラー発生時のエラーコード                                                       */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                         */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :                                                                                                          */
/**********************************************************************************************************************/
DECLARE

   ln_para01                           numeric DEFAULT 0;
   ln_zeimoku_cd                       numeric;
   ln_dairinin_yukokikan_kaishi_ymd    numeric;
   ln_dairinin_yukokikan_shuryo_ymd    numeric;
   
   lc_nozeigimusha_kojin_no            character varying;
   lc_dairinin_kojin_no                character varying;
   lc_gyomu_cd                         character varying;
   lc_zeimoku_cd                       character varying;
   lc_denwa                            character varying;
   lc_sql                              character varying(1000);
   
   ln_shori_count                      numeric DEFAULT 0;
   ln_ins_count                        numeric DEFAULT 0;
   ln_upd_count                        numeric DEFAULT 0;
   ln_del_count                        numeric DEFAULT 0;
   ln_err_count                        numeric DEFAULT 0;
   lc_err_text                         character varying(100);
   ln_result_cd                        numeric DEFAULT 0;
   lc_err_cd                           character varying;

   rec_log                             f_renkei_log%ROWTYPE;
   
   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                       f_renkei_parameter%ROWTYPE;
   
   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_dairinin
   WHERE saishin_flg = '1'
   AND katagaki <> '09'
   AND result_cd < 8;

   rec_main                            i_r4g_dairinin%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM f_dairinin
   WHERE nozeigimusha_kojin_no = lc_nozeigimusha_kojin_no
      AND dairinin_kojin_no = lc_dairinin_kojin_no
      AND gyomu_cd = lc_gyomu_cd
      AND zeimoku_cd = ln_zeimoku_cd
      AND dairinin_yukokikan_kaishi_ymd = ln_dairinin_yukokikan_kaishi_ymd;
   
   rec_lock                            f_dairinin%ROWTYPE;
   
BEGIN
   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;
   ln_err_count = 0;

   CALL proc_error_check( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );
   
   IF io_c_err_code <> '0'  THEN
      RETURN;
   END IF;

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
         UPDATE f_taino SET kanrinin_kojin_no = 0 where kanrinin_cd <> 0 and del_flg = 0;
         SELECT COUNT(*) INTO ln_del_count FROM f_dairinin;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_dairinin';
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

         lc_nozeigimusha_kojin_no := rec_main.atena_no;
         lc_dairinin_kojin_no := rec_main.dairinin_atena_no;
         lc_gyomu_cd := rec_main.gyomu_id;
         ln_zeimoku_cd := get_r4g_code_conv(1, '3', null, rec_main.zeimoku_cd::character varying);
         ln_dairinin_yukokikan_kaishi_ymd := getdatetonum(to_date(rec_main.dairinin_yukokikan_kaishi_ymd, 'yyyy-mm-dd'));
         ln_dairinin_yukokikan_shuryo_ymd := getdatetonum(to_date(rec_main.dairinin_yukokikan_shuryo_ymd, 'yyyy-mm-dd'));
         lc_denwa := get_trimmed_space(rec_main.denwa_no);

         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         IF rec_lock IS NULL THEN
            BEGIN
               INSERT INTO f_dairinin(
                  nozeigimusha_kojin_no
                  , dairinin_kojin_no
                  , gyomu_cd
                  , zeimoku_cd
                  , dairinin_yukokikan_kaishi_ymd
                  , dairinin_yukokikan_shuryo_ymd
                  , dairinin_katagaki
                  , memo
                  , renrakusaki_kbn
                  , denwa_no
                  , ins_datetime
                  , upd_datetime
                  , upd_tantosha_cd
                  , upd_tammatsu
                  , del_flg)
               VALUES (
                  lc_nozeigimusha_kojin_no
                  , lc_dairinin_kojin_no
                  , lc_gyomu_cd
                  , ln_zeimoku_cd
                  , ln_dairinin_yukokikan_kaishi_ymd
                  , ln_dairinin_yukokikan_shuryo_ymd
                  , rec_main.katagaki
                  , rec_main.memo 
                  , CASE WHEN rec_main.renrakusaki_kbn IS NOT NULL OR rec_main.renrakusaki_kbn <> '' THEN rec_main.renrakusaki_kbn::numeric ELSE 0 END
                  , lc_denwa
                  , concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                  , concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                  , rec_main.sosasha_cd
                  , 'SERVER'
                  , CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> '' THEN rec_main.del_flg::numeric ELSE 0 END);

                  ln_ins_count := ln_ins_count + 1;
                  lc_err_cd    := '0';
                  lc_err_text  := '';
                  ln_result_cd := 1;

            EXCEPTION WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_cd    := '9';
               lc_err_text  := SUBSTRING( SQLERRM, 1, 100 );
               ln_result_cd := 9;
            END;
         ELSE
            BEGIN
               UPDATE f_dairinin
               SET dairinin_yukokikan_shuryo_ymd = ln_dairinin_yukokikan_shuryo_ymd
                  , dairinin_katagaki = rec_main.katagaki
                  , memo = rec_main.memo 
                  , renrakusaki_kbn = CASE WHEN rec_main.renrakusaki_kbn IS NOT NULL OR rec_main.renrakusaki_kbn <> '' THEN rec_main.renrakusaki_kbn::numeric ELSE 0 END
                  , denwa_no = lc_denwa
                  , upd_datetime = CURRENT_TIMESTAMP
                  , upd_tantosha_cd = rec_main.sosasha_cd
                  , upd_tammatsu = 'SERVER'
                  , del_flg = CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> '' THEN rec_main.del_flg::numeric ELSE 0 END
               WHERE nozeigimusha_kojin_no = lc_nozeigimusha_kojin_no
                  AND dairinin_kojin_no = lc_dairinin_kojin_no
                  AND gyomu_cd = lc_gyomu_cd
                  AND zeimoku_cd = ln_zeimoku_cd
                  AND dairinin_yukokikan_kaishi_ymd = ln_dairinin_yukokikan_kaishi_ymd;

                  ln_upd_count := ln_upd_count + 1;
                  lc_err_cd    := '0';
                  lc_err_text  := '';
                  ln_result_cd := 2;
                  
            EXCEPTION
               WHEN OTHERS THEN
                  ln_result_cd := 9;
                  ln_err_count := ln_err_count + 1;
                  lc_err_cd    := '9';
                  lc_err_text  := SUBSTRING( SQLERRM, 1, 100 );
            END;
         END IF;

         BEGIN 
            UPDATE i_r4g_dairinin
            SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
            AND atena_no = rec_main.atena_no
            AND dairinin_atena_no = rec_main.dairinin_atena_no
            AND gyomu_id = rec_main.gyomu_id
            AND zeimoku_cd = rec_main.zeimoku_cd
            AND dairinin_yukokikan_kaishi_ymd = rec_main.dairinin_yukokikan_kaishi_ymd;
         EXCEPTION
            WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
               lc_err_cd := '9';
               ln_result_cd := 9;
         END;
         
      END LOOP;
   CLOSE cur_main;

    CALL proc_r4g_kanrinin_upd(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

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
