--------------------------------------------------------
--  DDL for Procedure  proc_r4g_kanrinin_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_kanrinin_upd (
   in_n_renkei_data_cd IN numeric,
   in_n_renkei_seq IN numeric,
   in_n_shori_ymd IN numeric,
   io_c_err_code INOUT character varying,
   io_c_err_text INOUT character varying
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 代理人情報（統合収滞納）」                                                                                 */
/* 引数 IN  : in_n_renkei_data_cd  … 連携データコード                                                                   */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                   */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                 */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                       */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                         */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/27  CRESS-INFO.Drexler     新規作成     036o006「収納履歴情報（統合収滞納）」の取込を行う            */
/**********************************************************************************************************************/

DECLARE
   ln_shori_count                      numeric DEFAULT 0;
   ln_upd_count                        numeric DEFAULT 0;
   ln_err_count                        numeric DEFAULT 0;
   ln_result_cd                        numeric DEFAULT 0;
   lc_err_text                         character varying;
   lc_sql                              character varying;
   lc_err_cd                           character varying;

   ln_kanrinin_cd			               numeric;
   lc_kanrinin_kojin_no		            character varying;
   ln_zeimoku_cd                       numeric;

   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_dairinin
   WHERE saishin_flg = '1'
      AND katagaki = '09'
      AND zeimoku_cd = '00'
      AND result_cd < 8;

   rec_main                            i_r4g_dairinin%ROWTYPE;

   cur_main2 CURSOR FOR
   SELECT *
   FROM i_r4g_dairinin
   WHERE saishin_flg = '1'
      AND katagaki = '09'
      AND zeimoku_cd <> '00'
      AND result_cd < 8;

   rec_main2                           i_r4g_dairinin%ROWTYPE;

   cur_taino_lock CURSOR (p_kojin_no VARCHAR, p_zeimoku_cd NUMERIC) FOR
   SELECT *
   FROM f_taino
   WHERE kojin_no = p_kojin_no
      AND zeimoku_cd = p_zeimoku_cd;

   rec_lock                            f_taino%ROWTYPE;

BEGIN
   OPEN cur_main;
   LOOP
      FETCH cur_main INTO rec_main;
      EXIT WHEN NOT FOUND;

      ln_shori_count := ln_shori_count + 1;
      ln_zeimoku_cd := get_r4g_code_conv(1, 3, null, rec_main.zeimoku_cd::character varying);

      OPEN cur_taino_lock(rec_main.atena_no, ln_zeimoku_cd);
      FETCH cur_taino_lock INTO rec_lock;
      CLOSE cur_taino_lock;

      IF rec_lock.kibetsu_key IS NOT NULL THEN
         BEGIN
            IF(
               rec_main.dairinin_atena_no = rec_lock.kojin_no
               AND ln_zeimoku_cd = rec_lock.zeimoku_cd
               AND getdatetonum(to_date(rec_main.dairinin_yukokikan_kaishi_ymd, 'yyyy-mm-dd')) <= to_number(to_char(CURRENT_DATE,'yyyymmdd'), '99999999')
               AND getdatetonum(to_date(rec_main.dairinin_yukokikan_shuryo_ymd, 'yyyy-mm-dd')) >= to_number(to_char(CURRENT_DATE,'yyyymmdd'), '99999999')
               AND rec_main.del_flg = '0'
            ) THEN
               ln_kanrinin_cd := 1;
               lc_kanrinin_kojin_no := rec_main.dairinin_atena_no;
            ELSE
               ln_kanrinin_cd := 0;
               lc_kanrinin_kojin_no := LPAD( '0', 15, '0' );
            END IF;

            UPDATE f_taino
            SET kanrinin_cd = ln_kanrinin_cd,
               kanrinin_kojin_no = lc_kanrinin_kojin_no,
               upd_datetime = concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp,
               upd_tantosha_cd = rec_main.sosasha_cd,
               upd_tammatsu = 'SERVER'
            WHERE kibetsu_key = rec_lock.kibetsu_key;

            ln_upd_count := ln_upd_count + 1;

		   EXCEPTION
            WHEN OTHERS THEN
                ln_err_count := ln_err_count + 1;
                lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                lc_err_cd := '9';
                ln_result_cd := 9;
				RAISE NOTICE '% === %', SQLSTATE, SQLERRM;
         END;
      END IF;

   END LOOP;
   CLOSE cur_main;

   OPEN cur_main2;
   LOOP
      FETCH cur_main2 INTO rec_main2;
      EXIT WHEN NOT FOUND;

      ln_shori_count := ln_shori_count + 1;
      ln_zeimoku_cd  := get_r4g_code_conv(1, 3, null, rec_main.zeimoku_cd::character varying);
	  	  	  
      OPEN cur_taino_lock(rec_main2.atena_no, ln_zeimoku_cd);
      FETCH cur_taino_lock INTO rec_lock;
      CLOSE cur_taino_lock;

      IF rec_lock.kibetsu_key IS NOT NULL THEN
         BEGIN
            IF(
               rec_main2.dairinin_atena_no = rec_lock.kojin_no
               AND ln_zeimoku_cd = rec_lock.zeimoku_cd
               AND getdatetonum(to_date(rec_main2.dairinin_yukokikan_kaishi_ymd, 'yyyy-mm-dd'))  <= to_number(to_char(CURRENT_DATE,'yyyymmdd'), '99999999')
               AND getdatetonum(to_date(rec_main2.dairinin_yukokikan_shuryo_ymd, 'yyyy-mm-dd'))  >= to_number(to_char(CURRENT_DATE,'yyyymmdd'), '99999999')
               AND rec_main2.del_flg = '0'
            ) THEN
               ln_kanrinin_cd := 1;
               lc_kanrinin_kojin_no := rec_main2.dairinin_atena_no;
            ELSE
               ln_kanrinin_cd := 0;
               lc_kanrinin_kojin_no := LPAD( '0', 15, '0' );
            END IF;
			
            UPDATE f_taino
            SET kanrinin_cd = ln_kanrinin_cd,
               kanrinin_kojin_no = lc_kanrinin_kojin_no,
               upd_datetime = concat(rec_main2.sosa_ymd, ' ', rec_main2.sosa_time)::timestamp,
               upd_tantosha_cd = rec_main2.sosasha_cd,
               upd_tammatsu = 'SERVER'
            WHERE kibetsu_key = rec_lock.kibetsu_key;

            ln_upd_count := ln_upd_count + 1;

		   EXCEPTION
            WHEN OTHERS THEN
                ln_err_count := ln_err_count + 1;
                lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                lc_err_cd := '9';
                ln_result_cd := 9;
            RAISE NOTICE '% === %', SQLSTATE, SQLERRM;
         END;
      END IF;

   END LOOP;
   CLOSE cur_main2;

   EXCEPTION
      WHEN OTHERS THEN NULL;
      
END;
$$;