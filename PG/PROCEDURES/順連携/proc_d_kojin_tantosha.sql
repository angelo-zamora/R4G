--------------------------------------------------------
--  DDL for Procedure proc_d_kojin_tantosha
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_d_kojin_tantosha ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, out_n_result_code INOUT numeric, out_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* �����T�v : �l�S���ҍX�V                                                                                          */
/* ���� IN  : in_n_renkei_data_cd �c �A�g�f�[�^�R�[�h                                                                 */
/*            in_n_renkei_seq     �c �A�gSEQ�i�����P�ʂŕ��Ԃ����SEQ�j                                               */
/*            in_n_shori_ymd      �c ������ �i�����P�ʂŐݒ肳��鏈�����j                                            */
/*      OUT : out_n_result_co     �c ��O�G���[�������̃G���[�R�[�h                                                   */
/*            out_c_err_text      �c ��O�G���[�������̃G���[���e                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* �����@�@ : �V�K�쐬                                                                                                */
/**********************************************************************************************************************/

DECLARE

   ln_para01                      numeric DEFAULT 0;
   ln_para02                      numeric DEFAULT 0;
   ln_para03                      numeric DEFAULT 0;
   ln_para04                      numeric DEFAULT 0;
   ln_para05                      numeric DEFAULT 0;

   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   ln_rec_count                   numeric DEFAULT 0;

   ld_kaishi_datetime             timestamp;
   ld_shuryo_datetime             timestamp;

   ln_i                           numeric DEFAULT 0;
   ln_j                           numeric DEFAULT 0;
   lc_sql                         character varying(10000);

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;

   cur_busho CURSOR FOR
   SELECT busho_cd, del_flg
   FROM t_busho
   ORDER BY busho_cd;

   rec_busho_array                type_busho[];
   rec_busho                      type_busho;

BEGIN
-- 1
   ld_kaishi_datetime := CURRENT_TIMESTAMP(0);
-- 2
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 3 THEN ln_para03 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 4 THEN ln_para04 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 5 THEN ln_para05 := rec_parameter.parameter_value; END IF;
      END LOOP;
   CLOSE cur_parameter;
   
-- 3
   OPEN cur_busho;
      LOOP
         FETCH cur_busho INTO rec_busho;
         EXIT WHEN NOT FOUND;
		 
		 rec_busho_array := ARRAY_APPEND(rec_busho_array, rec_busho);
		 
	  END LOOP;
   CLOSE cur_busho;
   
-- 4
	IF rec_busho_array IS NOT NULL THEN
		FOR ln_i IN ARRAY_LOWER(rec_busho_array, 1)..ARRAY_UPPER(rec_busho_array, 1) LOOP
			IF ln_para01 = 0 THEN
				BEGIN
					SELECT COUNT(*)
					INTO ln_rec_count
					FROM f_kojin
						LEFT JOIN t_chiku 
							ON t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
							AND f_kojin.chiku_cd = t_chiku.chiku_cd
						LEFT JOIN f_kojin_tantosha 
							ON f_kojin_tantosha.busho_cd = rec_busho_array[ln_i].busho_cd 
							AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
						WHERE f_kojin_tantosha.kojin_no IS NULL;

					INSERT INTO f_kojin_tantosha( busho_cd, kojin_no, tantosha_cd1, tantosha_cd2, ins_datetime, upd_datetime, upd_tantosha_cd, upd_tammatsu, del_flg )
					SELECT t_chiku.busho_cd
						, f_kojin.kojin_no
						, t_chiku.tantosha_cd1
						, t_chiku.tantosha_cd2
						, CURRENT_TIMESTAMP(0)
						, CURRENT_TIMESTAMP(0)
						, 'BATCH'
						, 'SERVER'
						, 0
					FROM f_kojin
					LEFT JOIN t_chiku 
						ON t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
						AND f_kojin.chiku_cd = t_chiku.chiku_cd
					LEFT JOIN f_kojin_tantosha 
						ON f_kojin_tantosha.busho_cd = rec_busho_array[ln_i].busho_cd
						AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
					WHERE f_kojin_tantosha.kojin_no IS NULL;

					ln_ins_count := ln_ins_count + COALESCE( ln_rec_count, 0 );
				EXCEPTION
					WHEN OTHERS THEN
						ln_err_count := ln_err_count + 1;
				END;
			ELSE
				BEGIN
					SELECT COUNT(*)
					INTO ln_rec_count
					FROM f_kojin
					JOIN t_chiku 
						ON t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
						AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
					LEFT JOIN f_kojin_tantosha 
						ON f_kojin.kojin_no = f_kojin_tantosha.kojin_no
						AND t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
					WHERE f_kojin_tantosha.kojin_no IS NULL;

					INSERT INTO f_kojin_tantosha( busho_cd, kojin_no, tantosha_cd1, tantosha_cd2, ins_datetime, upd_datetime, upd_tantosha_cd, upd_tammatsu, del_flg )
					SELECT t_chiku.busho_cd
						, f_kojin.kojin_no
						, t_chiku.tantosha_cd1
						, t_chiku.tantosha_cd2
						, CURRENT_TIMESTAMP(0)
						, CURRENT_TIMESTAMP(0)
						, 'BATCH'
						, 'SERVER'
						, 0
					FROM f_kojin
						, t_chiku
						, f_kojin_tantosha
					LEFT JOIN t_chiku 
						ON t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
						AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
					LEFT JOIN f_kojin_tantosha ON f_kojin_tantosha.busho_cd = rec_busho_array[ln_i].busho_cd
					WHERE f_kojin_tantosha.kojin_no IS NULL;

					ln_ins_count := ln_ins_count + COALESCE( ln_rec_count, 0 );
				EXCEPTION
					WHEN OTHERS THEN
						ln_err_count := ln_err_count + 1;
				END;
			END IF;
		END LOOP;
-- 5

   FOR ln_i IN ARRAY_LOWER(rec_busho_array, 1)..ARRAY_UPPER(rec_busho_array, 1) LOOP
      IF ln_para01 = 0 THEN
         BEGIN
            SELECT COUNT(8)
            INTO ln_rec_count
            FROM f_kojin
               , t_chiku
               , f_kojin_tantosha
            WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
              AND f_kojin.chiku_cd = t_chiku.chiku_cd
              AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
              AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
              AND f_kojin_tantosha.henko_fuka_kbn = 0
              AND ( t_chiku.tantosha_cd1 <> f_kojin_tantosha.tantosha_cd1 OR t_chiku.tantosha_cd2 <> f_kojin_tantosha.tantosha_cd2 );

            UPDATE f_kojin_tantosha
            SET ( tantosha_cd1, tantosha_cd2, upd_datetime, upd_tantosha_cd, upd_tammatsu )
              = ( SELECT t_chiku.tantosha_cd1, t_chiku.tantosha_cd2, CURRENT_TIMESTAMP(0), 'BATCH', 'SERVER'
                  FROM f_kojin
                     , t_chiku
                  WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                    AND f_kojin.chiku_cd = t_chiku.chiku_cd
                    AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                    AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no )
            WHERE EXISTS( SELECT 1
                          FROM f_kojin
                             , t_chiku
                          WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                            AND f_kojin.chiku_cd = t_chiku.chiku_cd
                            AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                            AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
                            AND f_kojin_tantosha.henko_fuka_kbn = 0
                            AND ( COALESCE( t_chiku.tantosha_cd1, 'AAA' ) <> COALESCE( f_kojin_tantosha.tantosha_cd1, 'BBB' )
                               OR COALESCE( t_chiku.tantosha_cd2, 'AAA' ) <> COALESCE( f_kojin_tantosha.tantosha_cd2, 'BBB' ) ) );

            ln_upd_count := ln_upd_count + ln_rec_count;

            SELECT COUNT(8)
            INTO ln_rec_count
            FROM f_kojin
               , t_chiku
               , f_kojin_tantosha
            WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
              AND f_kojin.chiku_cd = t_chiku.chiku_cd
              AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
              AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
              AND f_kojin_tantosha.henko_fuka_kbn = 2
              AND t_chiku.tantosha_cd1 <> f_kojin_tantosha.tantosha_cd1;

            UPDATE f_kojin_tantosha
            SET ( tantosha_cd1, upd_datetime, upd_tantosha_cd, upd_tammatsu )
              = ( SELECT t_chiku.tantosha_cd1, CURRENT_TIMESTAMP(0), 'BATCH', 'SERVER'
                  FROM f_kojin
                     , t_chiku
                  WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                    AND f_kojin.chiku_cd = t_chiku.chiku_cd
                    AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                    AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no )
            WHERE EXISTS( SELECT 1
                          FROM f_kojin
                             , t_chiku
                          WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                            AND f_kojin.chiku_cd = t_chiku.chiku_cd
                            AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                            AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
                            AND f_kojin_tantosha.henko_fuka_kbn = 2
                            AND COALESCE( t_chiku.tantosha_cd1, 'AAA' ) <> COALESCE( f_kojin_tantosha.tantosha_cd1, 'BBB' ) );

            ln_upd_count := ln_upd_count + ln_rec_count;

            SELECT COUNT(8)
            INTO ln_rec_count
            FROM f_kojin
               , t_chiku
               , f_kojin_tantosha
            WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
              AND f_kojin.chiku_cd = t_chiku.chiku_cd
              AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
              AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
              AND f_kojin_tantosha.henko_fuka_kbn = 1
              AND t_chiku.tantosha_cd2 <> f_kojin_tantosha.tantosha_cd2;

            UPDATE f_kojin_tantosha
            SET ( tantosha_cd2, upd_datetime, upd_tantosha_cd, upd_tammatsu )
              = ( SELECT t_chiku.tantosha_cd2, CURRENT_TIMESTAMP(0), 'BATCH', 'SERVER'
                  FROM f_kojin
                     , t_chiku
                  WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                    AND f_kojin.chiku_cd = t_chiku.chiku_cd
                    AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                    AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no )
            WHERE EXISTS( SELECT 1
                          FROM f_kojin
                             , t_chiku
                          WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                            AND f_kojin.chiku_cd = t_chiku.chiku_cd
                            AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                            AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
                            AND f_kojin_tantosha.henko_fuka_kbn = 1
                            AND COALESCE( t_chiku.tantosha_cd2, 'AAA' ) <> COALESCE( f_kojin_tantosha.tantosha_cd2, 'BBB' ) );

            ln_upd_count := ln_upd_count + ln_rec_count;
         EXCEPTION
            WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
         END;
      ELSE
         BEGIN
            SELECT COUNT(8)
            INTO ln_rec_count
            FROM f_kojin
               , t_chiku
               , f_kojin_tantosha
            WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
              AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
              AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
              AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
              AND f_kojin_tantosha.henko_fuka_kbn = 0
              AND ( t_chiku.tantosha_cd1 <> f_kojin_tantosha.tantosha_cd1 OR t_chiku.tantosha_cd2 <> f_kojin_tantosha.tantosha_cd2 );

            UPDATE f_kojin_tantosha
            SET ( tantosha_cd1, tantosha_cd2, upd_datetime, upd_tantosha_cd, upd_tammatsu )
              = ( SELECT t_chiku.tantosha_cd1, t_chiku.tantosha_cd2, CURRENT_TIMESTAMP(0), 'BATCH', 'SERVER'
                  FROM f_kojin
                     , t_chiku
                  WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                    AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
                    AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                    AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no )
            WHERE EXISTS( SELECT 1
                          FROM f_kojin
                             , t_chiku
                          WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                            AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
                            AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                            AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
                            AND f_kojin_tantosha.henko_fuka_kbn = 0
                            AND ( COALESCE( t_chiku.tantosha_cd1, 'AAA' ) <> COALESCE( f_kojin_tantosha.tantosha_cd1, 'BBB' )
                               OR COALESCE( t_chiku.tantosha_cd2, 'AAA' ) <> COALESCE( f_kojin_tantosha.tantosha_cd2, 'BBB' ) ) );

            ln_upd_count := ln_upd_count + ln_rec_count;

            SELECT COUNT(8)
            INTO ln_rec_count
            FROM f_kojin
               , t_chiku
               , f_kojin_tantosha
            WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
              AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
              AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
              AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
              AND f_kojin_tantosha.henko_fuka_kbn = 2
              AND t_chiku.tantosha_cd1 <> f_kojin_tantosha.tantosha_cd1;

            UPDATE f_kojin_tantosha
            SET ( tantosha_cd1, upd_datetime, upd_tantosha_cd, upd_tammatsu )
              = ( SELECT t_chiku.tantosha_cd1, CURRENT_TIMESTAMP(0), 'BATCH', 'SERVER'
                  FROM f_kojin
                     , t_chiku
                  WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                    AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
                    AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                    AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no )
            WHERE EXISTS( SELECT 1
                          FROM f_kojin
                             , t_chiku
                          WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                            AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
                            AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                            AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
                            AND f_kojin_tantosha.henko_fuka_kbn = 2
                            AND COALESCE( t_chiku.tantosha_cd1, 'AAA' ) <> COALESCE( f_kojin_tantosha.tantosha_cd1, 'BBB' ) );

            ln_upd_count := ln_upd_count + ln_rec_count;

            SELECT COUNT(8)
            INTO ln_rec_count
            FROM f_kojin
               , t_chiku
               , f_kojin_tantosha
            WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
              AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
              AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
              AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
              AND f_kojin_tantosha.henko_fuka_kbn = 1
              AND t_chiku.tantosha_cd2 <> f_kojin_tantosha.tantosha_cd2;

            UPDATE f_kojin_tantosha
            SET ( tantosha_cd2, upd_datetime, upd_tantosha_cd, upd_tammatsu )
              = ( SELECT t_chiku.tantosha_cd2, CURRENT_TIMESTAMP(0), 'BATCH', 'SERVER'
                  FROM f_kojin
                     , t_chiku
                  WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                    AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
                    AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                    AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no )
            WHERE EXISTS( SELECT 1
                          FROM f_kojin
                             , t_chiku
                          WHERE t_chiku.busho_cd = rec_busho_array[ln_i].busho_cd
                            AND f_kojin.doitsunin_chiku_cd = t_chiku.chiku_cd
                            AND t_chiku.busho_cd = f_kojin_tantosha.busho_cd
                            AND f_kojin.kojin_no = f_kojin_tantosha.kojin_no
                            AND f_kojin_tantosha.henko_fuka_kbn = 1
                            AND COALESCE( t_chiku.tantosha_cd2, 'AAA' ) <> COALESCE( f_kojin_tantosha.tantosha_cd2, 'BBB' ) );

            ln_upd_count := ln_upd_count + ln_rec_count;
         EXCEPTION
            WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
         END;
      END IF;
   END LOOP;
	END IF;

   ld_shuryo_datetime := CURRENT_TIMESTAMP(0);

   BEGIN
      INSERT INTO f_batch_log( renkei_data_cd, seq_no_renkei, shori_ymd, kaishi_datetime, shuryo_datetime, shori_count, ins_count, upd_count, del_count, err_count )
      VALUES( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, ld_kaishi_datetime, ld_shuryo_datetime, ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count );
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   RAISE NOTICE '�J�n�F% �I���F% ���������F% �ǉ������F% �X�V�����F% �G���[�����F%', ld_kaishi_datetime, ld_shuryo_datetime, ln_shori_count, ln_ins_count, ln_upd_count, ln_err_count;

END;
$$;