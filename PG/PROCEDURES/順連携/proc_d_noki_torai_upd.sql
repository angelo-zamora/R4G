--------------------------------------------------------
--  DDL for Procedure proc_d_noki_torai_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_d_noki_torai_upd ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, out_n_result_code INOUT numeric, out_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* �����T�v : �[���������X�V����                                                                                      */
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

   ld_kaishi_datetime             timestamp;
   ld_shuryo_datetime             timestamp;

   cur_batch_log CURSOR FOR
   SELECT shuryo_datetime
   FROM f_batch_log
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_batch_log                  f_batch_log.shuryo_datetime%TYPE;

   cur_main01 CURSOR FOR
   SELECT *
   FROM f_taino;
--   FOR UPDATE;

   cur_main02 CURSOR FOR
   SELECT *
   FROM f_taino
   WHERE upd_datetime >= rec_batch_log.shuryo_datetime;
--   FOR UPDATE;

   rec_main                       f_taino%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT f_renkei_parameter.parameter_no
        , f_renkei_parameter.parameter_value
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;


BEGIN

   ld_kaishi_datetime := CURRENT_TIMESTAMP(0);

   OPEN cur_batch_log;
      FETCH cur_batch_log INTO rec_batch_log;
		IF NOT FOUND THEN
         rec_batch_log := to_date( '1900/01/01', 'YYYY/MM/DD' );
      END IF;
   CLOSE cur_batch_log;

   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no =  1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no =  2 THEN ln_para02 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no =  3 THEN ln_para03 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no =  4 THEN ln_para04 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no =  5 THEN ln_para05 := rec_parameter.parameter_value; END IF;
      END LOOP;
   CLOSE cur_parameter;


   IF ln_para01 = 0 THEN
      OPEN cur_main01;
         LOOP
            FETCH cur_main01 INTO rec_main;
            EXIT WHEN NOT FOUND;

            ln_shori_count := ln_shori_count + 1;

            CALL sp_noki_torai_upd(rec_main);

         END LOOP;
      CLOSE cur_main01;
   ELSE
      OPEN cur_main02;
         LOOP
            FETCH cur_main02 INTO rec_main;
            EXIT WHEN NOT FOUND;

            ln_shori_count := ln_shori_count + 1;

            CALL sp_noki_torai_upd(rec_main);

         END LOOP;
      CLOSE cur_main02;
   END IF;

   ld_shuryo_datetime := CURRENT_TIMESTAMP(0);

   BEGIN
      INSERT INTO f_batch_log( renkei_data_cd, seq_no_renkei, shori_ymd, kaishi_datetime, shuryo_datetime, shori_count )
      VALUES( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, ld_kaishi_datetime, ld_shuryo_datetime, ln_shori_count );
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

END;
$$;
