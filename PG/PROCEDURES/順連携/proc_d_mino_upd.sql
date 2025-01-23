--------------------------------------------------------
--  DDL for Procedure proc_d_mino_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_d_mino_upd ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, out_n_result_code INOUT numeric, out_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 機能概要 : データ更新処理                                                                                          */
/* 入力 IN  : in_n_renkei_data_cd  連携データコード                                                                     */
/*            in_n_renkei_seq      連携SEQ（連携の一意性を保つSEQ）                                                   */
/*            in_n_shori_ymd       処理日 （連携の処理を行う日付）                                                    */
/* 出力 OUT : out_n_result_co      結果エラーが発生した場合のエラーコード                                               */
/*            out_c_err_text       結果エラーが発生した場合のエラーメッセージ                                           */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 更新履歴 : 新規作成                                                                                                */
/**********************************************************************************************************************/

DECLARE
   -- rec_f_taino                    dlgmain.f_taino%ROWTYPE;

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

   ld_kaishi_datetime             timestamp(0);
   ld_shuryo_datetime             timestamp(0);


   cur_main01 CURSOR FOR
   SELECT f_taino.kibetsu_key, f_taino.zeigaku, f_taino.tokusoku, f_taino.entaikin, f_taino.zeigaku_shuno, f_taino.tokusoku_shuno, f_taino.entaikin_shuno
   FROM f_taino;

   cur_main02 CURSOR FOR
   SELECT f_taino.kibetsu_key, f_taino.zeigaku, f_taino.tokusoku, f_taino.entaikin, f_taino.zeigaku_shuno, f_taino.tokusoku_shuno, f_taino.entaikin_shuno
   FROM f_taino
      , i_r4g_shuno
   WHERE f_taino.kibetsu_key = i_r4g_shuno.kibetsu_key;

   cur_main03 CURSOR FOR
   SELECT f_taino.kibetsu_key, f_taino.zeigaku, f_taino.tokusoku, f_taino.entaikin, f_taino.zeigaku_shuno, f_taino.tokusoku_shuno, f_taino.entaikin_shuno
   FROM f_taino
      , i_r4g_shuno_rireki
   WHERE f_taino.kibetsu_key = i_r4g_shuno_rireki.kibetsu_key;


   rec_main                       f_taino%ROWTYPE;


   cur_parameter CURSOR FOR
   SELECT f_renkei_parameter.parameter_no
        , f_renkei_parameter.parameter_value
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;

   cur_batch_log CURSOR FOR
   SELECT shuryo_datetime
   FROM f_batch_log
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_batch_log                  RECORD;


BEGIN

   ld_kaishi_datetime := CURRENT_TIMESTAMP(0);
		OPEN cur_batch_log;
		LOOP
			FETCH cur_batch_log INTO rec_batch_log;
			EXIT WHEN NOT FOUND;
		END LOOP;

		IF NOT FOUND THEN
			rec_batch_log.shuryo_datetime := to_date( '1900/01/01', 'YYYY/MM/DD' );
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

            CALL mino_upd(ln_para02, rec_main);

         END LOOP;
      CLOSE cur_main01;
   ELSIF ln_para01 = 1 THEN
      OPEN cur_main02;
         LOOP
            FETCH cur_main02 INTO rec_main;
            EXIT WHEN NOT FOUND;

            ln_shori_count := ln_shori_count + 1;

            CALL mino_upd(ln_para02, rec_main);

         END LOOP;
      CLOSE cur_main02;

      OPEN cur_main03;
         LOOP
            FETCH cur_main03 INTO rec_main;
            EXIT WHEN NOT FOUND;

            ln_shori_count := ln_shori_count + 1;

            CALL mino_upd(ln_para02, rec_main);

         END LOOP;
      CLOSE cur_main03;
   ELSIF ln_para01 = 2 THEN
      OPEN cur_main02;
         LOOP
            FETCH cur_main02 INTO rec_main;
            EXIT WHEN NOT FOUND;

            ln_shori_count := ln_shori_count + 1;

            CALL mino_upd(ln_para02, rec_main);

         END LOOP;
      CLOSE cur_main02;
   ELSIF ln_para01 = 3 THEN
      OPEN cur_main03;
         LOOP
            FETCH cur_main03 INTO rec_main;
            EXIT WHEN NOT FOUND;

            ln_shori_count := ln_shori_count + 1;

            CALL mino_upd(ln_para02, rec_main);

         END LOOP;
      CLOSE cur_main03;
   END IF;

   ld_shuryo_datetime := CURRENT_TIMESTAMP(0);

   BEGIN
      INSERT INTO f_batch_log( renkei_data_cd, seq_no_renkei, shori_ymd, kaishi_datetime, shuryo_datetime, shori_count )
      VALUES( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, to_date(ld_kaishi_datetime::text, 'YYYY/MM/DD'), to_date(ld_shuryo_datetime::text, 'YYYY/MM/DD'), ln_shori_count );
   EXCEPTION
      WHEN OTHERS THEN NULL;
		RAISE NOTICE '% : %', SQLSTATE, SQLERRM;
   END;
END;
$$;