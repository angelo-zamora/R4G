--------------------------------------------------------
--  DDL for Procedure proc_d_jiko_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_d_jiko_upd ( 
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   out_n_result_code INOUT numeric, 
   out_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 機能概要 : 時効予定日更新                                                                                            */
/* 入力 IN  : in_n_renkei_data_cd  連携データコード                                                                     */
/*            in_n_renkei_seq      連携SEQ（連携の一意性を保つSEQ）                                                     */
/*            in_n_shori_ymd       処理日 （連携の処理を行う日付）                                                      */
/* 出力 OUT : out_n_result_co      結果エラーが発生した場合のエラーコード                                                 */
/*            out_c_err_text       結果エラーが発生した場合のエラーメッセージ                                             */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/02/03  CRESS-INFO.Drexler     新規作成     時効予定日更新                                             */
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
   FROM dlgrenkei.f_batch_log
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_batch_log                  dlgrenkei.f_batch_log.shuryo_datetime%TYPE;

   cur_main01 CURSOR FOR
   SELECT *
   FROM f_taino
   WHERE kanno_cd <> 4
   AND del_flg = 0;

   cur_main02 CURSOR FOR
   SELECT kibetsu_key
   FROM f_taino
   WHERE upd_datetime >= rec_batch_log.shuryo_datetime
   UNION
   SELECT kibetsu_key
   FROM f_shuno
   WHERE upd_datetime >= rec_batch_log.shuryo_datetime
   UNION
   SELECT kibetsu_key
   FROM f_bunno_kibetsu
   WHERE upd_datetime >= rec_batch_log.shuryo_datetime
   UNION
   SELECT kibetsu_key
   FROM f_yuyo_kibetsu
   WHERE upd_datetime >= rec_batch_log.shuryo_datetime
   UNION
   SELECT kibetsu_key
   FROM f_kesson_kibetsu
   WHERE upd_datetime >= rec_batch_log.shuryo_datetime
   UNION
   SELECT kibetsu_key
   FROM f_shikkoteishi_kibetsu
   WHERE upd_datetime >= rec_batch_log.shuryo_datetime
   UNION
   SELECT kibetsu_key
   FROM f_shobun_kibetsu
   WHERE upd_datetime >= rec_batch_log.shuryo_datetime
   UNION
   SELECT kibetsu_key
   FROM f_jiko_chudan_kibetsu
   WHERE upd_datetime >= rec_batch_log.shuryo_datetime
   UNION
   SELECT kibetsu_key
   FROM f_saikoku_rireki
      , f_saikoku_rireki_kibetsu
   WHERE f_saikoku_rireki.seq_no_saikoku = f_saikoku_rireki_kibetsu.seq_no_saikoku
     AND f_saikoku_rireki_kibetsu.upd_datetime >= rec_batch_log.shuryo_datetime
     AND f_saikoku_rireki.saikoku_reibun_cd IN( SELECT saikoku_reibun_cd
                                                FROM t_saikoku_reibun
                                                WHERE jiko_encho_flg = 1 );

   cur_main03 CURSOR FOR
        SELECT kibetsu_key
        FROM dlgrenkei.i_r4g_shuno
        WHERE error_cd = '0'
        UNION
        SELECT kibetsu_key
        FROM dlgrenkei.i_r4g_shuno_rireki
        WHERE error_cd = '0';
    
   rec_main                       f_taino%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;


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
    
      END LOOP;
   CLOSE cur_parameter;

   IF ln_para01 = 0 THEN
      OPEN cur_main01;
         LOOP
            FETCH cur_main01 INTO rec_main;
            EXIT WHEN NOT FOUND;

            ln_shori_count := ln_shori_count + 1;
            
            CALL dlgrenkei.jiko_upd(rec_main);

         END LOOP;
      CLOSE cur_main01;
   ELSIF ln_para01 = 1 THEN
      OPEN cur_main02;
         LOOP
            FETCH cur_main02 INTO rec_main;
            EXIT WHEN NOT FOUND;

            ln_shori_count := ln_shori_count + 1;

            CALL dlgrenkei.jiko_upd(rec_main);

         END LOOP;
      CLOSE cur_main02;
   ELSIF ln_para01 = 2 THEN
      OPEN cur_main03;
         LOOP
            FETCH cur_main03 INTO rec_main;
            EXIT WHEN NOT FOUND;

             ln_shori_count := ln_shori_count + 1;

            CALL dlgrenkei.jiko_upd(rec_main);

         END LOOP;
   ELSIF ln_para01 = 3 THEN
      RETURN; -- 確認中
   END IF;

   ld_shuryo_datetime := CURRENT_TIMESTAMP(0);

   BEGIN
      INSERT INTO dlgrenkei.f_batch_log( renkei_data_cd, seq_no_renkei, shori_ymd, kaishi_datetime, shuryo_datetime, shori_count )
      VALUES( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, ld_kaishi_datetime, ld_shuryo_datetime, ln_shori_count );
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;
END;
$$;