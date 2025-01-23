--------------------------------------------------------
--  DDL for Procedure  proc_r4g_tokusoku_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_tokusoku_upd ( 
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 督促情報更新                                                                                          */
/* 引数 IN  : in_n_renkei_data_cd  … 連携データコード                                                                   */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                   */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                 */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                       */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                         */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :                                                                                                     */
/**********************************************************************************************************************/

DECLARE

    rec_log                        f_renkei_log%ROWTYPE;

    ln_shori_count                 numeric DEFAULT 0;
    ln_err_count                   numeric DEFAULT 0;
    lc_err_cd                      character varying;
    lc_err_text                    character varying(100);
    ln_result_cd                   numeric DEFAULT 0;

    lc_sql                         character varying(1000);

    ld_kaishi_datetime             DATE;
    ld_shuryo_datetime             DATE;

    ln_para01                      numeric DEFAULT 0;

    cur_parameter CURSOR FOR
    SELECT *
    FROM f_renkei_parameter
    WHERE renkei_data_cd = in_n_renkei_data_cd;

    rec_parameter                  f_renkei_parameter%ROWTYPE;

    cur_main CURSOR FOR
    SELECT *
    FROM f_tokusoku_kibetsu
    WHERE tokusoku_ymd <> 0 
    AND tokusoku_henrei_ymd <> 0
    AND hikinuki_kbn = 0;

    cur_main_02 CURSOR FOR
    SELECT *
    FROM i_r4g_tokusoku
    WHERE result_cd IN (1,2)
    AND tokusoku_hakko_ymd <> 0
    AND tokusoku_henrei_ymd = 0
    AND hikinuki_del_kbn = 0;

    cur_main_03 CURSOR FOR
    SELECT *
    FROM f_tokusoku_kibetsu
    WHERE tokusoku_henrei_ymd <> 0
    OR hikinuki_kbn = 1
    OR tokusoku_henrei_ymd = 0
    OR del_flg = 1;

    cur_main_04 CURSOR FOR
    SELECT *
    FROM i_r4g_tokusoku
    WHERE result_cd IN (1,2,3)
    OR tokusoku_henrei_ymd <> 0
    OR hikinuki_del_kbn = 1;

    rec_main                       f_tokusoku_kibetsu%ROWTYPE;
    rec_i_r4g_tokusoku             i_r4g_tokusoku%ROWTYPE;

BEGIN
    lc_sql := '';
    ln_shori_count := 0;
    ld_kaishi_datetime := SYSDATE;

    rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;
         ln_para01 := rec_parameter.parameter_value;
      END LOOP;
   CLOSE cur_parameter;

   CALL proc_error_check( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );
   
   IF io_c_err_code <> '0'  THEN
      RETURN;
   END IF;

      -- 条件を満たすデータが存在する場合、督促発布日を更新する。
      IF ln_para01 IN (0, 1)THEN
        OPEN cur_main;
            LOOP
                FETCH cur_main INTO rec_main;
                EXIT WHEN NOT FOUND;

                ln_shori_count := ln_shori_count + 1;

                    BEGIN
                        UPDATE f_taino
                        SET tokusoku_ymd = 0
                        , upd_datetime = CURRENT_TIMESTAMP
                        , upd_tantosha_cd = 'RENKEI'
                        , upd_tammatsu = 'SERVER'
                        WHERE kibetsu_key = rec_main.kibetsu_key;  

                    EXCEPTION
                    WHEN OTHERS THEN
                        io_c_err_code := SQLSTATE;
                        io_c_err_text := SQLERRM;
                        RETURN;
                    END;

            END LOOP;
         CLOSE cur_main;

         OPEN cur_main_03;
            LOOP
                FETCH cur_main_03 INTO rec_main;
                EXIT WHEN NOT FOUND;

                    ln_shori_count := ln_shori_count + 1;

                    BEGIN
                        UPDATE f_taino
                        SET tokusoku_ymd = 0
                        , upd_datetime = CURRENT_TIMESTAMP
                        , upd_tantosha_cd = 'RENKEI'
                        , upd_tammatsu = 'SERVER'
                        WHERE kibetsu_key = rec_main.kibetsu_key;  

                    EXCEPTION
                    WHEN OTHERS THEN
                        io_c_err_code := SQLSTATE;
                        io_c_err_text := SQLERRM;
                        RETURN;
                    END;

            END LOOP;
         CLOSE cur_main_03;

         -- 更新日時がdlgrenkei：f_バッチ_ログ（f_batch_log）の処理終了日時より大きい
         IF ln_para01 = 1 THEN
               ld_shuryo_datetime := SYSDATE;
               BEGIN
                  INSERT INTO f_batch_log( renkei_data_cd, seq_no_renkei, shori_ymd, kaishi_datetime, shuryo_datetime, shori_count )
                  VALUES( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, ld_kaishi_datetime, ld_shuryo_datetime, ln_shori_count );
               EXCEPTION
               WHEN OTHERS THEN
                  io_c_err_code := SQLSTATE;
                  io_c_err_text := SQLERRM;
                  RETURN;
               END;
         END IF;

      ELSEIF ln_para01 = 2 THEN
         OPEN cur_main_02;
               LOOP
                  FETCH cur_main_02 INTO rec_i_r4g_tokusoku;
                  EXIT WHEN NOT FOUND;
                  ln_shori_count := ln_shori_count + 1;
                     BEGIN
                        UPDATE f_taino
                           SET tokusoku_ymd = 0
                           , upd_datetime = CURRENT_TIMESTAMP
                           , upd_tantosha_cd = 'RENKEI'
                           , upd_tammatsu = 'SERVER'
                           WHERE kibetsu_key = rec_i_r4g_tokusoku.kibetsu_key;  

                     EXCEPTION
                     WHEN OTHERS THEN
                           io_c_err_code := SQLSTATE;
                           io_c_err_text := SQLERRM;
                           RETURN;
                     END;
               END LOOP;
         CLOSE cur_main_02;

         OPEN cur_main_04;
            LOOP
               FETCH cur_main_04 INTO rec_i_r4g_tokusoku;
               EXIT WHEN NOT FOUND;
                  ln_shori_count := ln_shori_count + 1;

                  BEGIN
                     UPDATE f_taino
                           SET tokusoku_ymd = 0
                           , upd_datetime = CURRENT_TIMESTAMP
                           , upd_tantosha_cd = 'RENKEI'
                           , upd_tammatsu = 'SERVER'
                        WHERE kibetsu_key = rec_i_r4g_tokusoku.kibetsu_key;  

                     EXCEPTION
                     WHEN OTHERS THEN
                           io_c_err_code := SQLSTATE;
                           io_c_err_text := SQLERRM;
                           RETURN;
                     END;
               END LOOP;
         CLOSE cur_main_04;
      END IF;

      -- 処理結果を登録する。
      BEGIN
         INSERT INTO f_batch_log( renkei_data_cd, seq_no_renkei, shori_ymd, kaishi_datetime, shuryo_datetime, shori_count )
            VALUES( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, ld_kaishi_datetime, ld_shuryo_datetime, ln_shori_count );
         EXCEPTION
         WHEN OTHERS THEN
            io_c_err_code := SQLSTATE;
            io_c_err_text := SQLERRM;
            RETURN;
      END;

   EXCEPTION WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;
