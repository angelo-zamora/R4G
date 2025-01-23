--------------------------------------------------------
--  DDL for Procedure  proc_r4g_shiensochi_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_shiensochi_upd ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, io_c_err_code INOUT character varying, io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 支援措置情報更新                                                                                          */
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
    ln_kanri_count                 numeric DEFAULT 0;
    lc_kanrinin_kojin_no           character varying;

    lc_sql                         character varying(1000);

    ln_para01                      numeric DEFAULT 0;
    ld_kaishi_datetime             DATE;
    ld_shuryo_datetime             DATE;

    cur_parameter CURSOR FOR
    SELECT *
    FROM f_renkei_parameter
    WHERE renkei_data_cd = in_n_renkei_data_cd;

    rec_parameter                              f_renkei_parameter%ROWTYPE;

    cur_main CURSOR FOR
    SELECT *
    FROM kojin
    WHERE tenshutsu_ymd <> 0 
    AND del_flg = 0;

    cur_main_02 CURSOR FOR
    SELECT *
    FROM i_r4g_atena
    WHERE result_cd in (1, 2)
    AND tenshutsu_ymd <> 0;

    cur_busho CURSOR FOR
    SELECT *
    FROM t_busho
    ORDER BY busho_cd;

    rec_busho            t_busho%ROWTYPE;

    rec_main                                   f_kojin%ROWTYPE;
    rec_i_r4g_atena                            i_r4g_atena%ROWTYPE;

BEGIN
    lc_sql := '';

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

   IF ln_para01 IN (0, 1)THEN
        OPEN cur_main;
            LOOP
                FETCH cur_main INTO rec_main;
                EXIT WHEN NOT FOUND;
                    BEGIN
                        UPDATE f_shiensochi
                        SET ido_flg = 1            
                        , upd_datetime = CURRENT_TIMESTAMP
                        , upd_tantosha_cd = rec_main.upd_tantosha_cd
                        , upd_tammatsu = rec_main.upd_tammatsu
                        WHERE kojin_no = rec_main.kojin_no
                        AND rec_main.tenshutsu_ymd BETWEEN kaishi_ymd AND shuryo_ymd
                        AND shiensochi_kbn IN(1, 2)  -- 1または2（3：終了のデータは更新しない）
                        AND del_flg = 0;
                    EXCEPTION
                    WHEN OTHERS THEN
                        io_c_err_code := SQLSTATE;
                        io_c_err_text := SQLERRM;
                        RETURN;
                    END;
                    -- 異動フラグを立てた個人に対して記事情報を追加する。
                     OPEN cur_busho;
                        LOOP
                            FETCH cur_busho INTO rec_busho;
                                BEGIN
                                    INSERT INTO f_kiji(
                                        busho_cd
                                        , seq_no_kiji
                                        , kojin_no
                                        , kodo_yotei_kbn
                                        , kiji_ymd
                                        , kiji_time
                                        , kiji_bunrui_cd
                                        , kiji_bunrui
                                        , kiji_naiyo_cd
                                        , kiji_naiyo
                                        , kosho_hoho_cd
                                        , kosho_hoho
                                        , tainoseiri_kiroku_cd
                                        , tainoseiri_kiroku
                                        , midashi
                                        , kiji_biko
                                        , sessho_flg
                                        , sessho_aite_cd
                                        , sessho_aite
                                        , sessho_basho_cd
                                        , sessho_basho
                                        , tantosha_cd
                                        , tantosha
                                        , busho
                                        , nofu_yotei_ymd
                                        , nofu_yotei_kingaku
                                        , sashiosae_yotei_ymd
                                        , kyocho_hyoji_flg
                                        , ins_datetime
                                        , upd_datetime
                                        , upd_tantosha_cd
                                        , upd_tammatsu
                                        , del_flg
                                    ) VALUES (
                                        rec_busho.busho_cd
                                        , nextval('dlgmain.seq_no_kiji')
                                        , rec_main.kojin_no
                                        , 1
                                        , get_date_to_num(current_date)
                                        , 0
                                        , 2
                                        , '事務処理用'
                                        , NULL
                                        , NULL
                                        , NULL
                                        , NULL
                                        , '029'
                                        , '住所異動'
                                        , '支援措置対象者異動'
                                        , '支援措置対象者が異動しました。必要に応じて注意喚起情報を作成してください。'
                                        , 0
                                        , 0
                                        , NULL
                                        , 0
                                        , NULL
                                        , NULL
                                        , NULL
                                        , rec_busho.busho
                                        , 0
                                        , 0
                                        , 0
                                        , 1
                                        , current_timestamp
                                        , current_timestamp
                                        , 'RENKEI'
                                        , 'SERVER'
                                        , 0
                                    );
                                 EXCEPTION
                                WHEN OTHERS THEN
                                    io_c_err_code := SQLSTATE;
                                    io_c_err_text := SQLERRM;
                                    RETURN;
                                END;
                        END LOOP;
                        CLOSE cur_busho;
            END LOOP;
        CLOSE cur_main;

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
                FETCH cur_main_02 INTO rec_i_r4g_atena;
                EXIT WHEN NOT FOUND;
                ln_shori_count := ln_shori_count + 1;
                    BEGIN
                        UPDATE f_shiensochi
                        SET ido_flg = 1         
                        , upd_datetime = CURRENT_TIMESTAMP
                        , upd_tantosha_cd = rec_i_r4g_atena.upd_tantosha_cd
                        , upd_tammatsu = rec_i_r4g_atena.upd_tammatsu
                        WHERE kojin_no = rec_i_r4g_atena.kojin_no
                        AND rec_i_r4g_atena.tenshutsu_ymd BETWEEN kaishi_ymd AND shuryo_ymd
                        AND shiensochi_kbn IN(1, 2)  -- 1または2（3：終了のデータは更新しない）
                        AND del_flg = 0;
                    EXCEPTION
                    WHEN OTHERS THEN
                        io_c_err_code := SQLSTATE;
                        io_c_err_text := SQLERRM;
                        RETURN;
                    END;
                    
                    -- 異動フラグを立てた個人に対して記事情報を追加する。
                    OPEN cur_busho;
                        LOOP
                            FETCH cur_busho INTO rec_busho;
                                BEGIN
                                    INSERT INTO f_kiji(
                                        busho_cd
                                        , seq_no_kiji
                                        , kojin_no
                                        , kodo_yotei_kbn
                                        , kiji_ymd
                                        , kiji_time
                                        , kiji_bunrui_cd
                                        , kiji_bunrui
                                        , kiji_naiyo_cd
                                        , kiji_naiyo
                                        , kosho_hoho_cd
                                        , kosho_hoho
                                        , tainoseiri_kiroku_cd
                                        , tainoseiri_kiroku
                                        , midashi
                                        , kiji_biko
                                        , sessho_flg
                                        , sessho_aite_cd
                                        , sessho_aite
                                        , sessho_basho_cd
                                        , sessho_basho
                                        , tantosha_cd
                                        , tantosha
                                        , busho
                                        , nofu_yotei_ymd
                                        , nofu_yotei_kingaku
                                        , sashiosae_yotei_ymd
                                        , kyocho_hyoji_flg
                                        , ins_datetime
                                        , upd_datetime
                                        , upd_tantosha_cd
                                        , upd_tammatsu
                                        , del_flg
                                    ) VALUES (
                                        rec_busho.busho_cd
                                        , nextval('dlgmain.seq_no_kiji')
                                        , rec_i_r4g_atena.kojin_no
                                        , 1
                                        , get_date_to_num(current_date)
                                        , 0
                                        , 2
                                        , '事務処理用'
                                        , NULL
                                        , NULL
                                        , NULL
                                        , NULL
                                        , '029'
                                        , '住所異動'
                                        , '支援措置対象者異動'
                                        , '支援措置対象者が異動しました。必要に応じて注意喚起情報を作成してください。'
                                        , 0
                                        , 0
                                        , NULL
                                        , 0
                                        , NULL
                                        , NULL
                                        , NULL
                                        , rec_busho.busho
                                        , 0
                                        , 0
                                        , 0
                                        , 1
                                        , current_timestamp
                                        , current_timestamp
                                        , 'RENKEI'
                                        , 'SERVER'
                                        , 0
                                    );
                                 EXCEPTION
                                WHEN OTHERS THEN
                                    io_c_err_code := SQLSTATE;
                                    io_c_err_text := SQLERRM;
                                    RETURN;
                                END;
                        END LOOP;
                        CLOSE cur_busho;
               END LOOP;
         CLOSE cur_main_02;
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
