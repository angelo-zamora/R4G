--------------------------------------------------------
--  DDL for Procedure proc_r4g_kaoku_shokai
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_kaoku_shokai ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, io_c_err_code INOUT character varying, io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 個人宛名情報連携（電話番号）                                                                               */
/* 引数 IN  :  in_n_renkei_data_cd … 連携データコード                                                                   */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                  */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                */
/*      OUT : io_c_err_code      …例外エラー発生時のエラーコード                                                      */
/*            io_c_err_text    … 例外エラー発生時のエラー内容                                                        */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :                                                                                                         */
/**********************************************************************************************************************/

DECLARE
   
   ln_shori_count                      numeric;
   ln_ins_count                        numeric;
   ln_upd_count                        numeric;
   ln_del_count                        numeric;
   ln_err_count                        numeric;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;
   lc_sql                            character varying(1000);

   rec_log                           f_renkei_log%ROWTYPE;

   cur_main CURSOR FOR
   SELECT *
	FROM i_r4g_kaoku
	WHERE saishin_flg = '1'
	AND result_cd < 8
	AND kazei_nendo = (SELECT MAX(kazei_nendo) FROM i_r4g_kaoku WHERE saishin_flg = '1')
	AND kaoku_kihon_rireki_no = (SELECT MAX(kaoku_kihon_rireki_no) FROM i_r4g_kaoku WHERE saishin_flg = '1' AND kazei_nendo = (SELECT MAX(kazei_nendo) FROM i_r4g_kaoku WHERE saishin_flg = '1'));


   rec_main                          i_r4g_kaoku%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                     f_renkei_parameter%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM f_shokai_fudosan
   WHERE 
   --Add PK and declare
   ;

   rec_lock                       f_shokai_fudosan%ROWTYPE;

BEGIN
   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   --パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;

      END LOOP;
   CLOSE cur_parameter;

   --連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_shokai_fudosan;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_shokai_fudosan';
         EXECUTE lc_sql;
         EXCEPTION
         WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text := SQLERRM;
         RETURN;
      END; 
   END IF;

   --連携データの作成・更新
   ln_shori_count := 0;

   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

         --Declare PK VALUE

            OPEN cur_lock;
               FETCH cur_cur_lock INTO rec_lock;
            CLOSE cur_lock;

            IF rec_lock IS NULL THEN
               BEGIN
                  -- 登録処理
                  INSERT INTO f_shokai_fudosan(						
                  )
                  VALUES (
                  );

                  ln_ins_count := ln_ins_count + 1;
                  lc_err_text := '';
                  lc_err_cd := '0';
                  ln_result_cd := 1;

                  EXCEPTION
                  WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
                  lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                  lc_err_cd := '9';
                  ln_result_cd := 9;
               END;
            ELSE
               -- 連携データの作成・更新
               BEGIN
                  UPDATE f_shokai_fudosan
                     SET  
                  WHERE 
                  ;

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
         END IF;

      END LOOP;
   CLOSE cur_main;

END;
$$;
