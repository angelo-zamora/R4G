--------------------------------------------------------
--  DDL for Procedure proc_r4g_mynumber
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE proc_r4g_mynumber(
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying 
)

LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 振替口座情報（統合収滞納）                                                                               */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                  */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                      */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                         */
/**********************************************************************************************************************/
DECLARE

	ln_shori_count                 numeric DEFAULT 0;
	ln_ins_count                   numeric DEFAULT 0;
	ln_upd_count                   numeric DEFAULT 0;
	ln_del_count                   numeric DEFAULT 0;
	ln_err_count                   numeric DEFAULT 0;
	lc_err_cd                      character varying;
	lc_err_text                    character varying(100);
	ln_result_cd                   numeric DEFAULT 0;

	lc_sql                        character varying;
	rec_log                        f_renkei_log%ROWTYPE;

   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_atena AS atena1
      LEFT JOIN(
         SELECT
            shikuchoson_cd,
            atena_no,
            MAX(rireki_no) AS max_rireki_no,
            MAX(rireki_no_eda) AS max_rireki_no_eda
         FROM i_r4g_atena
         GROUP BY
            atena_no,
         rireki_no
      ) AS atena2
      ON atena1.shikuchoson_cd = atena2.shikuchoson_cd
         AND atena1.atena_no = atena2.atena_no
         AND atena1.rireki_no = atena2.max_rireki_no
         AND atena1.rireki_no_eda = atena2.max_rireki_no_eda
   WHERE saishin_flg = '1'
      AND result_cd < 8;
   /*SELECT *
   FROM i_r4g_jutogai_atena t1
   WHERE t1.saishin_flg = '1'
   AND t1.result_cd < 8
   AND t1.rireki_no = (
         SELECT MAX(t2.rireki_no)
         FROM i_r4g_jutogai_atena t2
         WHERE t2.atena_no = t1.atena_no
            AND t2.result_cd < 8
            AND t2.saishin_flg = '1'
   )
   AND t1.seq_no_renkei = (
         SELECT MAX(t3.seq_no_renkei)
         FROM i_r4g_jutogai_atena t3
         WHERE t3.atena_no = t1.atena_no
            AND t3.rireki_no = t1.rireki_no
            AND t3.result_cd < 8
            AND t3.saishin_flg = '1'
   );*/

   rec_main              i_r4g_jutogai_atena%ROWTYPE;
   
   cur_lock CURSOR FOR
   SELECT *
   FROM f_kojin_mynumber
   WHERE kojin_no = lc_kojin_no;

   rec_lock                       f_kojin_mynumber%ROWTYPE;
 
BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;
 
   -- １．パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;--マイナンバー同一人取得区分
         IF rec_parameter.parameter_no = 9 THEN ln_para09 := rec_parameter.parameter_value; END IF;--検索用カナ設定区分
         IF rec_parameter.parameter_no = 12 THEN ln_para12 := rec_parameter.parameter_value; END IF;--同一人情報
      END LOOP;
   CLOSE cur_parameter;

   -- ２．連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_kojin;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_kojin_mynumber';
         EXECUTE lc_sql;
      EXCEPTION WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text := SQLERRM;
         RETURN;
      END;
   END IF;

   -- 3. 中間テーブルデータのエラーチェックは不要

   -- 4. 桁数設定情報取得
   -- r4gでは不要
   
   -- 5. 連携データの作成・更新
   ln_shori_count := 0;
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

         ln_shori_count                 := ln_shori_count + 1;
         lc_err_cd                      := '0';
         ln_result_cd                   := 0;
         lc_err_text                    := NULL;
         rec_lock                       := NULL;
		 
         lc_kojin_no := rec_main.atena_no;
 
         IF rec_main.del_flg = 1 THEN
            BEGIN
               DELETE FROM f_kojin_number
               WHERE kojin_no = lc_kojin_no;

               GET DIAGNOSIS ln_del_count := ln_del_count + ROW_COUNT;
               lc_err_text := '';
               lc_err_cd := '0';
               ln_result_cd := 3;

            EXCEPTION WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
               lc_err_cd := '9';
               ln_result_cd := 9;
            END;
         ELSE
            BEGIN
               OPEN cur_lock;
                  FETCH cur_lock INTO rec_lock; 
               CLOSE cur_lock;
               
               IF NOT FOUND THEN
                  INSERT INTO f_kojin_number (
                     kojin_no
                     , mynumber
					 , hojin_no
                     , ins_datetime											
                     , upd_datetime											
                     , upd_tantosha_cd											
                     , upd_tammatsu											
                     , del_flg											
                  ) VALUES (
                     rec_main.atena_no
                     , rec_main.mynumber
					 , NULL
                     , concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
                     , concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
                     , rec_main.sosasha_cd
                     , 'SERVER'
                     , rec_main.del_flg
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
               ELSE
                  UPDATE f_kojin_number
                  SET
                     mynumber = rec_main.mynumber
                     , upd_datetime = concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
                     , upd_tantosha_cd = rec_main.sosasha_cd
                     , upd_tammatsu = 'SERVER'
                     , del_flg = rec_main.del_flg
                     WHERE kojin_no = lc_kojin_no;

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
               END IF;

            END;      
         END IF;
		-- 中間テーブル更新
         UPDATE i_r4g_jutogai_atena
         SET result_cd = ln_result_cd
            , error_cd = lc_err_cd
            , error_text = lc_err_text
         WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               atena_no = rec_main.atena_no
               rireki_no = rec_main.rireki_no;
      END LOOP;
   CLOSE cur_main;
   
	rec_log.seq_no_renkei := in_n_renkei_seq;
	rec_log.proc_shuryo_datetime := CURRENT_TIMESTAMP;
	rec_log.proc_shori_count := ln_shori_count;
	rec_log.proc_ins_count := ln_ins_count;
	rec_log.proc_upd_count := ln_upd_count;
	rec_log.proc_del_count := ln_del_count;
	/*   rec_log.proc_jogai_count := ln_jogai_count;
	rec_log.proc_alert_count := ln_alert_count;*/
	rec_log.proc_err_count := ln_err_count;
   
   -- 更新内容は連携ツールの連携処理クラス（RenkeiProcess）の処理：insertRenkeiKekkaを参照
   CALL proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;
   COMMIT;

   EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      ROLLBACK;
      RETURN;

END;
$$;