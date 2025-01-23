--------------------------------------------------------
--  DDL for Procedure proc_r4g_mynumber
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_mynumber(
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying 
)

LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : f_個人_マイナンバー（f_kojin_mynumber）の追加／更新／削除を実施する                                          */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                    */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                   */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                 */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                        */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                          */
/*---------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/23  CRESS-INFO.Drexler     新規作成     001o006「住民情報（個人番号あり）」の取込を行う               */
/***********************************************************************************************************************/
DECLARE

   rec_f_kojin_mynumber           f_kojin_mynumber%ROWTYPE;
   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;

   ln_shori_count                 numeric DEFAULT 0;             -- 処理件数用変数
   ln_ins_count                   numeric DEFAULT 0;             -- 追加件数用変数
   ln_upd_count                   numeric DEFAULT 0;             -- 更新件数用変数
   ln_del_count                   numeric DEFAULT 0;             -- 削除件数用変数
   ln_err_count                   numeric DEFAULT 0;             -- エラー件数用変数
   lc_err_cd                      character varying;             -- エラーコード用変数
   lc_err_text                    character varying(100):='';    -- エラー内容用変数
   ln_result_cd                   numeric DEFAULT 0;             -- 結果区分更新用変数
   ln_para01                      numeric DEFAULT 0;
   ln_para02                      numeric DEFAULT 0;
   ln_para09                      numeric DEFAULT 0;
   ln_para12                      numeric DEFAULT 0;
   ln_del_diag_count              numeric DEFAULT 0;
   
   ln_result_cd_add               numeric DEFAULT 1;              -- 追加
   ln_result_cd_upd               numeric DEFAULT 2;              -- 更新
   ln_result_cd_del               numeric DEFAULT 3;              -- 削除
   ln_result_cd_warning           numeric DEFAULT 7;              -- 警告
   ln_result_cd_err               numeric DEFAULT 9;              -- エラー

   lc_err_cd_normal               character varying = '0';        -- 通常
   lc_err_cd_err                  character varying = '9';        -- エラー

   ln_kojin_no_length             numeric DEFAULT 0;              -- 個人番号の文字数用変数
   
   lc_sql                         character varying;              -- SQL文用変数

   -- メインカーソル
   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_atena AS tbl_atena
   WHERE tbl_atena.saishin_flg = '1'
   AND tbl_atena.rireki_no = (
      SELECT MAX(rireki_no)
      FROM i_r4g_atena
      WHERE atena_no = tbl_atena.atena_no
   )
   AND tbl_atena.rireki_no_eda = (
      SELECT MAX(rireki_no_eda)
      FROM i_r4g_atena
      WHERE atena_no = tbl_atena.atena_no
        AND rireki_no = tbl_atena.rireki_no
   )
   AND tbl_atena.result_cd < 8;
  
   rec_main              i_r4g_atena%ROWTYPE;

   -- パラメータ取得カーソル
   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;
   
   -- 行ロック用カーソル
   cur_lock CURSOR FOR
   SELECT *
   FROM f_kojin_mynumber
   WHERE kojin_no = rec_f_kojin_mynumber.atena_no;

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

   -- 個人番号桁数の取得
   BEGIN
      SELECT kojin_no_length
          INTO ln_kojin_no_length
       FROM f_data_kanri_kojin
       WHERE data_kanri_no = 1;

      IF ln_kojin_no_length IS NULL OR ln_kojin_no_length = 0 OR ln_kojin_no_length > 15 THEN
         ln_kojin_no_length := 15;
      END IF;

   EXCEPTION
      WHEN OTHERS THEN
         ln_kojin_no_length := 15;
   END;
   ln_shori_count := 0;
   
   -- メイン処理
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

         ln_shori_count := ln_shori_count + 1;

         -- 個人番号
         IF ln_para02 = 0 THEN
            rec_f_kojin_mynumber.kojin_no  := SUBSTR( LPAD( rec_main.atena_no, 15, 0 ), - ln_kojin_no_length );
         ELSE
            rec_f_kojin_mynumber.kojin_no  :=  get_doitsunin_main( SUBSTR( LPAD( rec_main.atena_no, 15, 0 ), - ln_kojin_no_length ) );
         END IF;

         -- マイナンバー
         rec_f_kojin_mynumber.mynumber := CASE WHEN rec_main.mynumber = '0' THEN NULL ELSE rec_main.mynumber END;
         -- 法人番号
         rec_f_kojin_mynumber.hojin_no := NULL;
         -- データ作成日時
         rec_f_kojin_mynumber.ins_datetime := CONCAT(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時
         rec_f_kojin_mynumber.upd_datetime := CONCAT(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_f_kojin_mynumber.upd_tantosha_cd := rec_main.sosasha_cd;
         -- 更新端末名称
         rec_f_kojin_mynumber.upd_tammatsu := 'SERVER';
         -- 削除フラグ
         rec_f_kojin_mynumber.del_flg := 0;


         -- 削除フラグが「1」の場合は対象データを物理削除する。
         IF rec_main.del_flg = 1 THEN
            BEGIN
               DELETE FROM f_kojin_number
               WHERE kojin_no = rec_f_kojin_mynumber.atena_no;
               GET DIAGNOSTICS ln_del_diag_count := ROW_COUNT;
               ln_del_count = ln_del_count + ln_del_diag_count;

               lc_err_text := '';
               lc_err_cd := lc_err_cd_normal;
               ln_result_cd := ln_result_cd_del; 

            EXCEPTION WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
               lc_err_cd := lc_err_cd_err;
               ln_result_cd := ln_result_cd_err;
            END;
         ELSE               
               OPEN cur_lock;
               FETCH cur_lock INTO rec_lock;
               CLOSE cur_lock;

               IF rec_lock IS NULL THEN
                  BEGIN
                     --データ登録処理
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
                        rec_f_kojin_mynumber.kojin_no
                        , rec_f_kojin_mynumber.mynumber
                        , rec_f_kojin_mynumber.hojin_no
                        , rec_f_kojin_mynumber.ins_datetime
                        , rec_f_kojin_mynumber.upd_datetime
                        , rec_f_kojin_mynumber.upd_tantosha_cd
                        , rec_f_kojin_mynumber.upd_tammatsu
                        , rec_f_kojin_mynumber.del_flg
                     );
                        ln_ins_count := ln_ins_count + 1;
                        lc_err_text := '';
                        lc_err_cd := lc_err_cd_normal;
                        ln_result_cd := ln_result_cd_add;

                     EXCEPTION WHEN OTHERS THEN
                        ln_ins_count := ln_ins_count + 1;
                        lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                        lc_err_cd := lc_err_cd_err;
                        ln_result_cd := ln_result_cd_err;
                     END;
               ELSE
                  BEGIN
                     --データ更新処理
                     UPDATE f_kojin_number
                     SET
                        kojin_no = rec_f_kojin_mynumber.kojin_no
                        ,mynumber = rec_f_kojin_mynumber.mynumber
                        , upd_datetime = rec_f_kojin_mynumber.upd_datetime
                        , upd_tantosha_cd = rec_f_kojin_mynumber.upd_tantosha_cd
                        , upd_tammatsu = rec_f_kojin_mynumber.upd_tammatsu
                        , del_flg = rec_f_kojin_mynumber.del_flg
                        WHERE kojin_no = lc_kojin_no;

                        ln_upd_count := ln_upd_count + 1;
                        lc_err_text := '';
                        lc_err_cd := lc_err_cd_normal;
                        ln_result_cd := ln_result_cd_upd;

                        EXCEPTION WHEN OTHERS THEN
                           ln_err_count := ln_err_count + 1;
                           lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                           lc_err_cd := lc_err_cd_err;
                           ln_result_cd := ln_result_cd_err;
                        END;
               END IF;   
         END IF;

        -- 中間テーブル更新
         UPDATE i_r4g_atena
         SET result_cd = ln_result_cd
            , error_cd = lc_err_cd
            , error_text = lc_err_text
         WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND atena_no = rec_main.atena_no
               AND rireki_no = rec_main.rireki_no;

      END LOOP;
   CLOSE cur_main;
   
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