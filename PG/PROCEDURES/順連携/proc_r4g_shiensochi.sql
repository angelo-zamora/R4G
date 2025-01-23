--------------------------------------------------------
--  DDL for Procedure proc_r4g_shiensochi
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_shiensochi(
	in_n_renkei_data_cd numeric,
	in_n_renkei_seq numeric,
	in_n_shori_ymd numeric,
	INOUT io_c_err_code character varying,
	INOUT io_c_err_text character varying)
LANGUAGE 'plpgsql'
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 期別マスタ情報（統合収滞納）                                                                               */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code      … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :                                                         */
/**********************************************************************************************************************/

DECLARE
   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;

   ln_para01 numeric DEFAULT 0;
   lc_kojin_no character varying;
   ln_kaishi_ymd numeric;
   lc_tokusoku_kaijo_ymd character varying;
   lc_sql character varying;
   
   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter f_renkei_parameter%ROWTYPE;

   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_shiensochi AS shiensochi1
      LEFT JOIN(
         SELECT
            shikuchoson_cd,
            atena_no,
            shiensochi_kaishi_ymd,
            MAX(rireki_no) AS max_rireki_no
         FROM i_r4g_shiensochi
         GROUP BY
            shikuchoson_cd,
            atena_no,
            shiensochi_kaishi_ymd
      ) AS shiensochi2
      ON shiensochi1.shikuchoson_cd = shiensochi2.shikuchoson_cd
         AND shiensochi1.atena_no = shiensochi2.atena_no
         AND shiensochi1.shiensochi_kaishi_ymd = shiensochi2.shiensochi_kaishi_ymd
         AND shiensochi1.rireki_no = shiensochi2.max_rireki_no
   WHERE saishin_flg = '1'
      AND result_cd < 8;

   rec_main i_r4g_shiensochi%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM f_shiensochi
   WHERE kojin_no  = lc_kojin_no 
      AND kaishi_ymd = ln_kaishi_ymd; 

   rec_lock f_shiensochi%ROWTYPE;

   cur_busho CURSOR FOR
   SELECT *
   FROM t_busho
   ORDER BY busho_cd;

   rec_busho            t_busho%ROWTYPE;
   rec_log         f_renkei_log%ROWTYPE;

BEGIN
	
   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;
   
   -- パラメータ情報の取得
   OPEN cur_parameter;
   LOOP
      FETCH cur_parameter INTO rec_parameter;
      EXIT WHEN NOT FOUND;

      IF rec_parameter.parameter_no = 1 THEN
         ln_para01 := rec_parameter.parameter_value;
      END IF;
   END LOOP;
   CLOSE cur_parameter;

   -- 連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_shiensochi;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_shiensochi;';
         EXECUTE lc_sql;

      EXCEPTION
         WHEN OTHERS THEN
            io_c_err_code := SQLSTATE;
            io_c_err_text := SQLERRM;

            RETURN;
      END;
   END IF;

   -- 中間テーブルデータのエラーチェック
   CALL proc_error_check(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

   IF io_c_err_code <> '0' THEN
      RETURN;
   END IF;

   ln_shori_count := 0;
   -- 連携データの作成・更新
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;
		 
         ln_shori_count                 := ln_shori_count + 1;
         lc_err_cd                      := '0';
         ln_result_cd                   := 0;
         lc_err_text                    := NULL;
         rec_busho                      := NULL;
         rec_lock                       := NULL;

         lc_kojin_no := rec_main.atena_no::character varying;
         ln_kaishi_ymd := get_date_to_num(to_date(rec_main.shiensochi_kaishi_ymd, 'yyyy-mm-dd'));
         lc_tokusoku_kaijo_ymd := rec_main.shiensochi_kaishi_ymd;

         OPEN cur_lock;
               FETCH cur_lock INTO rec_lock;
		   CLOSE cur_lock;

			IF rec_lock IS NULL THEN
                  BEGIN
                     INSERT INTO f_shiensochi(
                           kojin_no
                        , kaishi_ymd
                        , shiensochi_kbn
                        , shuryo_ymd
                        , kaijo_shokai_flg
                        , kaijo_shokai_kaishi_datetime
                        , kaijo_shokai_shuryo_datetime
                        , kaijo_hakko_flg
                        , kaijo_hakko_kaishi_datetime
                        , kaijo_hakko_shuryo_datetime
                        , kaijo_hakko_kaisu
                        , ido_flg
                        , shuryo_flg
                        , biko
                        , rireki_no
                        , ins_datetime
                        , upd_datetime
                        , upd_tantosha_cd
                        , upd_tammatsu
                        , del_flg
                     ) VALUES (
                        lc_kojin_no
                        , ln_kaishi_ymd
                        , CASE WHEN rec_main.sochi_kbn IS NOT NULL OR rec_main.sochi_kbn <> '' THEN rec_main.sochi_kbn::numeric ELSE 0 END
                        , CASE WHEN lc_tokusoku_kaijo_ymd IS NULL OR lc_tokusoku_kaijo_ymd = '' THEN 99999999 ELSE get_date_to_num(to_date(rec_main.shiensochi_shuryo_ymd, 'yyyy-mm-dd')) END
                        , 0
                        , null
                        , null
                        , 0
                        , null
                        , null
                        , 1
                        , 0
                        , CASE WHEN rec_main.sochi_kbn = '3' THEN 1 ELSE 0 END
                        , null
                        , CASE WHEN rec_main.rireki_no IS NOT NULL OR rec_main.rireki_no <> '' THEN rec_main.rireki_no::numeric ELSE 0 END
                        , concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                        , concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                        , rec_main.sosasha_cd
                        , 'SERVER'
                        , CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> '' THEN rec_main.del_flg::numeric ELSE 0 END
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
                  BEGIN
                     UPDATE f_shiensochi
                     SET shiensochi_kbn = CASE WHEN rec_main.sochi_kbn IS NOT NULL OR rec_main.sochi_kbn <> '' THEN rec_main.sochi_kbn::numeric ELSE 0 END
                        , shuryo_ymd = CASE WHEN lc_tokusoku_kaijo_ymd IS NULL OR lc_tokusoku_kaijo_ymd = '' THEN 99999999 ELSE get_date_to_num(to_date(rec_main.shiensochi_shuryo_ymd, 'yyyy-mm-dd')) END
                        , shuryo_flg = CASE WHEN rec_main.sochi_kbn = '3' THEN 1 ELSE 0 END
                        , rireki_no = CASE WHEN rec_main.rireki_no IS NOT NULL OR rec_main.rireki_no <> '' THEN rec_main.rireki_no::numeric ELSE 0 END
                        , upd_datetime = concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                        , upd_tantosha_cd = rec_main.sosasha_cd
                        , upd_tammatsu = 'SERVER'
                        , del_flg = CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> '' THEN rec_main.del_flg::numeric ELSE 0 END
                     WHERE 
                        kojin_no  = lc_kojin_no 
                        AND kaishi_ymd = ln_kaishi_ymd;

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

         IF (rec_main.del_flg = '0' AND rec_main.sochi_kbn = '3') OR rec_main.del_flg = '1' THEN
            
            OPEN cur_busho;
               LOOP
                  FETCH cur_busho INTO rec_busho;
                  EXIT WHEN NOT FOUND;
                  
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
                        CASE WHEN rec_busho.busho_cd IS NOT NULL OR rec_busho.busho_cd <> '' THEN rec_busho.busho_cd::numeric ELSE 0 END
                        , nextval('dlgmain.seq_no_kiji')
                        , rec_main.atena_no
                        , 1
                        , get_date_to_num(current_date)
                        , 0
                        , 2
                        , '事務処理用'
                        , NULL
                        , NULL
                        , NULL
                        , NULL
                        , 999
                        , 'その他'
                        , '支援措置終了'
                        , '支援措置終了情報が連携されました。
                     　必要に応じて注意喚起情報を更新してください。'
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
                        , rec_main.sosasha_cd
                        , 'SERVER'
                        , 0
                     );
               END LOOP;
            CLOSE cur_busho;
         END IF;

         BEGIN
            -- 中間テーブル更新
            UPDATE i_r4g_shiensochi
            SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND atena_no = rec_main.atena_no
               AND rireki_no = rec_main.rireki_no
               AND shiensochi_kaishi_ymd = rec_main.shiensochi_kaishi_ymd;
         EXCEPTION
            WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
               lc_err_cd := '9';
               ln_result_cd := 9;
         END;

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
