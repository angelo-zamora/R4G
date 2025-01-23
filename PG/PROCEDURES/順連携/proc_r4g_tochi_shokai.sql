--------------------------------------------------------
--  DDL for Procedure proc_r4g_tochi_shokai
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_tochi_shokai (
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 督促停止情報（統合収滞納）                                                                              */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                  */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                      */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
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
   rec_log                        f_renkei_log%ROWTYPE;

   ln_para01                      numeric DEFAULT 0;
   ln_para02                      numeric DEFAULT 0;
   lc_kojin_no                    character varying;
   lc_sql                         character varying;

   lc_bukken_shozai               character varying;
   lc_chimoku                     character varying;
   lc_chiseki_yuka_menseki        character varying;

   ln_del_count_f_shokai_fudosan  numeric DEFAULT 0;
   ln_del_count_f_shokai_fudosan_kaiso numeric DEFAULT 0;
    
   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;
    
   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_tochi
   WHERE saishin_flg = '1'
      AND kazei_nendo = (SELECT MAX(kazei_nendo) FROM i_r4g_tochi)
      AND tochi_kihon_rireki_no = (SELECT MAX(tochi_kihon_rireki_no) FROM i_r4g_tochi)
      AND result_cd < 8;

   rec_main                       i_r4g_tokusoku_teishi%ROWTYPE;
    
   cur_busho CURSOR FOR
   SELECT *
     FROM t_busho
    WHERE del_flg = 0
   ORDER BY busho_cd;

   rec_busho                       t_busho%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT seq_no_shokai, kanren_seq_no_shokai, kojin_no
     FROM f_shokai_fudosan
    WHERE busho_cd         = rec_busho.busho_cd
      AND kojin_no         = rec_main.KOJIN_NO
      AND bukken_no        = rec_main.bukken_no
      AND bukken_shurui_cd = 1;

   rec_lock                         record;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;
   
   -- 1. パラメータ情報の取得
   OPEN cur_parameter;
   LOOP
      FETCH cur_parameter INTO rec_parameter;
      EXIT WHEN NOT FOUND;

      IF rec_parameter.parameter_no = 1 THEN
         ln_para01 := rec_parameter.parameter_value;

      ELSEIF  rec_parameter.parameter_no = 2  THEN
         ln_para02 := rec_parameter.parameter_value;

      END IF;
   END LOOP;
   CLOSE cur_parameter;

   -- 2. 連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count_f_shokai_fudosan FROM f_shokai_fudosan;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_shokai_fudosan;';
         EXECUTE lc_sql;

        ELECT COUNT(*) INTO ln_del_count_f_shokai_fudosan_kaiso FROM f_shokai_fudosan_kaiso;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_shokai_fudosan_kaiso;';
         EXECUTE lc_sql;
      
         ln_del_count := ln_del_count_f_shokai_fudosan + ln_del_count_f_shokai_fudosan_kaiso;

      EXCEPTION
         WHEN OTHERS THEN
            io_c_err_code := SQLSTATE;
            io_c_err_text    := SQLERRM;

            RETURN;
      END;
   END IF;

   -- 3. 中間テーブルデータのエラーチェック(不要)

   -- 4. 桁数設定情報取得(不要)

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

         lc_kojin_no := rec_main.gimusha_atena_no;

         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;

         OPEN cur_busho;
            LOOP

            FETCH cur_busho INTO rec_busho;
            EXIT WHEN NOT FOUND;

               lc_bukken_shozai := CASE
                                       WHEN ln_para02 = 0 THEN rec_main.tochi_genkyo_jusho  
                                       WHEN ln_para02 = 1 THEN rec_main.tochi_toki_jusho 
                                       WHEN ln_para02 = 2 AND (rec_main.tochi_toki_jusho IS NULL OR rec_main.tochi_toki_jusho = '') THEN rec_main.tochi_genkyo_jusho  
                                       WHEN ln_para02 = 2 AND (rec_main.tochi_toki_jusho IS NOT NULL AND rec_main.tochi_toki_jusho <> '') THEN rec_main.tochi_toki_jusho  
                                       ELSE NULL 
                                    END;

               lc_chimoku  := CASE
                                 WHEN ln_para02 = 2 AND (rec_main.toki_chimoku IS NULL OR rec_main.toki_chimoku = '') THEN rec_main.genkyo_chimoku
                                 WHEN ln_para02 = 2 AND (rec_main.toki_chimoku IS NOT NULL AND rec_main.toki_chimoku <> '') THEN rec_main.toki_chimoku
                                 ELSE rec_main.genkyo_chimoku  
                              END;

               lc_chiseki_yuka_menseki := CASE
                                             WHEN ln_para02 = 2 AND (rec_main.toki_chiseki IS NULL OR rec_main.toki_chiseki = '') THEN rec_main.genkyo_chiseki
                                             WHEN ln_para02 = 2 AND (rec_main.toki_chiseki IS NOT NULL AND rec_main.toki_chiseki != '') THEN rec_main.toki_chiseki
                                             ELSE rec_main.toki_chiseki
                                          END;
 
               IF rec_lock.kojin_no IS NULL THEN
                  BEGIN
                  INSERT INTO f_shokai_fudosan (
                           busho_cd,
                           kojin_no,
                           seq_no_shokai,
                           bukken_no,
                           bukken_shurui_cd,
                           bukken_shozai,
                           bukken_shozai_chiban,
                           kaoku_no,
                           bukken_fugo,
                           chimoku,
                           kozo,
                           chiseki_yuka_menseki,
                           shikichiken_cd,
                           shikichiken_wariai,
                           itto_tatemono_no,
                           itto_tatemono_kozo,
                           senyu_no,
                           senyu_menseki,
                           teitoken_flg,
                           sashiosae_kbn,
                           toki_ymd,
                           uketsuke_no,
                           sashiosae_kikan,
                           sashiosae_kikan_jusho,
                           sashiosae_kikan_yubin_no,
                           sashiosae_kikan_nyuryoku_kbn,
                           sashiosae_kikan_shikuchoson_cd,
                           sashiosae_kikan_machiaza_cd,
                           sashiosae_kikan_todofuken,
                           sashiosae_kikan_shikugunchoson,
                           sashiosae_kikan_machiaza,
                           sashiosae_kikan_banchigohyoki,
                           sashiosae_kikan_jusho_katagaki,
                           sashiosae_kikan_kakutei_jusho,
                           sashiosae_kikan_kokumei_cd,
                           sashiosae_kikan_kokumeito,
                           sashiosae_kikan_kokugai_jusho,
                           sashiosae_kikan_kana,
                           sashiosae_busho,
                           sashiosae_denwa_no,
                           baikyaku_flg,
                           fudosan_no,
                           mochibun,
                           sashiosae_kahi_flg,
                           kanren_seq_no_shokai,
                           renkei_flg,
                           ins_datetime,
                           upd_datetime,
                           upd_tantosha_cd,
                           upd_tammatsu,
                           del_flg
                        ) VALUES (
                           rec_busho.busho_cd,
                           lc_kojin_no,
                           CASE WHEN rec_lock.seq_no_shokai IS NOT NULL THEN rec_lock.seq_no_shokai ELSE SEQ_SHOKAI.NEXTVAL END,
                           rec_main.bukken_no,
                           1,
                           lc_bukken_shozai, 
                           null,
                           null,
                           null,
                           lc_chimoku, 
                           null,
                           lc_chiseki_yuka_menseki, 
                           0,
                           null,
                           null,
                           null,
                           null,
                           null,
                           0,
                           0,
                           0,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           null,
                           0,
                           null,
                           null,
                           0,
                           0,
                           1,
                           concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp,
                           concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp,
                           rec_main.sosasha_cd,
                           'SERVER',
                           rec_main.del_flg::numeric
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
                     UPDATE f_shokai_fudosan
                     SET bukken_shozai = lc_bukken_shozai
                        , chimoku = lc_chimoku
                        , chiseki_yuka_menseki = lc_chiseki_yuka_menseki
                        , kanren_seq_no_shokai = rec_lock.kanren_seq_no_shokai
                        , upd_datetime = concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                        , upd_tantosha_cd = rec_main.sosasha_cd
                        , upd_tammatsu = 'SERVER'
                        , del_flg = rec_main.del_flg::numeric
                     WHERE busho_cd = rec_busho.busho_cd
                        AND kojin_no = lc_kojin_no
                        AND seq_no_shokai = SEQ_SHOKAI.NEXTVAL
                        AND bukken_no = rec_main.bukken_no
                        AND bukken_shurui_cd = 1;

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

          END LOOP;
      CLOSE cur_busho;
		 
       BEGIN
		 -- 中間テーブル更新
         UPDATE i_r4g_tochi 
         SET result_cd = ln_result_cd
            , error_cd = lc_err_cd
            , error_text = lc_err_text
         WHERE shikuchoson_cd = rec_main.shikuchoson_cd
            AND bukken_no = rec_main.bukken_no
            AND kazei_nendo = rec_main.kazei_nendo
            AND tochi_kihon_rireki_no = rec_main.tochi_kihon_rireki_no;
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