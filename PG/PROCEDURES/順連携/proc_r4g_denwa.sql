--------------------------------------------------------
--  DDL for Procedure proc_r4g_denwa
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_denwa ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, in_n_para07 IN numeric, io_c_err_code INOUT character varying, io_c_err_text INOUT character varying )
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

   ln_busho_cd                         numeric;
   ln_remban                           numeric;
   ln_i                                numeric default 0 ;
   ln_renkei_rec_count                 numeric;
   ln_rec_count                        numeric;
   ln_zeimoku_cd                       numeric;
   ln_gyomu_cd                         numeric;
   ln_para01                           numeric;
   ln_para07                           numeric;
   
   ln_shori_count                      numeric;
   ln_ins_count                        numeric;
   ln_upd_count                        numeric;
   ln_del_count                        numeric;
   ln_err_count                        numeric;
   lc_err_text                         character varying(100);
   ln_result_cd                        numeric DEFAULT 0;
   ln_yusen_flg                        numeric DEFAULT 0;
   lc_err_cd                           character varying;
   
   lc_kojin_no                         character varying;
   lc_denwa                            character varying;
   lc_zeimoku_cd                       character varying;

   lc_sql                              character varying(1000);

   rec_log                             f_renkei_log%ROWTYPE;
   rec_f_denwa                         f_denwa%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                       f_renkei_parameter%ROWTYPE;

   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_sofu_renrakusaki
   WHERE saishin_flg = '1'
   AND sofu_kbn = '99'
   AND (tel_no  IS NOT NULL or tel_no <> '')
   AND sofu_rireki_no = MAX(atena_no)
   AND result_cd < 8 ;

   rec_main                            i_r4g_sofu_renrakusaki%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM t_busho
   WHERE del_flg = 0;
   
   rec_busho                           t_busho%ROWTYPE;

   cur_remban CURSOR FOR
   SELECT COALESCE(SUM(CASE WHEN del_flg = 0 THEN 1 ELSE 0 END), 0) AS yuko_count,
      COALESCE(SUM(renkei_flg), 0) AS renkei_count,
      COALESCE(CASE WHEN SUM(renkei_flg) > 0 THEN MAX(renkei_flg * remban) ELSE MAX(remban) + 1 END, 1) AS renkei_remban,
      COALESCE(MIN(CASE WHEN del_flg = 0 AND yusen_flg = 1 AND renkei_flg = 0 THEN 1
         WHEN del_flg = 0 AND yusen_flg = 1 AND renkei_flg = 1 THEN 2
         ELSE 9
         END), 9) AS yusen_kbn
   FROM f_denwa
   WHERE busho_cd = rec_f_denwa.busho_cd
   AND kojin_no = rec_f_denwa.kojin_no;

   rec_remban                         f_denwa%ROWTYPE;

BEGIN
   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   ln_shori_count := 0;
   ln_rec_count := 0;
   io_c_err_code := 0;

   --パラメータ情報の取得
   OPEN cur_parameter;
   LOOP
      FETCH cur_parameter INTO rec_parameter;
      EXIT WHEN NOT FOUND;
         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 7 THEN ln_para07 := rec_parameter.parameter_value; END IF;
   END LOOP;
   CLOSE cur_parameter;

   --連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_denwa;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_denwa';
         EXECUTE lc_sql;
      EXCEPTION
         WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text := SQLERRM;
         RETURN;
      END; 
   END IF;

   --中間テーブルデータのエラーチェック
   CALL proc_error_check(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

   IF io_c_err_code <> '0' THEN
      RETURN;
   END IF;

   --連携データの作成・更新

   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

            ln_zeimoku_cd := get_r4g_code_conv(1, 3, null, rec_main.zeimoku_cd::character varying);
            ln_gyomu_cd :=  rec_main.gyomu_id::numeric;
            lc_kojin_no :=  rec_main.atena_no;
            lc_denwa :=  get_trimmed_space(rec_main.tel_no);
            ln_shori_count   := ln_shori_count + 1;

            OPEN cur_lock;
            LOOP
               FETCH cur_lock INTO rec_busho;
               EXIT WHEN NOT FOUND;

               ln_busho_cd :=  rec_busho.busho_cd::numeric;

               SELECT COALESCE( SIGN( SUM( CASE WHEN renkei_flg = 1 THEN 1 ELSE 0 END )), 0 ),COALESCE( CASE SIGN( SUM( CASE WHEN renkei_flg = 1 THEN 1 ELSE 0 END ) ) WHEN 1 THEN MAX( CASE WHEN renkei_flg = 1 THEN renkei_flg ELSE remban END ) ELSE MAX( remban ) + 1 END, 1 ),COUNT(*)
               FROM f_denwa INTO ln_renkei_rec_count, rec_f_denwa.remban, ln_rec_count
               WHERE busho_cd = ln_busho_cd
               AND kojin_no = rec_main.kojin_no
               AND gyomu_id = CASE WHEN rec_main.gyomu_id IS NOT NULL OR rec_main.gyomu_id <> '' THEN rec_main.gyomu_id::numeric ELSE 0 END
               AND zeimoku_cd = ln_zeimoku_cd;

               IF in_n_para07 = 1 THEN
                  IF ln_rec_count <> 0 AND ln_rec_count <> ln_renkei_rec_count THEN
                     BEGIN
                        UPDATE f_denwa
                           SET yusen_flg = 0
                           WHERE busho_cd = rec_busho.busho_cd
                           AND kojin_no = rec_main.kojin_no;
                        EXCEPTION
                        WHEN OTHERS THEN NULL;
                     END;
                  END IF;
                  
                  rec_f_denwa.yusen_flg := 1;
               ELSE
                  IF ln_rec_count <> 0 AND ln_rec_count <> ln_renkei_rec_count THEN
                     rec_f_denwa.yusen_flg := 1;
                  ELSE
                     rec_f_denwa.yusen_flg := 0;
                  END IF;
               END IF;


               ln_yusen_flg   := 0;

               rec_remban     := NULL;
               rec_f_denwa      := NULL;
               rec_f_denwa.busho_cd         := ln_busho_cd;
               rec_f_denwa.kojin_no         := rec_main.kojin_no;

               -- 連番取得処理
               OPEN cur_remban;
                  FETCH cur_remban INTO rec_remban;
               CLOSE CUR_remban;

               -- PARA07：1（連携データを優先電話番号としてデータ更新する（NULL以外））
               IF in_n_para07 = 1 THEN
                  ln_yusen_flg := 1;
               END IF;

               IF ln_yusen_flg = 0 AND rec_remban.yuko_count = 0 THEN
                  ln_yusen_flg := 1;
               END IF;

               IF ln_yusen_flg = 0 AND rec_remban.yusen_kbn = 9 THEN
                  ln_yusen_flg := 1;
               END IF;

               IF ln_yusen_flg = 0 THEN
                  BEGIN
                     SELECT yusen_flg
                     INTO ln_yusen_flg
                     FROM f_denwa
                     WHERE busho_cd = rec_f_denwa.busho_cd
                        AND kojin_no = rec_main.kojin_no
                        AND remban = rec_remban.renkei_remban

                        AND del_flg = 0;
                  EXCEPTION
                     WHEN OTHERS THEN
                        ln_yusen_flg := 0;
                  END;
               END IF;

               -- PARA07：1（連携データを優先電話番号としてデータ更新する（NULL以外））
               IF ln_para07 = 1 OR ln_yusen_flg = 1 THEN
                  BEGIN
                     UPDATE f_denwa
                     SET yusen_flg = 0
                     WHERE busho_cd = rec_f_denwa.busho_cd
                     AND kojin_no = rec_main.kojin_no;
                  EXCEPTION
                     WHEN OTHERS THEN NULL;
                  END;
               END IF;

               rec_f_denwa.remban            := rec_remban.renkei_remban;
               rec_f_denwa.yusen_flg         := ln_yusen_flg;
               rec_f_denwa.denwa_no          := rec_main.tel_no;
               rec_f_denwa.denwa_bunrui_kbn  := rec_main.renrakusaki_kbn;
               rec_f_denwa.biko_denwa        := NULL;
               rec_f_denwa.renkei_flg        := 1;

               IF rec_main.del_flg = 1 THEN
                  BEGIN
                     UPDATE f_denwa
                        SET  del_flg = 1 
                        WHERE busho_cd = ln_busho_cd 
                        AND kojin_no = CASE WHEN rec_main.sofu_rireki_no IS NOT NULL OR rec_main.sofu_rireki_no <> '' THEN rec_main.sofu_rireki_no::numeric ELSE 0 END
                        AND remban = rec_f_denwa.remban;

                        ln_upd_count := ln_upd_count + 1;
                        lc_err_cd    := '0';
                        lc_err_text  := '';
                        ln_result_cd := 2;
                     
                     EXCEPTION
                     WHEN OTHERS THEN
                        ln_err_count := ln_err_count + 1;
                        lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                        lc_err_cd := '9';
                        ln_result_cd := 9;
                     END;
               END IF;

               IF ln_rec_count = 0 THEN
                  BEGIN
                  -- 登録処理
                  INSERT INTO f_denwa(
                  busho_cd
                  , kojin_no
                  , remban
                  , yusen_flg
                  , denwa_no
                  , denwa_bunrui_kbn
                  , biko_denwa
                  , gyomu_cd
                  , zeimoku_cd
                  , renkei_flg
                  , sofu_rireki_no
                  , ins_datetime
                  , upd_datetime
                  , upd_tantosha_cd
                  , upd_tammatsu
                  , del_flg)
                  VALUES (
                  ln_busho_cd 
                  , rec_main.atena_no
                  , rec_f_denwa.remban 
                  , CASE WHEN rec_f_denwa.yusen_flg IS NOT NULL OR rec_f_denwa.yusen_flg <> '' THEN rec_f_denwa.yusen_flg::numeric ELSE 0 END 
                  , lc_denwa
                  , CASE WHEN rec_f_denwa.denwa_bunrui_kbn IS NOT NULL OR rec_f_denwa.denwa_bunrui_kbn <> '' THEN rec_f_denwa.denwa_bunrui_kbn::numeric ELSE 0 END
                  , rec_f_denwa.biko_denwa
                  , CASE WHEN rec_main.gyomu_id IS NOT NULL OR rec_main.gyomu_id <> '' THEN rec_main.gyomu_id::numeric ELSE 0 END
                  , ln_zeimoku_cd
                  , rec_f_denwa.renkei_flg 
                  , CASE WHEN rec_main.sofu_rireki_no IS NOT NULL OR rec_main.sofu_rireki_no <> '' THEN rec_main.sofu_rireki_no::numeric ELSE 0 END
                  , concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                  , concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                  , rec_main.sosasha_cd
                  , 'SERVER'
                  , rec_main.del_flg::numeric);

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
                        UPDATE f_denwa
                           SET  yusen_flg = CASE WHEN rec_f_denwa.yusen_flg IS NOT NULL OR rec_f_denwa.yusen_flg <> '' THEN rec_f_denwa.yusen_flg::numeric ELSE 0 END 
                           , denwa_no = lc_denwa
                           , denwa_bunrui_kbn = CASE WHEN rec_f_denwa.denwa_bunrui_kbn IS NOT NULL OR rec_f_denwa.denwa_bunrui_kbn <> '' THEN rec_f_denwa.denwa_bunrui_kbn::numeric ELSE 0 END
                           , gyomu_cd = CASE WHEN rec_main.gyomu_id IS NOT NULL OR rec_main.gyomu_id <> '' THEN rec_main.gyomu_id::numeric ELSE 0 END
                           , zeimoku_cd = ln_zeimoku_cd
                           , sofu_rireki_no = CASE WHEN rec_main.sofu_rireki_no IS NOT NULL OR rec_main.sofu_rireki_no <> '' THEN rec_main.sofu_rireki_no::numeric ELSE 0 END
                           , upd_datetime = CURRENT_TIMESTAMP
                           , upd_tantosha_cd = rec_main.sosasha_cd
                           , upd_tammatsu = 'SERVER'
                           , del_flg = CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> '' THEN rec_main.del_flg::numeric ELSE 0 END
                           WHERE busho_cd = ln_busho_cd 
                           AND kojin_no = CASE WHEN rec_main.sofu_rireki_no IS NOT NULL OR rec_main.sofu_rireki_no <> '' THEN rec_main.sofu_rireki_no::numeric ELSE 0 END
                           AND remban = rec_f_denwa.remban;

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
           
               BEGIN 
               -- 中間テーブル更新
                  UPDATE i_r4g_sofu_renrakusaki
                     SET result_cd = ln_result_cd
                     , error_cd = lc_err_cd
                     , error_text = lc_err_text
                     WHERE  
                     shikuchoson_cd = rec_main.shikuchoson 
                     AND atena_no = rec_main.kojin_no
                     AND gyomu_id = CASE WHEN rec_main.gyomu_id IS NOT NULL OR rec_main.gyomu_id <> '' THEN rec_main.gyomu_id::numeric ELSE 0 END
                     AND zeimoku_cd = ln_zeimoku_cd
                     AND keiji_kanri_no = rec_main.keiji_kanri_no
                     AND sofu_rireki_no = CASE WHEN rec_main.sofu_rireki_no IS NOT NULL OR rec_main.sofu_rireki_no <> '' THEN rec_main.sofu_rireki_no::numeric ELSE 0 END;
               EXCEPTION
                     WHEN OTHERS THEN
                        ln_err_count := ln_err_count + 1;
                        lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                        lc_err_cd := '9';
                        ln_result_cd := 9;
               END;
         END LOOP;
		 CLOSE cur_lock;
      END LOOP;
   CLOSE cur_main;

   CALL proc_kojin_denwa_chofuku_upd(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

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
