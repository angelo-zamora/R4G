--------------------------------------------------------
--  DDL for Procedure proc_r4g_kotei_kyoyu
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE proc_r4g_kotei_kyoyu( 
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying 
)

LANGUAGE plpgsql
AS $$
/**********************************************************************************************************************/
/* 処理概要 : 固定資産税_共有管理（統合収滞納）                                                                       */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                  */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                    */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/

DECLARE

   ln_para01                           numeric DEFAULT 0;
   ln_kazei_nendo                      numeric;      
   lc_kyoyusha_gimusha_kojin_no        character varying;
   lc_kyoyu_shisan_no                  character varying;
   ln_koseiin_renban                   numeric;
   lc_koseiin_gimusha_kojin_no         character varying;
   lc_sql                              character varying;
   ln_del_flg                          numeric;

   ln_shori_count                      numeric DEFAULT 0;
   ln_ins_count                        numeric DEFAULT 0;
   ln_upd_count                        numeric DEFAULT 0;
   ln_del_count                        numeric DEFAULT 0;
   ln_err_count                        numeric DEFAULT 0;
   lc_err_text                         character varying(100);
   ln_result_cd                        numeric DEFAULT 0;
   lc_err_cd                           character varying;
   rec_log                             f_renkei_log%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT * FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                       f_renkei_parameter%ROWTYPE;

   cur_main CURSOR FOR
   SELECT DISTINCT ON (
      kazei_nendo,
      kyoyu_atena_no,
      kyoyu_shisan_no
   ) *
   FROM
      i_r4g_kotei_kyoyu
   WHERE
      saishin_flg = '1'
      AND result_cd < 8
   ORDER BY
      kazei_nendo,
      kyoyu_atena_no,
      kyoyu_shisan_no,
      kyoyu_rireki_no DESC;

   rec_main                            i_r4g_kotei_kyoyu%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT * FROM f_kyoyukanri
   WHERE kazei_nendo   = ln_kazei_nendo
      AND kyoyusha_gimusha_kojin_no = lc_kyoyusha_gimusha_kojin_no
      AND kyoyu_shisan_no = lc_kyoyu_shisan_no
      AND koseiin_renban = ln_koseiin_renban
      AND koseiin_gimusha_kojin_no = lc_koseiin_gimusha_kojin_no;

   rec_lock                            f_kyoyukanri%ROWTYPE;
   rec_kotei                           f_kyoyukanri%ROWTYPE;

BEGIN
      
   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   -- 1. パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN
            ln_para01 := rec_parameter.parameter_value;
         END IF;
      END LOOP;
   CLOSE cur_parameter;
     
   -- 2. 連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_kyoyukanri;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_kyoyukanri;';
         EXECUTE lc_sql;
      EXCEPTION WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text := SQLERRM;
         RETURN;
      END;
   END IF;

   -- 3. 中間テーブルデータのエラーチェック
   CALL proc_error_check( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );

   IF io_c_err_code <> '0'  THEN
      RETURN;
   END IF;
   
   -- 4. 桁数設定情報取得
   -- r4gでは不要
   
   -- 5. 連携データの作成・更新
   ln_shori_count := 0;
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

         ln_shori_count               := ln_shori_count + 1;
         lc_err_cd                    := '0';
         ln_result_cd                 := 0;
         lc_err_text                  := NULL;
         rec_lock                     := NULL;
       
         ln_kazei_nendo := CASE WHEN rec_main.kazei_nendo IS NOT NULL OR rec_main.kazei_nendo <> '' THEN rec_main.kazei_nendo::numeric ELSE 0 END;
         lc_kyoyusha_gimusha_kojin_no := rec_main.kyoyu_atena_no;
         lc_kyoyu_shisan_no := rec_main.kyoyu_shisan_no;
         ln_koseiin_renban := CASE WHEN rec_main.koseiin_renban IS NOT NULL OR rec_main.koseiin_renban <> '' THEN rec_main.koseiin_renban::numeric ELSE 0 END;
         lc_koseiin_gimusha_kojin_no := rec_kotei_kyoyu.koseiin_gimusha_atena_no;
         ln_del_flg := CASE WHEN rec_kotei_kyoyu.del_flg IS NOT NULL OR rec_kotei_kyoyu.del_flg <> '' THEN rec_kotei_kyoyu.del_flg::numeric ELSE 0 END;
         rec_kotei.kyoyu_kbn := CASE WHEN rec_main.kyoyu_kbn IS NOT NULL OR rec_main.kyoyu_kbn <> '' THEN rec_main.kyoyu_kbn::numeric ELSE 0 END;
         rec_kotei.kyoyu_mochibun_kbn := 1;
         rec_kotei.daihyosha_flg := CASE WHEN rec_main.daihyo_flg IS NOT NULL OR rec_main.daihyo_flg <> '' THEN rec_main.daihyo_flg::numeric ELSE 0 END;
         rec_kotei.ido_ymd := CASE WHEN rec_main.ido_ymd IS NULL THEN 0 ELSE get_date_to_num(to_date(rec_main.ido_ymd, 'yyyy-mm-dd')) END;
         rec_kotei.ido_jiyu_cd := CASE WHEN rec_main.ido_jiyu IS NOT NULL OR rec_main.ido_jiyu <> '' THEN rec_main.ido_jiyu::numeric ELSE 0 END;
         rec_kotei.kyoyusha_ninzu := CASE WHEN rec_main.kyoyu_ninzu IS NOT NULL OR rec_main.kyoyu_ninzu <> '' THEN rec_main.kyoyu_ninzu::numeric ELSE 0 END;
         rec_kotei.toki_mochibun_bunshi := get_trimmed_space(rec_main.toki_bunshi);
         rec_kotei.toki_mochibun_bunbo := get_trimmed_space(rec_main.toki_bunbo);
         rec_kotei.genkyo_mochibun_bunshi := get_trimmed_space(rec_main.genkyo_bunshi);
         rec_kotei.genkyo_mochibun_bunbo := get_trimmed_space(rec_main.genkyo_bunbo);
         rec_kotei.kyoyusha_rirekibango := rec_main.kyoyu_rireki_no;
         rec_kotei.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_kotei.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_kotei.upd_tantosha_cd := rec_main.sosasha_cd;
         rec_kotei.upd_tammatsu := 'SERVER';
         
         OPEN cur_lock;
		    FETCH cur_lock INTO rec_lock;
		   CLOSE cur_lock;
		
         IF rec_lock IS NULL THEN
			  BEGIN
				 INSERT INTO f_kyoyukanri (
					kazei_nendo
					, kyoyusha_gimusha_kojin_no
					, kyoyu_shisan_no
					, koseiin_renban
					, koseiin_gimusha_kojin_no
					, kyoyu_kbn
					, kyoyu_mochibun_kbn
					, daihyosha_flg
					, ido_ymd
					, ido_jiyu_cd
					, kyoyusha_ninzu
					, toki_mochibun_bunshi
					, toki_mochibun_bunbo
					, genkyo_mochibun_bunshi
					, genkyo_mochibun_bunbo
					, kyoyusha_rirekibango
					, ins_datetime
					, upd_datetime
					, upd_tantosha_cd
					, upd_tammatsu
					, del_flg
				 ) VALUES (
					ln_kazei_nendo
					, lc_kyoyusha_gimusha_kojin_no
					, lc_kyoyu_shisan_no
					, ln_koseiin_renban
					, lc_koseiin_gimusha_kojin_no
					, rec_kotei.kyoyu_kbn
					, rec_kotei.kyoyu_mochibun_kbn
					, rec_kotei.daihyosha_flg
					, rec_kotei.ido_ymd
					, rec_kotei.ido_jiyu_cd      
					, rec_kotei.kyoyusha_ninzu
					, rec_kotei.toki_mochibun_bunshi
					, rec_kotei.toki_mochibun_bunbo
					, rec_kotei.genkyo_mochibun_bunshi
					, rec_kotei.genkyo_mochibun_bunbo
					, rec_kotei.kyoyusha_rirekibango
					, rec_kotei.ins_datetime
					, rec_kotei.upd_datetime
					, rec_kotei.upd_tantosha_cd
					, rec_kotei.upd_tammatsu
					, ln_del_flg
				 );
				 
				 ln_ins_count := ln_ins_count + 1;
				 lc_err_text := '';
				 lc_err_cd := '0';
				 ln_result_cd := 1;

			  EXCEPTION WHEN OTHERS THEN
				 ln_err_count := ln_err_count + 1;
				 lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
				 lc_err_cd := '9';
				 ln_result_cd := 9;
			  END;
		   ELSE
			  BEGIN
				 UPDATE f_kyoyukanri
				 SET
					kyoyu_kbn = rec_kotei.kyoyu_kbn
					, kyoyu_mochibun_kbn = rec_kotei.kyoyu_mochibun_kbn
					, daihyosha_flg = rec_kotei.daihyosha_flg
					, ido_ymd =  rec_kotei.ido_ymd
					, ido_jiyu_cd = rec_kotei.ido_jiyu_cd
					, kyoyusha_ninzu = rec_kotei.kyoyusha_ninzu
					, toki_mochibun_bunshi = rec_kotei.toki_mochibun_bunshi
					, toki_mochibun_bunbo = rec_kotei.toki_mochibun_bunbo
					, genkyo_mochibun_bunshi = rec_kotei.genkyo_mochibun_bunshi
					, genkyo_mochibun_bunbo = rec_kotei.genkyo_mochibun_bunbo
					, kyoyusha_rirekibango = rec_kotei.kyoyusha_rirekibango
					, upd_datetime = rec_kotei.upd_datetime
					, upd_tantosha_cd = rec_kotei.upd_tantosha_cd
					, upd_tammatsu = rec_kotei.upd_tammatsu
					, del_flg = ln_del_flg
				 WHERE kazei_nendo = ln_kazei_nendo               
					AND kyoyusha_gimusha_kojin_no = lc_kyoyusha_gimusha_kojin_no
					AND kyoyu_shisan_no = lc_kyoyu_shisan_no
					AND koseiin_renban = ln_koseiin_renban
					AND koseiin_gimusha_kojin_no = lc_koseiin_gimusha_kojin_no;

				 ln_upd_count := ln_upd_count + 1;
				 lc_err_text := '';
				 lc_err_cd := '0';
				 ln_result_cd := 2;

			  EXCEPTION WHEN OTHERS THEN
				 ln_err_count := ln_err_count + 1;
				 lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
				 lc_err_cd := '9';
				 ln_result_cd := 9;
			  END;
		   END IF;

         BEGIN
         -- 中間テーブル更新
            UPDATE i_r4g_kotei_kyoyu
            SET result_cd = ln_result_cd
               , error_cd = ln_err_cd
               , error_text = lc_err_text
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND kazei_nendo = rec_main.kazei_nendo
               AND kyoyu_atena_no = rec_main.kyoyu_atena_no
               AND kyoyu_shisan_no = rec_main.kyoyu_shisan_no
               AND kyoyu_rireki_no = rec_main.kyoyu_rireki_no
               AND koseiin_renban = rec_main.koseiin_renban
               AND koseiin_gimusha_atena_no = rec_main.koseiin_gimusha_atena_no;
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
   
   -- 更新内容は連携ツールの連携処理クラス（RenkeiProcess）の処理：insertRenkeiKekkaを参照
   CALL proc_upd_log(rec_log);

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;
   
   EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;