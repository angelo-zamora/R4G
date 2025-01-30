--------------------------------------------------------
--  DDL for Procedure proc_r4g_henrei
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE proc_r4g_henrei( 
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying )

LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 返戻情報（統合収滞納）                                                                                  */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                  */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                      */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/
DECLARE

	ln_para01                        numeric DEFAULT 0;
	lc_kojin_no                      character varying;
	ln_gyomu_id                      numeric;
	ln_zeimoku_cd                    numeric;
	lc_zeimoku_cd                    character varying;
	lc_henrei_shubetsu_cd            character varying;
	ln_rireki_no                     numeric;
	ln_fuka_nendo                    numeric;
	ln_soto_nendo                    numeric;
	ln_kibetsu_cd                    numeric;
	lc_tsuchisho_no                  character varying;
	lc_sql                           character varying;
	ln_del_flg                       numeric;
	lc_tantosha                      character varying;
	
	ln_shori_count                 	 numeric DEFAULT 0;
	ln_ins_count                     numeric DEFAULT 0;
	ln_upd_count                     numeric DEFAULT 0;
	ln_del_count                     numeric DEFAULT 0;
	ln_err_count                     numeric DEFAULT 0;
	lc_err_text                      character varying(100);
	ln_result_cd                     numeric DEFAULT 0;
	lc_err_cd                        character varying;

	rec_log                          f_renkei_log%ROWTYPE;

   cur_parameter CURSOR FOR
      SELECT * FROM f_renkei_parameter
      WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                     f_renkei_parameter%ROWTYPE;
   
   cur_main CURSOR FOR
      SELECT * FROM i_r4g_henrei
      WHERE saishin_flg = '1' 
      AND result_cd < 8;
   
   rec_main                          i_r4g_henrei%ROWTYPE;

   cur_lock CURSOR FOR
      SELECT * FROM f_henrei_renkei
      WHERE kojin_no = lc_kojin_no
         AND gyomu_id = ln_gyomu_id
         AND zeimoku_cd = lc_zeimoku_cd::numeric
         AND henrei_shubetsu_cd = lc_henrei_shubetsu_cd
         AND rireki_no = ln_rireki_no
         AND fuka_nendo = ln_fuka_nendo
         AND soto_nendo = ln_soto_nendo
         AND kibetsu_cd = ln_kibetsu_cd
         AND tsuchisho_no = lc_tsuchisho_no;
         
   rec_lock              f_henrei_renkei%ROWTYPE;
   rec_henrei            f_henrei_renkei%ROWTYPE;
      
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
	  	    SELECT COUNT(*) INTO ln_del_count FROM f_henrei_renkei;
            lc_sql := 'TRUNCATE TABLE dlgmain.f_henrei_renkei;';
            EXECUTE lc_sql;
         EXCEPTION WHEN OTHERS THEN
            io_c_err_code := SQLSTATE;
            io_c_err_text    := SQLERRM;
            RETURN;
         END;
      END IF;
	  
   -- 3. 中間テーブルデータのエラーチェック
   CALL proc_error_check( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );

   IF io_c_err_code <> '0'  THEN
      RETURN;
   END IF;

   -- 5. 連携データの作成・更新
   ln_shori_count := 0;
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;
                        
         SELECT tantosha INTO lc_tantosha
         FROM t_tantosha
         WHERE tantosha_cd = rec_main.tanto_id;
            
		 ln_shori_count                 := ln_shori_count + 1;
         lc_err_cd                      := '0';
         ln_result_cd                   := 0;
         lc_err_text                    := NULL;
         rec_lock                       := NULL;

         lc_kojin_no := rec_main.atena_no;
         ln_gyomu_id := CASE WHEN rec_main.gyomu_id IS NOT NULL OR rec_main.gyomu_id <> '' THEN rec_main.gyomu_id::numeric ELSE 0 END;
		 lc_zeimoku_cd := rec_main.zeimoku_cd;
         ln_zeimoku_cd := get_r4g_code_conv(1, 3, null, rec_main.zeimoku_cd::character varying);
         lc_henrei_shubetsu_cd := rec_main.henrei_syubetsu;
         ln_rireki_no := CASE WHEN rec_main.rireki_no IS NOT NULL OR rec_main.rireki_no <> '' THEN rec_main.rireki_no::numeric ELSE 0 END;
         ln_fuka_nendo := CASE WHEN rec_main.fuka_nendo IS NOT NULL OR rec_main.fuka_nendo <> '' THEN rec_main.fuka_nendo::numeric ELSE 0 END;
         ln_soto_nendo := CASE WHEN rec_main.soto_nendo IS NOT NULL OR rec_main.soto_nendo <> '' THEN rec_main.soto_nendo::numeric ELSE 0 END;
         ln_kibetsu_cd := CASE WHEN rec_main.kibetsu_cd IS NOT NULL OR rec_main.kibetsu_cd <> '' THEN rec_main.kibetsu_cd::numeric ELSE 0 END;
         lc_tsuchisho_no := rec_main.tsuchisho_no;
         rec_henrei.hihokensha_no  := rec_main.hihokensha_no;
         rec_henrei.jido_kojin_no  := rec_main.jido_atena_no;
         rec_henrei.henrei_chosa_no := CASE WHEN rec_main.henrei_chosa_no IS NOT NULL OR rec_main.henrei_chosa_no <> '' THEN rec_main.henrei_chosa_no::numeric ELSE 0 END;
         rec_henrei.chosa_henrei_kbn := CASE WHEN rec_main.chosa_henrei_kbn IS NOT NULL OR rec_main.chosa_henrei_kbn <> '' THEN rec_main.chosa_henrei_kbn::numeric ELSE 0 END;
         rec_henrei.bunsho_no  := rec_main.bunsho_no;
         rec_henrei.list_name  := rec_main.list_name;
         rec_henrei.henrei_toroku_ymd := CASE WHEN rec_main.henrei_toroku_ymd IS NULL OR rec_main.henrei_toroku_ymd ='0000-00-00' THEN 0 ELSE getdatetonum(to_date(rec_main.henrei_toroku_ymd, 'yyyy-mm-dd')) END;
         rec_henrei.henrei_ymd := CASE WHEN rec_main.henrei_ymd IS NULL OR rec_main.henrei_ymd ='0000-00-00' THEN 0 ELSE getdatetonum(to_date(rec_main.henrei_ymd, 'yyyy-mm-dd')) END;
         rec_henrei.henrei_jiyu_cd := rec_main.henrei_jiyu;
         rec_henrei.saihasso_ymd := CASE WHEN rec_main.re_hasso IS NULL OR rec_main.re_hasso ='0000-00-00' THEN 0 ELSE getdatetonum(to_date(rec_main.re_hasso, 'yyyy-mm-dd')) END;
         rec_henrei.koji_ymd := CASE WHEN rec_main.kouji_ymd IS NULL OR rec_main.kouji_ymd ='0000-00-00' THEN 0 ELSE getdatetonum(to_date(rec_main.kouji_ymd, 'yyyy-mm-dd')) END;
         rec_henrei.koji_sotatsu_ymd := CASE WHEN rec_main.koji_sotatsu_ymd IS NULL OR rec_main.koji_sotatsu_ymd ='0000-00-00' THEN 0 ELSE getdatetonum(to_date(rec_main.koji_sotatsu_ymd, 'yyyy-mm-dd')) END;
         rec_henrei.henkomae_noki_ymd := CASE WHEN rec_main.noki_henko_mae IS NULL OR rec_main.noki_henko_mae ='0000-00-00' THEN 0 ELSE getdatetonum(to_date(rec_main.noki_henko_mae, 'yyyy-mm-dd')) END;
         rec_henrei.henkogo_noki_ymd := CASE WHEN rec_main.noki_henko_ato IS NULL OR rec_main.noki_henko_ato ='0000-00-00' THEN 0 ELSE getdatetonum(to_date(rec_main.noki_henko_ato, 'yyyy-mm-dd')) END;
         rec_henrei.tantosha_cd_henrei := rec_main.tanto_id;
         rec_henrei.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_henrei.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_henrei.upd_tantosha_cd := rec_main.sosasha_cd;
         rec_henrei.upd_tammatsu :=  'SERVER';
         ln_del_flg := CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> '' THEN rec_main.del_flg::numeric ELSE 0 END;

            OPEN cur_lock;
			  FETCH cur_lock INTO rec_lock;
			CLOSE cur_lock;

				IF rec_lock IS NULL THEN
					BEGIN
					   INSERT INTO f_henrei_renkei (
						  kojin_no
						  , gyomu_id
						  , zeimoku_cd
						  , henrei_shubetsu_cd
						  , rireki_no
						  , fuka_nendo
						  , soto_nendo
						  , kibetsu_cd
						  , tsuchisho_no
						  , hihokensha_no
						  , jido_kojin_no
						  , henrei_chosa_no
						  , chosa_henrei_kbn
						  , bunsho_no
						  , list_name
						  , henrei_toroku_ymd
						  , henrei_ymd
						  , henrei_jiyu_cd
						  , saihasso_ymd
						  , koji_ymd
						  , koji_sotatsu_ymd
						  , henkomae_noki_ymd
						  , henkogo_noki_ymd
						  , tantosha_cd_henrei
						  , tantosha_henrei
						  , ins_datetime
						  , upd_datetime
						  , upd_tantosha_cd
						  , upd_tammatsu
						  , del_flg
					   )VALUES(
						  lc_kojin_no
						  , ln_gyomu_id
						  , ln_zeimoku_cd
						  , lc_henrei_shubetsu_cd
						  , ln_rireki_no
						  , ln_fuka_nendo
						  , ln_soto_nendo
						  , ln_kibetsu_cd
						  , lc_tsuchisho_no
						  , rec_henrei.hihokensha_no
						  , rec_henrei.jido_kojin_no
						  , rec_henrei.henrei_chosa_no
						  , rec_henrei.chosa_henrei_kbn
						  , rec_henrei.bunsho_no
						  , rec_henrei.list_name
						  , rec_henrei.henrei_toroku_ymd
						  , rec_henrei.henrei_ymd
						  , rec_henrei.henrei_jiyu_cd
						  , rec_henrei.saihasso_ymd
						  , rec_henrei.koji_ymd
						  , rec_henrei.koji_sotatsu_ymd
						  , rec_henrei.henkomae_noki_ymd
						  , rec_henrei.henkogo_noki_ymd
						  , rec_henrei.tantosha_cd_henrei
						  , lc_tantosha
						  , rec_henrei.ins_datetime
						  , rec_henrei.upd_datetime
						  , rec_henrei.upd_tantosha_cd
						  , rec_henrei.upd_tammatsu
						  , ln_del_flg
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
					   UPDATE f_henrei_renkei
						  SET 
						  hihokensha_no = rec_henrei.hihokensha_no
						  , jido_kojin_no = rec_henrei.jido_kojin_no
						  , henrei_chosa_no = rec_henrei.henrei_chosa_no
						  , chosa_henrei_kbn = rec_henrei.chosa_henrei_kbn
						  , bunsho_no = rec_henrei.bunsho_no
						  , list_name = rec_henrei.list_name
						  , henrei_toroku_ymd = rec_henrei.henrei_toroku_ymd
						  , henrei_ymd = rec_henrei.henrei_ymd
						  , henrei_jiyu_cd = rec_henrei.henrei_jiyu_cd
						  , saihasso_ymd = rec_henrei.saihasso_ymd
						  , koji_ymd =  rec_henrei.koji_ymd
						  , koji_sotatsu_ymd = rec_henrei.koji_sotatsu_ymd
						  , henkomae_noki_ymd = rec_henrei.henkomae_noki_ymd
						  , henkogo_noki_ymd = rec_henrei.henkogo_noki_ymd
						  , tantosha_cd_henrei = rec_henrei.tantosha_cd_henrei
						  , tantosha_henrei = lc_tantosha
						  , upd_datetime =  rec_henrei.upd_datetime
						  , upd_tantosha_cd = rec_henrei.upd_tantosha_cd
						  , upd_tammatsu = rec_henrei.upd_tammatsu
						  , del_flg = ln_del_flg
						  WHERE kojin_no = lc_kojin_no
						  AND gyomu_id = ln_gyomu_id
						  AND zeimoku_cd = lc_zeimoku_cd::numeric
						  AND henrei_shubetsu_cd = lc_henrei_shubetsu_cd
						  AND rireki_no = ln_rireki_no
						  AND fuka_nendo = ln_fuka_nendo
						  AND soto_nendo = ln_soto_nendo
						  AND kibetsu_cd = ln_kibetsu_cd
						  AND tsuchisho_no = lc_tsuchisho_no;
			   
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
            UPDATE i_r4g_henrei
            SET result_cd = ln_result_cd
            , error_cd = lc_err_cd
            , error_text = lc_err_text
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
				AND atena_no = rec_main.atena_no
				AND gyomu_id = rec_main.gyomu_id
				AND zeimoku_cd = rec_main.zeimoku_cd
				AND henrei_syubetsu = rec_main.henrei_syubetsu
				AND rireki_no = rec_main.rireki_no
				AND fuka_nendo = rec_main.fuka_nendo
				AND soto_nendo = rec_main.soto_nendo
				AND kibetsu_cd = rec_main.kibetsu_cd
				AND tsuchisho_no = rec_main.tsuchisho_no;
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
   CALL proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;

   EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;