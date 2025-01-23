----------------------------------------------------
 DDL for Procedure proc_r4g_furikae_koza
----------------------------------------------------
CREATE OR REPLACE PROCEDURE proc_r4g_furikae_koza(
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

	ln_para01                     numeric DEFAULT 0;
	lc_kojin_no                   character varying;
	ln_zeimoku_cd                 numeric;
	lc_zeimoku_cd                 character varying;
	lc_jido_atena_no              character varying;
	ln_koza_rireki_no             numeric;
	lc_sql                        character varying;
	ln_koza_kbn                   numeric;
	ln_del_flg                    numeric;
      
	ln_shori_count                 numeric DEFAULT 0;
	ln_ins_count                   numeric DEFAULT 0;
	ln_upd_count                   numeric DEFAULT 0;
	ln_del_count                   numeric DEFAULT 0;
	ln_err_count                   numeric DEFAULT 0;
	lc_err_text                    character varying(100);
	ln_result_cd                   numeric DEFAULT 0;
	lc_err_cd                      character varying;
	rec_log                        f_renkei_log%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;
   
   cur_main CURSOR FOR
	SELECT DISTINCT ON (
			shikuchoson_cd,
			atena_no,
			zeimoku_cd,
			furikae_kbn,
			jido_atena_no
		) *
	FROM
		i_r4g_furikae_koza
	WHERE
		saishin_flg = '1'
		AND result_cd < 8
	ORDER BY
		shikuchoson_cd,
		atena_no,
		zeimoku_cd,
		furikae_kbn,
		jido_atena_no,
		koza_rireki_no DESC;

   rec_main              i_r4g_furikae_koza%ROWTYPE;
   
   
   cur_lock CURSOR FOR
	SELECT *
	FROM f_kozajoho
	WHERE kojin_no = lc_kojin_no
	AND zeimoku_cd = lc_zeimoku_cd::numeric
	AND koza_kbn = ln_koza_kbn
	AND jido_atena_no = lc_jido_atena_no
	AND koza_rireki_no = ln_koza_rireki_no;


   rec_lock                       f_kozajoho%ROWTYPE;
   rec_furikae                     f_kozajoho%ROWTYPE;

   
BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   1. パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
        FETCH cur_parameter INTO rec_parameter;
        EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN
            ln_para01 := rec_parameter.parameter_value;
         END IF;
      END LOOP;
   CLOSE cur_parameter;

   2. 連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_kozajoho;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_kozajoho;';
         EXECUTE lc_sql;
      EXCEPTION WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text    := SQLERRM;
         RETURN;
	  END;
   END IF;

   3. 中間テーブルデータのエラーチェック
   CALL proc_error_check(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);
      
   IF io_c_err_code <> '0' THEN
      RETURN;
   END IF;

   4. 桁数設定情報取得
   r4gでは不要
   
   5. 連携データの作成・更新
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
		 lc_zeimoku_cd := rec_main.zeimoku_cd;
         ln_zeimoku_cd := get_r4g_code_conv(1, '3', null, rec_main.zeimoku_cd::character varying);
         ln_koza_kbn := CASE WHEN rec_main.furikae_kbn IS NOT NULL OR rec_main.furikae_kbn <> '' THEN rec_main.furikae_kbn::numeric ELSE 0 END;
         lc_jido_atena_no := rec_main.jido_atena_no;
         ln_koza_rireki_no := CASE WHEN rec_main.koza_rireki_no IS NOT NULL OR rec_main.koza_rireki_no <> '' THEN rec_main.koza_rireki_no::numeric ELSE 0 END;
         ln_del_flg := CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> '' THEN rec_main.del_flg::numeric ELSE 0 END;
         rec_furikae.kaishi_ymd := CASE WHEN rec_main.koufuri_kaishi_ymd IS NULL OR rec_main.koufuri_kaishi_ymd ='0000-00-00' THEN 0 ELSE get_date_to_num(rec_main.koufuri_kaishi_ymd::date) END;
         rec_furikae.shuryo_ymd := CASE WHEN rec_main.koufuri_shuryo_ymd IS NULL OR rec_main.koufuri_shuryo_ymd ='0000-00-00' THEN 0 ELSE get_date_to_num(rec_main.koufuri_shuryo_ymd::date) END;
         rec_furikae.kinyu_kikan_cd := rec_main.kinyukikan_cd;
         rec_furikae.kinyu_kikan_shiten_cd := rec_main.tempo_no;
         rec_furikae.yucho_kigo := rec_main.yucho_kigo;
         rec_furikae.yucho_no := rec_main.yucho_no;
         rec_furikae.kinyu_kikan_shubetsu_kbn := CASE WHEN rec_main.kinyukikan_shubetsu IS NOT NULL OR rec_main.kinyukikan_shubetsu <> '' THEN rec_main.kinyukikan_shubetsu::numeric ELSE 0 END;
         rec_furikae.koza_shubetsu_cd := CASE WHEN rec_main.koza_shubtsu IS NOT NULL OR rec_main.koza_shubtsu <> '' THEN rec_main.koza_shubtsu::numeric ELSE 0 END;
         rec_furikae.koza_no := rec_main.koza_no;
         rec_furikae.koza_meiginin_kana := get_trimmed_space(rec_main.koza_meigi_kana);
         rec_furikae.koza_meiginin := get_trimmed_space(rec_main.koza_meigi_kanji);
         rec_furikae.teishi_kaishi_ymd := CASE WHEN rec_main.koza_teishi_kaishi_ymd IS NULL OR rec_main.koza_teishi_kaishi_ymd ='0000-00-00' THEN 0 ELSE get_date_to_num(rec_main.koza_teishi_kaishi_ymd::date) END;
         rec_furikae.teishi_shuryo_ymd := CASE WHEN rec_main.koza_teishi_shuryo_ymd IS NULL OR rec_main.koza_teishi_shuryo_ymd ='0000-00-00' THEN 0 ELSE get_date_to_num(rec_main.koza_teishi_shuryo_ymd::date) END;
         rec_furikae.haishi_ymd := CASE WHEN rec_main.koza_haishi_ymd IS NULL OR rec_main.koza_haishi_ymd ='0000-00-00' THEN 0 ELSE get_date_to_num(rec_main.koza_haishi_ymd::date) END;
         rec_furikae.nofuhoho_kbn := CASE WHEN rec_main.nofuhoho IS NOT NULL OR rec_main.nofuhoho <> '' THEN rec_main.nofuhoho::numeric ELSE 0 END;
         rec_furikae.memo := rec_main.memo;
         rec_furikae.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_furikae.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_furikae.upd_tantosha_cd := rec_main.sosasha_cd;
         rec_furikae.upd_tammatsu :=  'SERVER';

         OPEN cur_lock;
		   FETCH cur_lock INTO rec_lock; 
		 CLOSE cur_lock;	
			  
			IF rec_lock IS NULL THEN
				 BEGIN
					INSERT INTO f_kozajoho (
					   kojin_no,
					   zeimoku_cd,
					   koza_kbn,
					   jido_atena_no,
					   koza_rireki_no,
					   kaishi_ymd,
					   shuryo_ymd,
					   kinyu_kikan_cd,
					   kinyu_kikan_shiten_cd,
					   yucho_kigo,
					   yucho_no,
					   kinyu_kikan_shubetsu_kbn,
					   koza_shubetsu_cd,
					   koza_no,
					   koza_meiginin_kana,
					   koza_meiginin,
					   teishi_kaishi_ymd,
					   teishi_shuryo_ymd,
					   haishi_ymd,
					   nofuhoho_kbn,
					   memo,
					   ins_datetime,
					   upd_datetime,
					   upd_tantosha_cd,
					   upd_tammatsu,
					   del_flg
					) VALUES (
					   lc_kojin_no
					   , ln_zeimoku_cd
					   , ln_koza_kbn
					   , lc_jido_atena_no
					   , ln_koza_rireki_no
					   , rec_furikae.kaishi_ymd
					   , rec_furikae.shuryo_ymd
					   , rec_furikae.kinyu_kikan_cd
					   , rec_furikae.kinyu_kikan_shiten_cd
					   , rec_furikae.yucho_kigo
					   , rec_furikae.yucho_no
					   , rec_furikae.kinyu_kikan_shubetsu_kbn
					   , rec_furikae.koza_shubetsu_cd
					   , rec_furikae.koza_no
					   , rec_furikae.koza_meiginin_kana
					   , rec_furikae.koza_meiginin
					   , rec_furikae.teishi_kaishi_ymd
					   , rec_furikae.teishi_shuryo_ymd
					   , rec_furikae.haishi_ymd
					   , rec_furikae.nofuhoho_kbn
					   , rec_furikae.memo
					   , rec_furikae.ins_datetime
					   , rec_furikae.upd_datetime
					   , rec_furikae.upd_tantosha_cd
					   , rec_furikae.upd_tammatsu
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
					UPDATE f_kozajoho
					SET
					   kaishi_ymd =  rec_furikae.kaishi_ymd,
					   shuryo_ymd =  rec_furikae.shuryo_ymd,
					   kinyu_kikan_cd = rec_furikae.kinyu_kikan_cd,
					   kinyu_kikan_shiten_cd = rec_furikae.kinyu_kikan_shiten_cd,
					   yucho_kigo =  rec_furikae.yucho_kigo,
					   yucho_no = rec_furikae.yucho_no,
					   kinyu_kikan_shubetsu_kbn =  rec_furikae.kinyu_kikan_shubetsu_kbn,
					   koza_shubetsu_cd =  rec_furikae.koza_shubetsu_cd,
					   koza_no = rec_furikae.koza_no,
					   koza_meiginin_kana = rec_furikae.koza_meiginin_kana,
					   koza_meiginin = rec_furikae.koza_meiginin,
					   teishi_kaishi_ymd =  rec_furikae.teishi_kaishi_ymd,
					   teishi_shuryo_ymd = rec_furikae.teishi_shuryo_ymd,
					   haishi_ymd =  rec_furikae.haishi_ymd,
					   nofuhoho_kbn = rec_furikae.nofuhoho_kbn,
					   memo = rec_furikae.memo,
					   upd_datetime = rec_furikae.upd_datetime,
					   upd_tantosha_cd = rec_furikae.upd_tantosha_cd,
					   upd_tammatsu = rec_furikae.upd_tammatsu,
					   del_flg = ln_del_flg
					   WHERE kojin_no = lc_kojin_no
					   AND zeimoku_cd = lc_zeimoku_cd::numeric
					   AND koza_kbn = ln_koza_kbn
					   AND jido_atena_no = lc_jido_atena_no
					   AND koza_rireki_no = ln_koza_rireki_no;
					   
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
			中間テーブル更新
			UPDATE i_r4g_furikae_koza
			SET result_cd = ln_result_cd
				, error_cd = lc_err_cd
				, error_text = lc_err_text
			WHERE shikuchoson_cd = rec_main.shikuchoson_cd
				AND atena_no = rec_main.atena_no
				AND zeimoku_cd = rec_main.zeimoku_cd
				AND furikae_kbn = rec_main.furikae_kbn
				AND jido_atena_no = rec_main.jido_atena_no
				AND koza_rireki_no = rec_main.koza_rireki_no;
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
   
   更新内容は連携ツールの連携処理クラス（RenkeiProcess）の処理：insertRenkeiKekkaを参照
   CALL proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;

   EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$