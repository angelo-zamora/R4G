--------------------------------------------------------
--  DDL for Procedure  proc_r4g_tokusoku
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_tokusoku ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, io_c_err_code INOUT character varying, io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 機能概要 : 更新処理（汎用連携用）                                                                                          */
/* 入力 IN  : in_n_renkei_data_cd  連携データコード                                                                       */
/*            in_n_renkei_seq      連携SEQ（連携の一意性を保つSEQ）                                                        */
/*            in_n_shori_ymd       処理日 （連携の処理を行う日付）                                                       */
/* 出力 OUT : io_c_err_code        結果エラーが発生した場合のエラーコード                                                              */
/*            io_c_err_text        結果エラーが発生した場合のエラーメッセージ                                                               */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 更新履歴 :                                                                                                     */
/**********************************************************************************************************************/

DECLARE

   rec_tokusoku                   f_tokusoku_kibetsu%ROWTYPE;
   rec_log                        f_renkei_log%ROWTYPE;

   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_cd                      character varying;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;

   ln_para01                      numeric DEFAULT 0;

   lc_sql                         character varying(1000);

   ln_tsuchisho_no_length         type_tsuchisho_no_length[];
   ln_kojin_no_length             numeric DEFAULT 15;

   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_tokusoku AS tokusoku1
   INNER JOIN (
      SELECT 
	     fuka_nendo
	     , soto_nendo
	     , tsuchisho_no
	     , zeimoku_cd
	     , tokucho_shitei_no
	     , kibetsu_cd
	     , shinkoku_rireki_no
	     , jigyo_nendo_no
	     , jido_atena_no
	     , atena_no
	     , MAX(rireki_no) AS rireki_no
	  FROM i_r4g_tokusoku
	  GROUP BY
	     fuka_nendo
	     , soto_nendo
	     , tsuchisho_no
	     , zeimoku_cd
	     , tokucho_shitei_no
	     , kibetsu_cd
	     , shinkoku_rireki_no
	     , jigyo_nendo_no
	     , jido_atena_no
	     , atena_no
	  ) AS tokusoku2
	  ON tokusoku1.fuka_nendo = tokusoku2.fuka_nendo
	  AND tokusoku1.soto_nendo = tokusoku2.soto_nendo
	  AND tokusoku1.tsuchisho_no = tokusoku2.tsuchisho_no
	  AND tokusoku1.zeimoku_cd = tokusoku2.zeimoku_cd
	  AND tokusoku1.tokucho_shitei_no = tokusoku2.tokucho_shitei_no
	  AND tokusoku1.kibetsu_cd = tokusoku2.kibetsu_cd
	  AND tokusoku1.shinkoku_rireki_no = tokusoku2.shinkoku_rireki_no
	  AND tokusoku1.jigyo_nendo_no = tokusoku2.jigyo_nendo_no
	  AND tokusoku1.jido_atena_no = tokusoku2.jido_atena_no
	  AND tokusoku1.atena_no = tokusoku2.atena_no
	  AND tokusoku1.rireki_no = tokusoku2.rireki_no
   WHERE saishin_flg = '1'
      AND result_cd < 8;

   rec_main                       i_r4g_tokusoku%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;
   
   cur_lock CURSOR FOR
   SELECT *
   FROM f_tokusoku_kibetsu
   WHERE kibetsu_key = rec_tokusoku.kibetsu_key;

   rec_lock                       f_tokusoku_kibetsu%ROWTYPE;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   -- 一件分のデータを取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN 
		    ln_para01 := rec_parameter.parameter_value; 
		 END IF;
      END LOOP;
   CLOSE cur_parameter;

   -- 二件分のデータを更新
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_tokusoku_kibetsu;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_tokusoku_kibetsu';
         EXECUTE lc_sql;
      EXCEPTION
         WHEN OTHERS THEN
            io_c_err_code := SQLSTATE;
            io_c_err_text := SQLERRM;
			RETURN;
      END;
   END IF;

   -- 三件分のレコードを削除
   CALL proc_error_check( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );

   IF io_c_err_code <> 0 THEN
      RETURN;
   END IF;

   -- 四件分のデータを取得
   -- r4gによる処理
   
   ln_shori_count := 0;

   -- 五件分のデータを作成・登録
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

		   ln_shori_count                             := ln_shori_count + 1;
         lc_err_cd                                  := '0';
         ln_result_cd                               := 0;
         lc_err_text                                := NULL;
         rec_tokusoku                               := NULL;
         rec_lock                                   := NULL;

         rec_tokusoku.kibetsu_key                   := get_kibetsu_key(rec_main.fuka_nendo::numeric, CASE WHEN rec_main.soto_nendo IS NOT NULL OR rec_main.soto_nendo <> '' THEN rec_main.soto_nendo::numeric ELSE 0 END, rec_main.zeimoku_cd,
                                                          get_r4g_cd(rec_main.zeimoku_cd, '3'), CASE WHEN rec_main.kibetsu_cd IS NOT NULL OR rec_main.kibetsu_cd <> '' THEN rec_main.kibetsu_cd ELSE '0' END, rec_main.tokucho_shitei_no, 
                                                          rec_main.jido_atena_no, rec_main.tsuchisho_no, CASE WHEN rec_main.jigyo_nendo_no IS NOT NULL OR rec_main.jigyo_nendo_no <> '' THEN rec_main.jigyo_nendo_no::numeric ELSE 0 END,
                                                          rec_main.shinkoku_rireki_no::numeric);
         rec_tokusoku.tokusoku_ymd                  := CASE WHEN rec_main.jigyo_kaishi_ymd IS NULL OR rec_main.jigyo_kaishi_ymd = '0000-00-00' THEN 0 
		                                                  ELSE get_date_to_num(to_date(rec_main.jigyo_kaishi_ymd, 'yyyy-mm-dd')) END;
         rec_tokusoku.tokusoku_henrei_ymd           := CASE WHEN rec_main.tokusoku_henrei_ymd IS NULL OR rec_main.tokusoku_henrei_ymd = '0000-00-00' THEN 0 
		                                                  ELSE get_date_to_num(to_date(rec_main.tokusoku_henrei_ymd, 'yyyy-mm-dd')) END;
         rec_tokusoku.tokusoku_kbn                  := CASE WHEN rec_main.tokusoku_kbn IS NOT NULL OR rec_main.tokusoku_kbn <> '' THEN rec_main.tokusoku_kbn::numeric ELSE 0 END;
         rec_tokusoku.hikinuki_kbn                  := CASE WHEN rec_main.hikinuki_del_kbn IS NOT NULL OR rec_main.hikinuki_del_kbn <> '' THEN rec_main.hikinuki_del_kbn::numeric ELSE 0 END;
         rec_tokusoku.hikinuki_jiyu_cd              := CASE WHEN rec_main.hikinuki_del_jiyu IS NOT NULL OR rec_main.hikinuki_del_jiyu <> '' THEN rec_main.hikinuki_del_jiyu::numeric ELSE 0 END;
         rec_tokusoku.rireki_no                     := CASE WHEN rec_main.rireki_no IS NOT NULL OR rec_main.rireki_no <> '' THEN rec_main.rireki_no::numeric ELSE 0 END;
         rec_tokusoku.ins_datetime                  := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_tokusoku.upd_datetime                  := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_tokusoku.upd_tantosha_cd               := rec_main.sosasha_cd;
         rec_tokusoku.upd_tammatsu                  := 'SERVER';
         rec_tokusoku.del_flg                       := CASE WHEN rec_main.del_flg IS NOT NULL OR rec_main.del_flg <> '' THEN rec_main.del_flg::numeric ELSE 0 END;

		 OPEN cur_lock;
		    FETCH cur_lock INTO rec_lock;
		 CLOSE cur_lock;

         IF rec_lock IS NULL THEN
            BEGIN
               INSERT INTO f_tokusoku_kibetsu(
                  kibetsu_key
                  , tokusoku_ymd
                  , tokusoku_henrei_ymd
                  , tokusoku_kbn
                  , hikinuki_kbn
                  , hikinuki_jiyu_cd
                  , rireki_no
                  , ins_datetime
                  , upd_datetime
                  , upd_tantosha_cd
                  , upd_tammatsu
                  , del_flg
               )
               VALUES (
                  rec_tokusoku.kibetsu_key
                  , rec_tokusoku.tokusoku_ymd
                  , rec_tokusoku.tokusoku_henrei_ymd
                  , rec_tokusoku.tokusoku_kbn
                  , rec_tokusoku.hikinuki_kbn
                  , rec_tokusoku.hikinuki_jiyu_cd
                  , rec_tokusoku.rireki_no
                  , rec_tokusoku.ins_datetime
                  , rec_tokusoku.upd_datetime
                  , rec_tokusoku.upd_tantosha_cd
                  , rec_tokusoku.upd_tammatsu
                  , rec_tokusoku.del_flg
               );
				
               ln_ins_count := ln_ins_count + 1;
               lc_err_cd    := '0';
               lc_err_text  := '';
               ln_result_cd := 1;

            EXCEPTION WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_cd    := '9';
               lc_err_text  := SUBSTRING( SQLERRM, 1, 100 );
               ln_result_cd := 9;
            END;
         ELSE
            BEGIN
               UPDATE f_tokusoku_kibetsu
               SET tokusoku_ymd = rec_tokusoku.tokusoku_ymd
                  , tokusoku_henrei_ymd = rec_tokusoku.tokusoku_henrei_ymd
                  , tokusoku_kbn = rec_tokusoku.tokusoku_kbn
                  , hikinuki_kbn = rec_tokusoku.hikinuki_kbn
                  , hikinuki_jiyu_cd = rec_tokusoku.hikinuki_jiyu_cd
                  , rireki_no = rec_tokusoku.rireki_no
                  , upd_datetime = rec_tokusoku.upd_datetime
                  , upd_tantosha_cd = rec_tokusoku.upd_tantosha_cd
                  , upd_tammatsu = rec_tokusoku.upd_tammatsu
                  , del_flg = rec_tokusoku.del_flg
               WHERE kibetsu_key = rec_tokusoku.kibetsu_key;
			   
               ln_upd_count := ln_upd_count + 1;
               lc_err_cd    := '0';
               lc_err_text  := '';
               ln_result_cd := 2;
			   
            EXCEPTION
               WHEN OTHERS THEN
                  ln_result_cd := 9;
                  ln_err_count := ln_err_count + 1;
                  lc_err_cd    := '9';
                  lc_err_text  := SUBSTRING( SQLERRM, 1, 100 );
            END;
         END IF;
         
         BEGIN 
            UPDATE i_r4g_tokusoku
            SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
               , kibetsu_key = rec_tokusoku.kibetsu_key
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND fuka_nendo = rec_main.fuka_nendo
               AND soto_nendo = rec_main.soto_nendo
               AND tsuchisho_no = rec_main.tsuchisho_no
               AND zeimoku_cd = rec_main.zeimoku_cd
               AND tokucho_shitei_no = rec_main.tokucho_shitei_no
               AND kibetsu_cd = rec_main.kibetsu_cd
               AND shinkoku_rireki_no = rec_main.shinkoku_rireki_no
               AND jigyo_nendo_no = rec_main.jigyo_nendo_no
               AND jido_atena_no = rec_main.jido_atena_no
               AND atena_no = rec_main.atena_no
               AND rireki_no = rec_main.rireki_no;
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
   
   -- データ更新処理
   CALL proc_upd_log(rec_log, io_c_err_code, io_c_err_text);
   
   RAISE NOTICE '処理結果 : % | エラーコード : % | 処理時間 : % | 処理内容 : % | 結果コード : %', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;
   
   EXCEPTION WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;
