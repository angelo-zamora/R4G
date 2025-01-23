--------------------------------------------------------
--  DDL for Procedure proc_d_group_kanshi_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_d_group_kanshi_upd ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, out_n_result_code INOUT numeric, out_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 機能概要 : データ変換処理                                                                                           */
/* 入力 IN  : in_n_renkei_data_cd  連携データコード                                                                     */
/*            in_n_renkei_seq      連携SEQ（連携の一意性を保つSEQ）                                                   */
/*            in_n_shori_ymd       処理日 （連携の処理を行う日付）                                                    */
/* 出力 OUT : out_n_result_co      結果エラーが発生した場合のエラーコード                                               */
/*            out_c_err_text       結果エラーが発生した場合のエラーメッセージ                                           */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 更新履歴 : 新規作成                                                                                                */
/**********************************************************************************************************************/

DECLARE
   rec_f_group_kanshi_kojin       dlgmain.f_group_kanshi_kojin%ROWTYPE;
   rec_f_group_kanshi_kibetsu     dlgmain.f_group_kanshi_kibetsu%ROWTYPE;

   ln_i                           numeric DEFAULT 0;
   ln_j                           numeric DEFAULT 0;
   lc_sql                         character varying(10000);

   ln_para01                      numeric DEFAULT 0;
   ln_para02                      numeric DEFAULT 0;
   ln_para03                      numeric DEFAULT 0;
   ln_para04                      numeric DEFAULT 0;
   ln_para05                      numeric DEFAULT 0;

   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;

   ld_kaishi_datetime             timestamp;
   ld_shuryo_datetime             timestamp;

   ln_ins_flg                     numeric DEFAULT 0;

   ln_new_flg                     numeric DEFAULT 0;

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;

   cur_kanshi CURSOR FOR
   SELECT *
   FROM f_group_kanshi_settei
   WHERE kanshi_kaishi_ymd <= get_date_to_num( CURRENT_TIMESTAMP(0)::date )
     AND kanshi_shuryo_ymd >= get_date_to_num( CURRENT_TIMESTAMP(0)::date );

   rec_kanshi                     f_group_kanshi_settei%ROWTYPE;

   cur_kanshi_kojin CURSOR FOR
   SELECT *
   FROM f_group_kanshi_kojin
   WHERE busho_cd = rec_kanshi.busho_cd
     AND group_kanshi_no = rec_kanshi.group_kanshi_no;

   cur_kanshi_kibetsu CURSOR FOR
   SELECT *
   FROM f_group_kanshi_kibetsu
   WHERE busho_cd = rec_kanshi.busho_cd
     AND group_kanshi_no = rec_kanshi.group_kanshi_no;

   rec_kanshi_kojin               f_group_kanshi_kojin%ROWTYPE;
   rec_kanshi_kibetsu             f_group_kanshi_kibetsu%ROWTYPE;
	
	lc_kojin_no character varying(15);

BEGIN

-- 1
   ld_kaishi_datetime := CURRENT_TIMESTAMP(0);

-- 2
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 3 THEN ln_para03 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 4 THEN ln_para04 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 5 THEN ln_para05 := rec_parameter.parameter_value; END IF;
      END LOOP;
   CLOSE cur_parameter;

-- 3
   OPEN cur_kanshi;
      LOOP
         FETCH cur_kanshi INTO rec_kanshi;
         EXIT WHEN NOT FOUND;
      -- 1
         ln_shori_count := ln_shori_count + 1;
      -- 2
         rec_kanshi_kojin.doitsunin_kojin_no := NULL;

         ln_new_flg := 0;

         OPEN cur_kanshi_kojin;
            FETCH cur_kanshi_kojin INTO rec_kanshi_kojin;
         CLOSE cur_kanshi_kojin;

         IF rec_kanshi_kojin.doitsunin_kojin_no IS NULL THEN
            ln_new_flg := 1;
            BEGIN
               INSERT INTO f_group_kanshi_kojin( busho_cd, group_kanshi_no, doitsunin_kojin_no, zeigaku_mino, tokusoku_mino, kasankin_mino, entaikin_mino, kingaku_mino, sessho_flg, sessho_count, saikoku_flg, saikoku_count, bunno_flg, shobun_flg, yuyo_flg, shikkoteishi_flg, kesson_flg, shuno_flg, mino_flg, ins_datetime, upd_datetime, upd_tantosha_cd, upd_tammatsu, del_flg )
               SELECT rec_kanshi.busho_cd, rec_kanshi.group_kanshi_no, f_group_kojin.kojin_no, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, CURRENT_TIMESTAMP(0), CURRENT_TIMESTAMP(0), 'BATCH', 'SERVER', 0
               FROM f_group_kojin
               WHERE f_group_kojin.busho_cd = rec_kanshi.busho_cd
                 AND f_group_kojin.group_no = rec_kanshi.group_no;

               ln_ins_count := ln_ins_count + 1;

            EXCEPTION
               WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
						ROLLBACK;
            END;
         END IF;
      -- 3
         rec_kanshi_kibetsu.kibetsu_key := NULL;

         OPEN cur_kanshi_kibetsu;
            FETCH cur_kanshi_kibetsu INTO rec_kanshi_kibetsu;
         CLOSE cur_kanshi_kibetsu;

         IF rec_kanshi_kibetsu.kibetsu_key IS NULL THEN
            BEGIN
               INSERT INTO f_group_kanshi_kibetsu( busho_cd, group_kanshi_no, kibetsu_key, doitsunin_kojin_no, kojin_no, zeigaku_mino, tokusoku_mino, kasankin_mino, entaikin_mino, kingaku_mino, kanno_cd, sessho_flg, sessho_count, saikoku_flg, saikoku_count, bunno_flg, shobun_flg, yuyo_flg, shikkoteishi_flg, kesson_flg, shuno_flg, ins_datetime, upd_datetime, upd_tantosha_cd, upd_tammatsu, del_flg )
               SELECT rec_kanshi.busho_cd, rec_kanshi.group_kanshi_no, f_taino.kibetsu_key, f_taino.doitsunin_kojin_no, f_taino.kojin_no, 
					CASE f_taino.kasankin_cd
                  WHEN 0 THEN f_taino.zeigaku_mino
                  ELSE 0
               END
					, f_taino.tokusoku_mino, 
					CASE f_taino.kasankin_cd
                  WHEN 0 THEN 0
                  ELSE f_taino.zeigaku_mino
               END
					, f_taino.entaikin_mino, f_taino.zeigaku_mino + f_taino.tokusoku_mino + f_taino.entaikin_mino, f_taino.kanno_cd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, CURRENT_TIMESTAMP(0), CURRENT_TIMESTAMP(0), 'BATCH', 'SERVER', 0
               FROM f_group_kojin
                  , f_taino
               WHERE f_group_kojin.busho_cd = rec_kanshi.busho_cd
                 AND f_group_kojin.group_no = rec_kanshi.group_no
                 AND f_group_kojin.kojin_no = f_taino.doitsunin_kojin_no
                 AND f_taino.zeimoku_cd IN( SELECT zeimoku_cd FROM t_zeimoku WHERE busho_cd = rec_kanshi.busho_cd AND del_flg = 0 )
                 AND f_taino.kanno_cd < 4
                 AND NOT EXISTS ( SELECT 1 FROM f_kesson_kibetsu WHERE f_taino.kibetsu_key = f_kesson_kibetsu.kibetsu_key AND shobun_jotai_cd IN( 10, 14 ) AND del_flg = 0 )
                 AND f_taino.del_flg = 0;
            EXCEPTION
               WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
						ROLLBACK;
            END;
         END IF;
      -- 4
         BEGIN
            UPDATE f_group_kanshi_kibetsu
            SET ( saikoku_flg, saikoku_count ) = ( SELECT SIGN( COUNT( f_saikoku_rireki_kibetsu.kibetsu_key ) )
                                                        , COUNT( f_saikoku_rireki_kibetsu.kibetsu_key )
                                                   FROM f_saikoku_rireki_kibetsu
                                                      , f_saikoku_rireki
                                                   WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
                                                     AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
                                                     AND f_group_kanshi_kibetsu.kibetsu_key = f_saikoku_rireki_kibetsu.kibetsu_key
                                                     AND f_saikoku_rireki_kibetsu.seq_no_saikoku = f_saikoku_rireki.seq_no_saikoku
                                                     AND f_saikoku_rireki.hakko_ymd >= rec_kanshi.kanshi_kaishi_ymd
                                                     AND f_saikoku_rireki_kibetsu.del_flg = 0
                                                     AND f_saikoku_rireki.del_flg = 0 )
            WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
              AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no

            ln_upd_count := ln_upd_count + 1;
         EXCEPTION
            WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
         END;

         BEGIN
            UPDATE f_group_kanshi_kibetsu
            SET ( bunno_flg ) = ( SELECT SIGN( COUNT( f_bunno_kibetsu.kibetsu_key ) )
                                  FROM f_bunno_kibetsu
                                  WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
                                    AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
                                    AND f_group_kanshi_kibetsu.kibetsu_key = f_bunno_kibetsu.kibetsu_key
                                    AND f_bunno_kibetsu.bunno_ymd >= rec_kanshi.kanshi_kaishi_ymd
                                    AND f_bunno_kibetsu.bunno_jotai_cd >= 10
                                    AND f_bunno_kibetsu.del_flg = 0 )
            WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
              AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
         EXCEPTION
            WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
         END;

         BEGIN
            UPDATE f_group_kanshi_kibetsu
            SET ( shobun_flg ) = ( SELECT SIGN( COUNT( f_shobun_kibetsu.kibetsu_key ) )
                                   FROM f_shobun_kibetsu
                                   WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
                                     AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
                                     AND f_group_kanshi_kibetsu.kibetsu_key = f_shobun_kibetsu.kibetsu_key
                                     AND f_shobun_kibetsu.shobun_ymd >= rec_kanshi.kanshi_kaishi_ymd
                                     AND f_shobun_kibetsu.shobun_jotai_cd >= 10
                                     AND f_shobun_kibetsu.del_flg = 0 )
            WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
              AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
         EXCEPTION
            WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
         END;

         BEGIN
            UPDATE f_group_kanshi_kibetsu
            SET ( yuyo_flg ) = ( SELECT SIGN( COUNT( f_yuyo_kibetsu.kibetsu_key ) )
                                 FROM f_yuyo_kibetsu
                                 WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
                                   AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
                                   AND f_group_kanshi_kibetsu.kibetsu_key = f_yuyo_kibetsu.kibetsu_key
                                   AND f_yuyo_kibetsu.yuyo_shinsei_ymd >= rec_kanshi.kanshi_kaishi_ymd
                                   AND f_yuyo_kibetsu.yuyo_jotai_cd >= 10
                                   AND f_yuyo_kibetsu.del_flg = 0 )
            WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
              AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
         EXCEPTION
            WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
         END;

         BEGIN
            UPDATE f_group_kanshi_kibetsu
            SET ( shikkoteishi_flg ) = ( SELECT SIGN( COUNT( f_shikkoteishi_kibetsu.kibetsu_key ) )
                                         FROM f_shikkoteishi_kibetsu
                                         WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
                                           AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
                                           AND f_group_kanshi_kibetsu.kibetsu_key = f_shikkoteishi_kibetsu.kibetsu_key
                                           AND f_shikkoteishi_kibetsu.shobun_ymd >= rec_kanshi.kanshi_kaishi_ymd
                                           AND f_shikkoteishi_kibetsu.shobun_jotai_cd >= 10
                                           AND f_shikkoteishi_kibetsu.del_flg = 0 )
            WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
              AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
         EXCEPTION
            WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
         END;

         BEGIN
            UPDATE f_group_kanshi_kibetsu
            SET ( kesson_flg ) = ( SELECT SIGN( COUNT( f_kesson_kibetsu.kibetsu_key ) )
                                   FROM f_kesson_kibetsu
                                   WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
                                     AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
                                     AND f_group_kanshi_kibetsu.kibetsu_key = f_kesson_kibetsu.kibetsu_key
                                     AND f_kesson_kibetsu.shobun_ymd >= rec_kanshi.kanshi_kaishi_ymd
                                     AND f_kesson_kibetsu.shobun_jotai_cd >= 10
                                     AND f_kesson_kibetsu.del_flg = 0 )
            WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
              AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no

         EXCEPTION
            WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
         END;

         BEGIN
            UPDATE f_group_kanshi_kibetsu
            SET ( shuno_flg ) = ( SELECT SIGN( COUNT( f_shuno.kibetsu_key ) )
                                  FROM f_shuno
                                   WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
                                     AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
                                     AND f_group_kanshi_kibetsu.kibetsu_key = f_shuno.kibetsu_key
                                     AND f_shuno.shuno_ymd >= rec_kanshi.kanshi_kaishi_ymd
                                     AND f_shuno.del_flg = 0 )
            WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
              AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
              AND EXISTS( SELECT 1
                          FROM f_shuno
                          WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
                            AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no
                            AND f_shuno.shuno_ymd >= rec_kanshi.kanshi_kaishi_ymd
                            AND f_shuno.del_flg = 0 );
         EXCEPTION
            WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
         END;
      -- 検索条件を設定
         IF ln_new_flg = 0 THEN
	         BEGIN
	             UPDATE f_group_kanshi_kibetsu
	                SET ( zeigaku_mino, tokusoku_mino, kasankin_mino, entaikin_mino, kingaku_mino, kanno_cd )
	                  = ( SELECT zeigaku_mino
	                           , tokusoku_mino
	                           , kasankin_mino
	                           , entaikin_mino
	                           , ( zeigaku_mino + tokusoku_mino + kasankin_mino + entaikin_mino )
	                           , kanno_cd
	                        FROM f_taino
	                       WHERE f_group_kanshi_kibetsu.kibetsu_key = f_taino.kibetsu_key
	                         AND f_taino.del_flg = 0 )
	              WHERE f_group_kanshi_kibetsu.busho_cd = rec_kanshi.busho_cd
	                AND f_group_kanshi_kibetsu.group_kanshi_no = rec_kanshi.group_kanshi_no;
	         EXCEPTION
	             WHEN OTHERS THEN
	                 ln_err_count := ln_err_count + 1;
	         END;
         END IF;

      -- 5
         BEGIN
            UPDATE f_group_kanshi_kojin
            SET ( zeigaku_mino, tokusoku_mino, kasankin_mino, entaikin_mino, kingaku_mino, saikoku_flg, saikoku_count
                , bunno_flg, shobun_flg, yuyo_flg, shikkoteishi_flg, kesson_flg, shuno_flg, mino_flg )
              = ( SELECT SUM( f_group_kanshi_kibetsu.zeigaku_mino )
                       , SUM( f_group_kanshi_kibetsu.tokusoku_mino )
                       , SUM( f_group_kanshi_kibetsu.kasankin_mino )
                       , SUM( f_group_kanshi_kibetsu.entaikin_mino )
                       , SUM( f_group_kanshi_kibetsu.kingaku_mino )
                       , SIGN( SUM( f_group_kanshi_kibetsu.saikoku_flg ) )
                       , MAX( f_group_kanshi_kibetsu.saikoku_count )
                       , SIGN( SUM( f_group_kanshi_kibetsu.bunno_flg ) )
                       , SIGN( SUM( f_group_kanshi_kibetsu.shobun_flg ) )
                       , SIGN( SUM( f_group_kanshi_kibetsu.yuyo_flg ) )
                       , SIGN( SUM( f_group_kanshi_kibetsu.shikkoteishi_flg ) )
                       , SIGN( SUM( f_group_kanshi_kibetsu.kesson_flg ) )
                       , SIGN( SUM( f_group_kanshi_kibetsu.shuno_flg ) )
                       , SIGN( SUM( f_group_kanshi_kibetsu.kingaku_mino ) )
                  FROM f_group_kanshi_kibetsu
                  WHERE f_group_kanshi_kojin.busho_cd = rec_kanshi.busho_cd
                    AND f_group_kanshi_kojin.group_kanshi_no = rec_kanshi.group_kanshi_no
                    AND f_group_kanshi_kojin.busho_cd = f_group_kanshi_kibetsu.busho_cd
                    AND f_group_kanshi_kojin.group_kanshi_no = f_group_kanshi_kibetsu.group_kanshi_no
                    AND f_group_kanshi_kojin.doitsunin_kojin_no = f_group_kanshi_kibetsu.doitsunin_kojin_no )
            WHERE f_group_kanshi_kojin.busho_cd = rec_kanshi.busho_cd
              AND f_group_kanshi_kojin.group_kanshi_no = rec_kanshi.group_kanshi_no
              AND EXISTS( SELECT 1
                          FROM f_group_kanshi_kibetsu
                          WHERE f_group_kanshi_kojin.busho_cd = rec_kanshi.busho_cd
                            AND f_group_kanshi_kojin.group_kanshi_no = rec_kanshi.group_kanshi_no
                            AND f_group_kanshi_kojin.busho_cd = f_group_kanshi_kibetsu.busho_cd
                            AND f_group_kanshi_kojin.group_kanshi_no = f_group_kanshi_kibetsu.group_kanshi_no
                            AND f_group_kanshi_kojin.doitsunin_kojin_no = f_group_kanshi_kibetsu.doitsunin_kojin_no );
         EXCEPTION
            WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
         END;

         IF rec_kanshi.kiji_sentaku_kbn = 1 THEN
            BEGIN
               UPDATE f_group_kanshi_kojin
               SET ( sessho_flg, sessho_count ) = ( SELECT SIGN( COUNT( f_kiji.seq_no_kiji ) )
                                                         , COUNT( f_kiji.seq_no_kiji )
                                                    FROM f_kiji
                                                    WHERE  f_group_kanshi_kojin.busho_cd = rec_kanshi.busho_cd
                                                      AND f_group_kanshi_kojin.group_kanshi_no = rec_kanshi.group_kanshi_no
                                                      AND f_group_kanshi_kojin.doitsunin_kojin_no = f_kiji.kojin_no
                                                      AND f_kiji.kiji_ymd >= rec_kanshi.kanshi_kaishi_ymd
                                                      AND f_kiji.busho_cd = rec_kanshi.busho_cd
                                                      AND f_kiji.kodo_yotei_kbn = 1
                                                      AND f_kiji.sessho_flg = 1
                                                      AND f_kiji.del_flg = 0 )
               WHERE f_group_kanshi_kojin.busho_cd = rec_kanshi.busho_cd
                 AND f_group_kanshi_kojin.group_kanshi_no = rec_kanshi.group_kanshi_no
            EXCEPTION
               WHEN OTHERS THEN
                     ln_err_count := ln_err_count + 1;
            END;
         ELSE
            BEGIN
               UPDATE f_group_kanshi_kojin
               SET ( sessho_flg, sessho_count ) = ( SELECT SIGN( COUNT( f_kiji.seq_no_kiji ) )
                                                         , COUNT( f_kiji.seq_no_kiji )
                                                    FROM f_kiji
                                                       , f_group_kanshi_kiji
                                                    WHERE f_group_kanshi_kojin.busho_cd = rec_kanshi.busho_cd
                                                      AND f_group_kanshi_kojin.group_kanshi_no = rec_kanshi.group_kanshi_no
                                                      AND f_group_kanshi_kojin.doitsunin_kojin_no = f_kiji.kojin_no
                                                      AND f_group_kanshi_kojin.busho_cd = f_group_kanshi_kiji.busho_cd
                                                      AND f_group_kanshi_kojin.group_kanshi_no = f_group_kanshi_kiji.group_kanshi_no
                                                      AND f_kiji.kiji_naiyo_cd = f_group_kanshi_kiji.kiji_naiyo_cd
                                                      AND f_kiji.kiji_ymd >= rec_kanshi.kanshi_kaishi_ymd
                                                      AND f_kiji.busho_cd = rec_kanshi.busho_cd
                                                      AND f_kiji.kodo_yotei_kbn = 1
                                                      AND f_kiji.sessho_flg = 1
                                                      AND f_kiji.del_flg = 0 )
               WHERE f_group_kanshi_kojin.busho_cd = rec_kanshi.busho_cd
                 AND f_group_kanshi_kojin.group_kanshi_no = rec_kanshi.group_kanshi_no
               ;
            EXCEPTION
               WHEN OTHERS THEN
                     ln_err_count := ln_err_count + 1;
            END;
         END IF;

         -- 新規作成時、エラーが発生した場合
         IF ln_new_flg = 1 THEN
	         BEGIN
	             UPDATE f_group_kanshi_settei
	                SET ( kojin_count, kanno_count, mino_count, zeigaku_mino_total, tokusoku_mino_total, kasankin_mino_total, entaikin_mino_total, joken_henko_flg )
	                  = ( SELECT COUNT(*) TAISHOSHA
	                           , SUM(CASE mino_flg WHEN 0 THEN 1 ELSE 0 END) KANNOSHA
	                           , SUM(CASE mino_flg WHEN 1 THEN 1 ELSE 0 END) MINOSHA
	                           , SUM( zeigaku_mino ) mino_zeigaku
	                           , SUM( tokusoku_mino ) mino_tokusoku
	                           , SUM( kasankin_mino ) mino_kasankin
	                           , SUM( entaikin_mino ) mino_entaikin
                               , 0
	                        FROM f_group_kanshi_kojin
	                       WHERE busho_cd = rec_kanshi.busho_cd
	                         AND group_kanshi_no = rec_kanshi.group_kanshi_no
	                         AND del_flg = 0
	                   )
	             WHERE f_group_kanshi_settei.busho_cd = rec_kanshi.busho_cd
	               AND f_group_kanshi_settei.group_kanshi_no = rec_kanshi.group_kanshi_no;
	         EXCEPTION
	             WHEN OTHERS THEN
	                 ln_err_count := ln_err_count + 1;
	         END;
         ELSE
	         BEGIN
	             UPDATE f_group_kanshi_settei
	                SET joken_henko_flg = 0
	             WHERE f_group_kanshi_settei.busho_cd = rec_kanshi.busho_cd
	               AND f_group_kanshi_settei.group_kanshi_no = rec_kanshi.group_kanshi_no;
	         EXCEPTION
	             WHEN OTHERS THEN
	                 ln_err_count := ln_err_count + 1;
	         END;
         END IF;
-- 2015/05/15 END

      END LOOP;
   CLOSE cur_kanshi;

   ld_shuryo_datetime := CURRENT_TIMESTAMP(0);

   BEGIN
      INSERT INTO f_batch_log( renkei_data_cd, seq_no_renkei, shori_ymd, kaishi_datetime, shuryo_datetime, shori_count, ins_count, upd_count, del_count, err_count )
      VALUES( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, ld_kaishi_datetime, ld_shuryo_datetime, ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count );
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

   RAISE NOTICE 'ジャンル : % | アイテム : % | 種類 : % | 数量 : % | 処理時間 : % | 結果コード : %', ld_kaishi_datetime, ld_shuryo_datetime, ln_shori_count, ln_ins_count, ln_upd_count, ln_err_count;


END;
$$;