--------------------------------------------------------
--  DDL for Procedure proc_r4g_tochi
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_tochi ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, io_c_err_code INOUT character varying, io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : f_土地基本_連携（f_tochikihon_renkei）の追加／更新／削除を実施する                                          */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                   */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                  */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                */
/*      OUT : io_c_err_code      …例外エラー発生時のエラーコード                                                        */
/*            io_c_err_text    … 例外エラー発生時のエラー内容                                                           */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/23  CRESS-INFO.Angelo   012o004「土地基本情報」の取込を行う                                        */
/**********************************************************************************************************************/

DECLARE
   rec_f_tochikihon_renkei             f_tochikihon_renkei%ROWTYPE;
   ln_para01                           numeric DEFAULT 0;
   ln_para02                           numeric DEFAULT 0;

   ln_shori_count                      numeric;
   ln_ins_count                        numeric;
   ln_upd_count                        numeric;
   ln_del_count                        numeric;
   ln_err_count                        numeric;
   lc_err_text                    		character varying(100);
   ln_result_cd                   		numeric DEFAULT 0;
   lc_err_cd                      		character varying;
   lc_sql                            	character varying(1000);
   ln_result_cd_add                    numeric DEFAULT 1; -- 追加
   ln_result_cd_upd                    numeric DEFAULT 2; -- 更新
   ln_result_cd_del                    numeric DEFAULT 3; -- 削除
   ln_result_cd_warning                numeric DEFAULT 7; -- 警告
   ln_result_cd_err                    numeric DEFAULT 9; -- エラー

   lc_err_cd_normal                    character varying = '0'; -- 通常
   lc_err_cd_err                       character varying = '9'; -- エラー

   lc_bukken_no                        character varying;
   ln_kazeinendo                       numeric;
   ln_rireki_no                        numeric;

   rec_log                             dlgrenkei.f_renkei_log%ROWTYPE;

   cur_main CURSOR FOR
   SELECT *
   FROM dlgrenkei.i_r4g_tochi
   WHERE saishin_flg = '1'
   AND result_cd < 8 ;

   rec_main                            dlgrenkei.i_r4g_tochi%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                       dlgrenkei.f_renkei_parameter%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM f_tochikihon_renkei
   WHERE bukken_no = lc_bukken_no
   AND kazeinendo = ln_kazeinendo
   AND rireki_no = ln_rireki_no;

   rec_lock                            f_tochikihon_renkei%ROWTYPE;

BEGIN
   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   --パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;

      END LOOP;
   CLOSE cur_parameter;

   --連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_tochikihon_renkei;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_tochikihon_renkei';
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
   ln_shori_count := 0;

   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

         lc_bukken_no := rec_main.bukken_no;
         ln_kazeinendo := CASE WHEN rec_main.kazei_nendo IS NULL OR rec_main.kazei_nendo = '' THEN 0 ELSE rec_main.kazei_nendo::numeric END;
         ln_rireki_no := CASE WHEN rec_main.tochi_kihon_rireki_no IS NULL OR rec_main.tochi_kihon_rireki_no = '' THEN 0 ELSE rec_main.tochi_kihon_rireki_no::numeric END;
         ln_shori_count   := ln_shori_count + 1;

         -- 物件番号
         rec_f_tochikihon_renkei.bukken_no := lc_bukken_no;
         -- 課税年度
         rec_f_tochikihon_renkei.kazei_nendo := ln_kazeinendo;
         -- 土地基本_履歴番号
         rec_f_tochikihon_renkei.rireki_no := ln_rireki_no;
         -- 土地_登記所在地
         rec_f_tochikihon_renkei.toki_shozai := get_trimmed_space(rec_main.tochi_toki_jusho);
         -- 登記地目
         rec_f_tochikihon_renkei.chimoku := rec_main.toki_chimoku; -- for confirmation  登記地目 toki_chimoku  - dlgrenkei.i_r4g_tochi.地目
         -- 登記地積
         rec_f_tochikihon_renkei.chiseki := rec_main.toki_chiseki::numeric; -- for confirmation 登記地積 toki_chiseki   dlgrenkei.i_r4g_tochi.地積
         -- 登記地積
         rec_f_tochikihon_renkei.gimusha_kojin_no := rec_main.gimusha_atena_no;
         -- 土地_現況所在地
         rec_f_tochikihon_renkei.genkyo_shozai := get_trimmed_space(tochi_genkyo_jusho);
         -- 現況地目
         rec_f_tochikihon_renkei.genkyo_chimoku := rec_main.genkyo_chimoku;
         -- 現況地積
         rec_f_tochikihon_renkei.genkyo_chiseki := rec_main.genkyo_chiseki::numeric;
         -- 課税地積
         rec_f_tochikihon_renkei.kazei_chiseki := rec_main.kazei_chiseki::numeric;
         -- 現況用途コード１
         rec_f_tochikihon_renkei.yoto_cd1 := rec_main.yoto_cd1;
         -- 現況用途コード２
         rec_f_tochikihon_renkei.yoto_cd2 := rec_main.yoto_cd2;
         -- 現況用途コード３
         rec_f_tochikihon_renkei.yoto_cd3 := rec_main.yoto_cd3;
         -- 分筆・合筆原因区分
         rec_f_tochikihon_renkei.genin_kbn := rec_main.bunhitsu_gapptsu_kbn::numeric;
         -- 分筆元・合筆先_物件番号
         rec_f_tochikihon_renkei.hitsu_bukken_no := rec_main.bun_gap_bukken_no;
         -- 分筆元・合筆先_履歴番号
         rec_f_tochikihon_renkei.hitsu_rireki_no := rec_main.bun_gap_bukken_rireki_no::numeric;
         --画地番号
         rec_f_tochikihon_renkei.kakuchi_no := rec_main.kakuchi_no;
         -- データ作成日時
         rec_f_tochikihon_renkei.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時
         rec_f_tochikihon_renkei.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_f_tochikihon_renkei.upd_tantosha_cd := rec_main.sosasha_cd;
         -- 更新端末名称
         rec_f_tochikihon_renkei.upd_tammatsu := 'SERVER';
         -- 削除フラグ
         rec_f_tochikihon_renkei.del_flg := rec_main.del_flg;

            OPEN cur_lock;
               FETCH cur_lock INTO rec_lock;
            CLOSE cur_lock;

            IF rec_lock IS NULL THEN
               BEGIN
                  -- 登録処理
                  INSERT INTO f_tochikihon_renkei(
                  bukken_no				
                  , kazei_nendo											
                  , rireki_no											
                  , toki_shozai											
                  , chimoku											
                  , chiseki											
                  , gimusha_kojin_no											
                  , genkyo_shozai											
                  , genkyo_chimoku											
                  , genkyo_chiseki											
                  , kazei_chiseki											
                  , yoto_cd1											
                  , yoto_cd2											
                  , yoto_cd3											
                  , genin_kbn											
                  , hitsu_bukken_no											
                  , hitsu_rireki_no											
                  , kakuchi_no											
                  , ins_datetime											
                  , upd_datetime											
                  , upd_tantosha_cd											
                  , upd_tammatsu											
                  , del_flg											
                  )
                  VALUES (
                  rec_f_tochikihon_renkei.bukken_no											
                  , rec_f_tochikihon_renkei.kazei_nendo											
                  , rec_f_tochikihon_renkei.rireki_no									
                  , rec_f_tochikihon_renkei.toki_shozai											
                  , rec_f_tochikihon_renkei.chimoku											
                  , rec_f_tochikihon_renkei.chiseki											
                  , rec_f_tochikihon_renkei.gimusha_kojin_no											
                  , rec_f_tochikihon_renkei.genkyo_shozai											
                  , rec_f_tochikihon_renkei.genkyo_chimoku											
                  , rec_f_tochikihon_renkei.genkyo_chiseki											
                  , rec_f_tochikihon_renkei.kazei_chiseki											
                  , rec_f_tochikihon_renkei.yoto_cd1											
                  , rec_f_tochikihon_renkei.yoto_cd2											
                  , rec_f_tochikihon_renkei.yoto_cd3											
                  , rec_f_tochikihon_renkei.genin_kbn											
                  , rec_f_tochikihon_renkei.hitsu_bukken_no											
                  , rec_f_tochikihon_renkei.hitsu_rireki_no											
                  , rec_f_tochikihon_renkei.kakuchi_no											
                  , rec_f_tochikihon_renkei.ins_datetime											
                  , rec_f_tochikihon_renkei.upd_datetime											
                  , rec_f_tochikihon_renkei.upd_tantosha_cd											
                  , rec_f_tochikihon_renkei.upd_tammatsu											
                  , rec_f_tochikihon_renkei.del_flg											
                  );

                  ln_ins_count := ln_ins_count + 1;
                  lc_err_text := '';
                  lc_err_cd := lc_err_cd_normal;
                  ln_result_cd := ln_result_cd_add;

                  EXCEPTION
                  WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
                  lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                  lc_err_cd := lc_err_cd_err;
                  ln_result_cd := ln_result_cd_err;
               END;
            ELSE
               -- 連携データの作成・更新
               BEGIN
                  UPDATE f_tochikihon_renkei
                     SET toki_shozai = rec_f_tochikihon_renkei.toki_shozai
                     , chimoku = rec_f_tochikihon_renkei.chimoku
                     , chiseki = rec_f_tochikihon_renkei.chiseki
                     , gimusha_kojin_no = rec_f_tochikihon_renkei.gimusha_kojin_no
                     , genkyo_shozai = rec_f_tochikihon_renkei.genkyo_shozai
                     , genkyo_chimoku = rec_f_tochikihon_renkei.genkyo_chimoku
                     , genkyo_chiseki = rec_f_tochikihon_renkei.genkyo_chiseki
                     , kazei_chiseki = rec_f_tochikihon_renkei.kazei_chiseki
                     , yoto_cd1 = rec_f_tochikihon_renkei.yoto_cd1
                     , yoto_cd2 = rec_f_tochikihon_renkei.yoto_cd2
                     , yoto_cd3 = rec_f_tochikihon_renkei.yoto_cd3
                     , genin_kbn = rec_f_tochikihon_renkei.genin_kbn
                     , hitsu_bukken_no = rec_f_tochikihon_renkei.hitsu_bukken_no
                     , hitsu_rireki_no = rec_f_tochikihon_renkei.hitsu_rireki_no
                     , kakuchi_no = rec_f_tochikihon_renkei.kakuchi_no
                     , upd_datetime = rec_f_tochikihon_renkei.upd_datetime
                     , upd_tantosha_cd = rec_f_tochikihon_renkei.upd_tantosha_cd
                     , upd_tammatsu = rec_f_tochikihon_renkei.upd_tammatsu
                     , del_flg = rec_f_tochikihon_renkei.del_flg
                  WHERE bukken_no = lc_bukken_no
                     AND kazei_nendo = ln_kazeinendo
                     AND rireki_no = ln_rireki_no;

                  ln_upd_count := ln_upd_count + 1;
                  lc_err_text := '';
                  lc_err_cd := lc_err_cd_normal;
                  ln_result_cd := ln_result_cd_upd;

                  EXCEPTION
                  WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
                  lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                  lc_err_cd := lc_err_cd_err;
                  ln_result_cd := ln_result_cd_err;
               END;
            END IF;

         -- 中間テーブルの「削除フラグ」が「1」のデータは「3：削除」を指定
         IF rec_main.del_flg = 1 THEN
            ln_result_cd = ln_result_cd_del;
         END IF;

         -- 中間テーブル更新
         UPDATE dlgrenkei.i_r4g_tochi
            SET result_cd = ln_result_cd
            , error_cd = lc_err_cd
            , error_text = lc_err_text
            WHERE  
            shikuchoson_cd = rec_main.shikuchoson_cd
               AND bukken_no = rec_main.bukken_no
               AND kazei_nendo = rec_main.kazei_nendo
               AND tochi_kihon_rireki_no = rec_main.tochi_kihon_rireki_no;

      END LOOP;
   CLOSE cur_main;

   CALL proc_r4g_tochi_shokai(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

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
