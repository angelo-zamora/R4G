--------------------------------------------------------
--  DDL for Procedure proc_r4g_kotei_kyoyu
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_kotei_kyoyu( 
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying 
)
LANGUAGE plpgsql
AS $$
/**********************************************************************************************************************/
/* 処理概要 : dlgmain：f_共有管理（f_kyoyukanri）の追加／更新／削除を実施する）                                            */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                    */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                   */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                 */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                         */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                          */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/30  CRESS-INFO.Drexler     新規作成     036o011「固定資産税_共有管理（統合収滞納）」の取込を行う      */
/**********************************************************************************************************************/

DECLARE

   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;
   rec_kotei                      f_kyoyukanri%ROWTYPE;

   ln_para01                      numeric DEFAULT 0;

   ln_shori_count                 numeric DEFAULT 0;             -- 処理件数用変数
   ln_ins_count                   numeric DEFAULT 0;             -- 追加件数用変数
   ln_upd_count                   numeric DEFAULT 0;             -- 更新件数用変数
   ln_del_count                   numeric DEFAULT 0;             -- 削除件数用変数
   ln_err_count                   numeric DEFAULT 0;             -- エラー件数用変数
   lc_err_cd                      character varying;             -- エラーコード用変数
   lc_err_text                    character varying(100):='';    -- エラー内容用変数
   ln_result_cd                   numeric DEFAULT 0;             -- 結果区分更新用変数
   ln_result_cd_add               numeric DEFAULT 1;             -- 追加フラグ
   ln_result_cd_upd               numeric DEFAULT 2;             -- 更新フラグ
   ln_result_cd_del               numeric DEFAULT 3;             -- 削除フラグ
   ln_result_cd_warning           numeric DEFAULT 7;             -- 警告フラグ
   ln_result_cd_err               numeric DEFAULT 9;             -- エラーフラグ

   lc_err_cd_normal               character varying = '0';       -- 通常フラグ
   lc_err_cd_err                  character varying = '9';       -- エラーフラグ
   lc_sql                         character varying;             -- SQL文用変数

   -- メインカーソル
   cur_main CURSOR FOR
   SELECT *
   FROM dlgrenkei.i_r4g_kotei_kyoyu as main
   WHERE saishin_flg = '1'
   AND kyoyu_rireki_no = (
      SELECT MAX(kyoyu_rireki_no)
      FROM dlgrenkei.i_r4g_kotei_kyoyu AS sub
      WHERE sub.kazei_nendo = main.kazei_nendo
         AND sub.kyoyu_atena_no = main.kyoyu_atena_no
         AND sub.kyoyu_shisan_no = main.kyoyu_shisan_no
   )
   AND result_cd < 8;

   rec_main                            dlgrenkei.i_r4g_kotei_kyoyu%ROWTYPE;
   
   -- パラメータ取得カーソル
   cur_parameter CURSOR FOR
   SELECT * FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                       dlgrenkei.f_renkei_parameter%ROWTYPE;

   -- 行ロック用カーソル
   cur_lock CURSOR FOR
   SELECT * FROM f_kyoyukanri
   WHERE shikuchoson_cd   = rec_kotei.shikuchoson_cd
      AND kyoyusha_gimusha_kojin_no = rec_kotei.kyoyusha_gimusha_kojin_no
      AND kyoyu_shisan_no = rec_kotei.kyoyu_shisan_no
      AND koseiin_renban = rec_kotei.koseiin_renban
      AND koseiin_gimusha_kojin_no = rec_kotei.koseiin_gimusha_kojin_no;

   rec_lock                            f_kyoyukanri%ROWTYPE;

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
         lc_sql := 'TRUNCATE TABLE dlgmain.f_kyoyukanri';
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

         rec_kotei.kazei_nendo := rec_main.kazei_nendo::numeric;
         rec_kotei.kyoyusha_gimusha_kojin_no := rec_main.kyoyu_atena_no;
         rec_kotei.kyoyu_shisan_no := rec_main.kyoyu_shisan_no;
         rec_kotei.koseiin_renba := rec_main.koseiin_renban::numeric;
         rec_kotei.koseiin_gimusha_kojin_no := rec_main.koseiin_gimusha_atena_no;
         rec_kotei.kyoyu_kbn := rec_main.kyoyu_kbn::numeric;
         -- rec_kotei.kyoyu_mochibun_kbn := rec_main.kyoyu_kbn; TO BE CONFIRMED
         rec_kotei.daihyosha_flg := rec_main.daihyo_flg::numeric;
         rec_kotei.ido_ymd := get_date_to_num(to_date(rec_main.ido_ymd, 'YYYY-MM-DD'));
         rec_kotei.ido_jiyu_cd := rec_main.ido_jiyu::numeric;
         rec_kotei.kyoyusha_ninzu := rec_main.kyoyu_ninzu::numeric;
         rec_kotei.toki_mochibun_bunshi := get_trimmed_space(rec_main.toki_bunshi);
         rec_kotei.toki_mochibun_bunbo := get_trimmed_space(rec_main.toki_bunbo);
         rec_kotei.genkyo_mochibun_bunshi := get_trimmed_space(rec_main.genkyo_bunshi);
         rec_kotei.genkyo_mochibun_bunbo := get_trimmed_space(rec_main.genkyo_bunbo);
         rec_kotei.kyoyusha_rireki_no := rec_main.kyoyu_rireki_no::numeric;
         rec_kotei.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_kotei.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         rec_kotei.upd_tantosha_cd := rec_main.upd_tantosha_cd;
         rec_kotei.upd_tammatsu := 'SERVER';
         rec_kotei.del_flg := rec_main.del_flg::numeric;

         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
           CLOSE cur_lock;
        
         IF rec_lock IS NULL THEN
              BEGIN
                 INSERT INTO f_kyoyukanri (
                     kazei_nendo                
                     ,kyoyusha_gimusha_kojin_no
                     ,kyoyu_shisan_no
                     ,koseiin_renba
                     ,koseiin_gimusha_kojin_no
                     ,kyoyu_kbn
                     ,kyoyu_mochibun_kbn
                     ,daihyosha_flg
                     ,ido_ymd
                     ,ido_jiyu_cd
                     ,kyoyusha_ninzu
                     ,toki_mochibun_bunshi
                     ,toki_mochibun_bunbo
                     ,genkyo_mochibun_bunshi
                     ,genkyo_mochibun_bunbo
                     ,kyoyusha_rireki_no
                     ,ins_datetime
                     ,upd_datetime
                     ,upd_tantosha_cd
                     ,upd_tammatsu
                     ,del_flg
                 ) VALUES (
                      rec_kotei.kazei_nendo                
                     ,rec_kotei.kyoyusha_gimusha_kojin_no
                     ,rec_kotei.kyoyu_shisan_no
                     ,rec_kotei.koseiin_renba
                     ,rec_kotei.koseiin_gimusha_kojin_no
                     ,rec_kotei.kyoyu_kbn
                     ,rec_kotei.kyoyu_mochibun_kbn
                     ,rec_kotei.daihyosha_flg
                     ,rec_kotei.ido_ymd
                     ,rec_kotei.ido_jiyu_cd
                     ,rec_kotei.kyoyusha_ninzu
                     ,rec_kotei.toki_mochibun_bunshi
                     ,rec_kotei.toki_mochibun_bunbo
                     ,rec_kotei.genkyo_mochibun_bunshi
                     ,rec_kotei.genkyo_mochibun_bunbo
                     ,rec_kotei.kyoyusha_rireki_no
                     ,rec_kotei.ins_datetime
                     ,rec_kotei.upd_datetime
                     ,rec_kotei.upd_tantosha_cd
                     ,rec_kotei.upd_tammatsu
                     ,rec_kotei.del_flg
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
                 WHERE kazei_nendo = rec_kotei.kazei_nendo
                    AND kyoyusha_gimusha_kojin_no = rec_kotei.kyoyusha_gimusha_kojin_no
                    AND kyoyu_shisan_no = rec_kotei.kyoyu_shisan_no   
                    AND koseiin_renban = rec_kotei.koseiin_renban   
                    AND koseiin_gimusha_kojin_no = rec_kotei.koseiin_gimusha_kojin_no;

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

         BEGIN
            -- 中間テーブル更新
            IF rec_main.del_flg::numeric = 1 THEN
               ln_del_count := ln_del_count + 1;
               ln_result_cd := ln_result_cd_del;
            END IF;

            UPDATE dlgrenkei.i_r4g_kotei_kyoyu
            SET result_cd = ln_result_cd
               , error_cd = ln_err_cd
               , error_text = lc_err_text
               , seq_no_renkei = in_n_renkei_seq
               , shori_ymd     = in_n_shori_ymd
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND kazei_nendo = rec_main.kazei_nendo
               AND kyoyu_atena_no = rec_main.kyoyu_atena_no
               AND kyoyu_shisan_no = rec_main.kyoyu_shisan_no
               AND kyoyu_rireki_no = rec_main.kyoyu_rireki_no
               AND koseiin_renban = rec_main.koseiin_renban
               AND koseiin_gimusha_atena_no = rec_main.koseiin_gimusha_atena_no;
         EXCEPTION
            WHEN OTHERS THEN NULL;
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
   CALL dlgrenkei.proc_upd_log(rec_log);

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;
   
   EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;