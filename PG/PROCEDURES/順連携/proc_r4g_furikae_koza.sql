----------------------------------------------------
 --DDL for Procedure proc_r4g_furikae_koza
----------------------------------------------------
CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_furikae_koza(
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying 
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 振替口座情報（統合収滞納）                                                                                 */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                   */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                  */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                        */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                         */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/28  CRESS-INFO.Drexler     新規作成     036o010「振替口座情報（統合収滞納）」の取込を行う            */
/**********************************************************************************************************************/
DECLARE

   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;
   rec_furikae                    f_kozajoho%ROWTYPE;
   ln_para01                     numeric DEFAULT 0;
   ln_shori_count                 numeric DEFAULT 0;             -- 処理件数用変数
   ln_ins_count                   numeric DEFAULT 0;             -- 追加件数用変数
   ln_upd_count                   numeric DEFAULT 0;             -- 更新件数用変数
   ln_del_count                   numeric DEFAULT 0;             -- 削除件数用変数
   ln_err_count                   numeric DEFAULT 0;             -- エラー件数用変数
   lc_err_cd                      character varying;             -- エラーコード用変数
   lc_err_text                    character varying(100):='';    -- エラー内容用変数
   ln_result_cd                   numeric DEFAULT 0;             -- 結果区分更新用変数
   ln_result_cd_add               numeric DEFAULT 1;              -- 追加フラグ
   ln_result_cd_upd               numeric DEFAULT 2;              -- 更新フラグ
   ln_result_cd_del               numeric DEFAULT 3;              -- 削除フラグ
   ln_result_cd_warning           numeric DEFAULT 7;              -- 警告フラグ
   ln_result_cd_err               numeric DEFAULT 9;              -- エラーフラグ
   lc_err_cd_normal               character varying = '0';        -- 通常フラグ
   lc_err_cd_err                  character varying = '9';        -- エラーフラグ
   lc_sql                         character varying;              -- SQL文用変数

   -- メインカーソル
   cur_main CURSOR FOR
   SELECT *
     FROM dlgrenkei.i_r4g_furikae_koza as tbl_main
   WHERE tbl_main.saishin_flg = '1'
     AND tbl_main.result_cd < 8
     AND tbl_main.koza_rireki_no = (
        SELECT MAX(koza_rireki_no)
           FROM dlgrenkei.i_r4g_furikae_koza AS tbl_sub
        WHERE tbl_sub.atena_no = tbl_main.atena_no
           AND tbl_sub.zeimoku_cd = tbl_main.zeimoku_cd
           AND tbl_sub.furikae_kbn = tbl_main.furikae_kbn
           AND tbl_sub.jido_atena_no = tbl_main.jido_atena_no);

   rec_main                       dlgrenkei.i_r4g_furikae_koza%ROWTYPE;

   -- パラメータ取得カーソル
   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;

   -- 行ロック用カーソル
   cur_lock CURSOR FOR
   SELECT *
      FROM f_kozajoho
   WHERE kojin_no = lc_kojin_no
      AND zeimoku_cd = lc_zeimoku_cd::numeric
      AND koza_kbn = rec_furikae.koza_kbn
      AND jido_atena_no = rec_furikae.jido_atena_no
      AND koza_rireki_no = rec_furikae.koza_rireki_no;

   rec_lock                       f_kozajoho%ROWTYPE;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   --1. パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
        FETCH cur_parameter INTO rec_parameter;
        EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN
            ln_para01 := rec_parameter.parameter_value;
         END IF;
      END LOOP;
   CLOSE cur_parameter;

   --2. 連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         lc_sql := 'TRUNCATE TABLE dlgmain.f_kozajoho';
         EXECUTE lc_sql;
      EXCEPTION WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text    := SQLERRM;
         RETURN;
      END;
   END IF;

   --3. 中間テーブルデータのエラーチェック
   CALL proc_error_check(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);
      
   IF io_c_err_code <> '0' THEN
      RETURN;
   END IF;

   --4. 桁数設定情報取得
   --r4gでは不要
   
   --5. 連携データの作成・更新
   ln_shori_count := 0;
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

         ln_shori_count                      := ln_shori_count + 1;
         -- 個人番号
         rec_furikae.kojin_no                := rec_main.atena_no;
         -- 税目コード
         rec_furikae.zeimoku_cd              := get_r4g_code_conv(0, 3, rec_main.zeimoku_cd, null)::numeric;
         -- 振替口座区分
         rec_furikae.koza_kbn                := rec_main.furikae_kbn::numeric;
         -- 児童_宛名番号
         rec_furikae.jido_kojin_no           := rec_main.jido_atena_no;
         -- 開始日
         rec_furikae.kaishi_ymd              := get_date_to_num(to_date(rec_main.koufuri_kaishi_ymd, 'YYYY-MM-DD'));
         -- 終了日
         rec_furikae.shuryo_ymd              := get_date_to_num(to_date(rec_main.koufuri_shuryo_ymd, 'YYYY-MM-DD'));
         -- 金融機関コード
         rec_furikae.kinyu_kikan_cd          := rec_main.kinyukikan_cd;
         -- 金融機関支店コード
         rec_furikae.kinyu_kikan_shiten_cd   := rec_main.tempo_no;
         -- ゆうちょ銀行記号
         rec_furikae.yucho_kigo              := rec_main.yucho_kigo;
         -- ゆうちょ銀行番号
         rec_furikae.yucho_no                := rec_main.yucho_no;
         -- 金融機関種別
         rec_furikae.kinyu_kikan_shubetsu_kbn := rec_main.kinyukikan_shubetsu::numeric;
         -- 口座種別コード
         rec_furikae.koza_shubetsu_cd        := rec_main.koza_shubtsu::numeric;
         -- 口座番号
         rec_furikae.koza_no                 := rec_main.koza_no;
         -- 口座名義人カナ
         rec_furikae.koza_meiginin_kana      := get_trimmed_space(rec_main.koza_meigi_kana);
         --- 口座名義人
         rec_furikae.koza_meiginin           := get_trimmed_space(rec_main.koza_meigi_kanji);
         -- 口座振替停止開始年月日
         rec_furikae.teishi_kaishi_ymd       := get_date_to_num(to_date(rec_main.koza_teishi_kaishi_ymd, 'YYYY-MM-DD'));
         -- 口座振替停止終了年月日
         rec_furikae.teishi_shuryo_ymd       := get_date_to_num(to_date(rec_main.koza_teishi_shuryo_ymd, 'YYYY-MM-DD'));
         -- 口座振替廃止年月日
         rec_furikae.haishi_ymd              := get_date_to_num(to_date(rec_main.koza_haishi_ymd, 'YYYY-MM-DD'));
         -- 納付方法
         rec_furikae.nofuhoho_kbn            := rec_main.nofuhoho::numeric;
         -- メモ
         rec_furikae.memo                    := rec_main.memo;
         -- 口座履歴番号
         rec_furikae.koza_rireki_no          := rec_main.koza_rireki_no::numeric;
         -- データ作成日時
         rec_furikae.ins_datetime            := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時
         rec_furikae.upd_datetime            := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_furikae.upd_tantosha_cd         := rec_main.upd_tantosha_cd;
         -- 更新端末名称
         rec_furikae.upd_tammatsu            := 'SERVER';
         -- 削除フラグ
         rec_furikae.del_flg                 := rec_main.del_flg::numeric;

         OPEN cur_lock;
           FETCH cur_lock INTO rec_lock; 
         CLOSE cur_lock;    
              
            IF rec_lock IS NULL THEN
                BEGIN
                    INSERT INTO f_kozajoho (
                        kojin_no
                        ,zeimoku_cd
                        ,koza_kbn
                        ,jido_kojin_no
                        ,kaishi_ymd
                        ,shuryo_ymd
                        ,kinyu_kikan_cd
                        ,kinyu_kikan_shiten_cd
                        ,yucho_kigo
                        ,yucho_no
                        ,kinyu_kikan_shubetsu_kbn
                        ,koza_shubetsu_cd
                        ,koza_no
                        ,koza_meiginin_kana
                        ,koza_meiginin
                        ,teishi_kaishi_ymd
                        ,teishi_shuryo_ymd
                        ,haishi_ymd
                        ,nofuhoho_kbn
                        ,memo
                        ,koza_rireki_no
                        ,ins_datetime
                        ,upd_datetime
                        ,upd_tantosha_cd
                        ,upd_tammatsu
                        ,del_flg
                    ) VALUES (
                        rec_furikae.kojin_no
                        ,rec_furikae.zeimoku_cd
                        ,rec_furikae.koza_kbn
                        ,rec_furikae.jido_kojin_no
                        ,rec_furikae.kaishi_ymd
                        ,rec_furikae.shuryo_ymd
                        ,rec_furikae.kinyu_kikan_cd
                        ,rec_furikae.kinyu_kikan_shiten_cd
                        ,rec_furikae.yucho_kigo
                        ,rec_furikae.yucho_no
                        ,rec_furikae.kinyu_kikan_shubetsu_kbn
                        ,rec_furikae.koza_shubetsu_cd
                        ,rec_furikae.koza_no
                        ,rec_furikae.koza_meiginin_kana
                        ,rec_furikae.koza_meiginin
                        ,rec_furikae.teishi_kaishi_ymd
                        ,rec_furikae.teishi_shuryo_ymd
                        ,rec_furikae.haishi_ymd
                        ,rec_furikae.nofuhoho_kbn
                        ,rec_furikae.memo
                        ,rec_furikae.koza_rireki_no
                        ,rec_furikae.ins_datetime
                        ,rec_furikae.upd_datetime
                        ,rec_furikae.upd_tantosha_cd
                        ,rec_furikae.upd_tammatsu
                        ,rec_furikae.del_flg
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
                 BEGIN
                    UPDATE f_kozajoho
                    SET
                        kaishi_ymd = rec_furikae.kaishi_ymd
                        ,shuryo_ymd = rec_furikae.shuryo_ymd
                        ,kinyu_kikan_cd = rec_furikae.kinyu_kikan_cd
                        ,kinyu_kikan_shiten_cd = rec_furikae.kinyu_kikan_shiten_cd
                        ,yucho_kigo = rec_furikae.yucho_kigo
                        ,yucho_no = rec_furikae.yucho_no
                        ,kinyu_kikan_shubetsu_kbn = rec_furikae.kinyu_kikan_shubetsu_kbn
                        ,koza_shubetsu_cd = rec_furikae.koza_shubetsu_cd
                        ,koza_no = rec_furikae.koza_no
                        ,koza_meiginin_kana = rec_furikae.koza_meiginin_kana
                        ,koza_meiginin = rec_furikae.koza_meiginin
                        ,teishi_kaishi_ymd = rec_furikae.teishi_kaishi_ymd
                        ,teishi_shuryo_ymd = rec_furikae.teishi_shuryo_ymd
                        ,haishi_ymd = rec_furikae.haishi_ymd
                        ,nofuhoho_kbn = rec_furikae.nofuhoho_kbn
                        ,memo = rec_furikae.kaismemohi_ymd
                        ,koza_rireki_no = rec_furikae.koza_rireki_no
                        ,upd_datetime = rec_furikae.upd_datetime
                        ,upd_tantosha_cd = rec_furikae.upd_tantosha_cd
                        ,upd_tammatsu = rec_furikae.upd_tammatsu
                        ,del_flg = rec_furikae.del_flg
                    WHERE kojin_no = rec_furikae.kojin_no
                       AND zeimoku_cd = rec_furikae.zeimoku_cd
                       AND koza_kbn = rec_furikae.koza_kbn
                       AND jido_atena_no = rec_furikae.jido_atena_no
                       AND koza_rireki_no = rec_furikae.koza_rireki_no;
                       
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

            -- 中間テーブルの「削除フラグ」が「1」のデータは「3：削除」を指定する
            IF rec_main.del_flg::numeric = 1 THEN
               ln_del_count := ln_del_count + 1;
               ln_result_cd := ln_result_cd_del;
            END IF;

            -- 中間テーブル更新
            BEGIN
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
   
   --更新内容は連携ツールの連携処理クラス（RenkeiProcess）の処理：insertRenkeiKekkaを参照
   CALL dlgrenkei.proc_upd_log(rec_log, io_c_err_code, io_c_err_text);
   
   EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$