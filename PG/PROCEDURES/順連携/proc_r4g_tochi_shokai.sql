--------------------------------------------------------
--  DDL for Procedure proc_r4g_tochi_shokai
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_tochi_shokai (
   in_n_renkei_data_cd IN numeric, 
   in_n_renkei_seq IN numeric, 
   in_n_shori_ymd IN numeric, 
   io_c_err_code INOUT character varying, 
   io_c_err_text INOUT character varying
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : f_照会_不動産（f_shokai_fudosan）の追加／更新／削除を実施する                                                */
/* 引数 IN  : in_n_renkei_data_cd   … 連携データコード                                                                  */
/*            in_n_renkei_seq      … 連携SEQ（処理単位で符番されるSEQ）                                                  */
/*            in_n_shori_ymd       … 処理日 （処理単位で設定される処理日）                                                */
/*      OUT : io_c_err_code        … 例外エラー発生時のエラーコード                                                      */
/*            io_c_err_text        … 例外エラー発生時のエラー内容                                                        */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/24  CRESS-INFO.Drexler     新規作成     012o004土地基本情報                                       */
/**********************************************************************************************************************/

DECLARE
   ln_shori_count                 numeric DEFAULT 0;             -- 処理件数用変数
   ln_ins_count                   numeric DEFAULT 0;             -- 追加件数用変数
   ln_upd_count                   numeric DEFAULT 0;             -- 更新件数用変数
   ln_del_count                   numeric DEFAULT 0;             -- 削除件数用変数
   ln_err_count                   numeric DEFAULT 0;             -- エラー件数用変数
   lc_err_cd                      character varying;             -- エラーコード用変数
   lc_err_text                    character varying(100):='';    -- エラー内容用変数
   ln_result_cd                   numeric DEFAULT 0;             -- 結果区分更新用変数
   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;
   rec_shokai_fudosan             f_shokai_fudosan%ROWTYPE;

   ln_para01                      numeric DEFAULT 0;
   ln_para02                      numeric DEFAULT 0;
   lc_kojin_no                    character varying;
   lc_sql                         character varying;

   lc_bukken_shozai               character varying;
   lc_chimoku                     character varying;
   lc_chiseki_yuka_menseki        character varying;

   ln_del_count_f_shokai_fudosan  numeric DEFAULT 0;
   ln_del_count_f_shokai_fudosan_kaiso numeric DEFAULT 0;

   ln_result_cd_add               numeric DEFAULT 1;              -- 追加フラグ
   ln_result_cd_upd               numeric DEFAULT 2;              -- 更新フラグ
   ln_result_cd_del               numeric DEFAULT 3;              -- 削除フラグ
   ln_result_cd_warning           numeric DEFAULT 7;              -- 警告フラグ
   ln_result_cd_err               numeric DEFAULT 9;              -- エラーフラグ

   lc_err_cd_normal               character varying = '0';        -- 通常フラグ
   lc_err_cd_err                  character varying = '9';        -- エラーフラグ

   -- メインカーソル
   cur_main CURSOR FOR
   SELECT *
   FROM dlgrenkei.i_r4g_tochi
   WHERE saishin_flg = '1'
      AND kazei_nendo = (SELECT MAX(kazei_nendo) FROM dlgrenkei.i_r4g_tochi)
      AND tochi_kihon_rireki_no = (SELECT MAX(tochi_kihon_rireki_no) FROM dlgrenkei.i_r4g_tochi)
      AND result_cd < 8;

   rec_main                       dlgrenkei.i_r4g_tochi%ROWTYPE;

   -- パラメータ取得カーソル
   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;
    
   cur_busho CURSOR FOR
   SELECT *
     FROM t_busho
    WHERE del_flg = 0
   ORDER BY busho_cd;

   rec_busho                       t_busho%ROWTYPE;

   -- 行ロック用カーソル
   cur_lock CURSOR FOR
   SELECT seq_no_shokai, kanren_seq_no_shokai, kojin_no
     FROM f_shokai_fudosan
    WHERE busho_cd         = rec_busho.busho_cd
      AND kojin_no         = rec_main.gimusha_atena_no
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

      IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF; -- 全件削除区分
      IF  rec_parameter.parameter_no = 2  THEN ln_para02 := rec_parameter.parameter_value; END IF; -- 所在地・地目・床面積情報

   END LOOP;
   CLOSE cur_parameter;

   -- 2. 連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count_f_shokai_fudosan FROM f_shokai_fudosan;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_shokai_fudosan';
         EXECUTE lc_sql;

         SELECT COUNT(*) INTO ln_del_count_f_shokai_fudosan_kaiso FROM f_shokai_fudosan_kaiso;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_shokai_fudosan_kaiso';
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

               -- 部署コード
               rec_shokai_fudosan.busho_cd := rec_busho.busho_cd;
               -- 個人番号
               rec_shokai_fudosan.kojin_no := rec_busho.gimusha_atena_no;
               -- 照会SEQ
               rec_shokai_fudosan.seq_no_shokai := CASE WHEN rec_lock.seq_no_shokai IS NOT NULL THEN rec_lock.seq_no_shokai ELSE SEQ_SHOKAI.NEXTVAL END;
               -- 物件番号
               rec_shokai_fudosan.bukken_no := rec_main.bukken_no;
               -- 物件種類コード
               rec_shokai_fudosan.bukken_shurui_cd := 1;
               -- 物件所在
               rec_shokai_fudosan.bukken_shozai := lc_bukken_shozai;
               -- 物件所在地番
               rec_shokai_fudosan.bukken_shozai_chiban := NULL;
               -- 家屋番号
               rec_shokai_fudosan.kaoku_no := NULL;
               -- 符号
               rec_shokai_fudosan.bukken_fugo := NULL;
               -- 地目・種類
               rec_shokai_fudosan.chimoku := lc_chimoku;
               -- 構造
               rec_shokai_fudosan.kozo := NULL;
               -- 地積床面積
               rec_shokai_fudosan.chiseki_yuka_menseki := lc_chiseki_yuka_menseki;
               -- 敷地権種類コード
               rec_shokai_fudosan.shikichiken_cd := 0;
               -- 敷地権の割合
               rec_shokai_fudosan.shikichiken_wariai := NULL;
               -- 一棟の番号
               rec_shokai_fudosan.itto_tatemono_no := NULL;
               -- 一棟の構造
               rec_shokai_fudosan.itto_tatemono_kozo := NULL;
               -- 占有の番号
               rec_shokai_fudosan.senyu_no := NULL;
               -- 占有面積
               rec_shokai_fudosan.senyu_menseki := NULL;
               -- 抵当権有無
               rec_shokai_fudosan.teitoken_flg := 0;
               -- 差押区分
               rec_shokai_fudosan.sashiosae_kbn := 0;
               -- 登記年月日
               rec_shokai_fudosan.toki_ymd := 0;
               -- 受付番号
               rec_shokai_fudosan.uketsuke_no := NULL;
               -- 差押執行機関名
               rec_shokai_fudosan.sashiosae_kikan := NULL;
               -- 差押執行機関住所
               rec_shokai_fudosan.sashiosae_kikan_jusho := NULL;
               -- 差押執行機関郵便番号
               rec_shokai_fudosan.sashiosae_kikan_yubin_no := NULL;
               -- 差押執行機関入力区分
               rec_shokai_fudosan.sashiosae_kikan_nyuryoku_kbn := NULL;
               -- 差押執行機関市区町村コード
               rec_shokai_fudosan.sashiosae_kikan_shikuchoson_cd := NULL;
               -- 差押執行機関町字コード
               rec_shokai_fudosan.sashiosae_kikan_machiaza_cd := NULL;
               -- 差押執行機関都道府県
               rec_shokai_fudosan.sashiosae_kikan_todofuken := NULL;
               -- 差押執行機関市区郡町村名
               rec_shokai_fudosan.sashiosae_kikan_shikugunchoson := NULL;
               -- 差押執行機関町字
               rec_shokai_fudosan.sashiosae_kikan_machiaza := NULL;
               -- 差押執行機関番地号表記
               rec_shokai_fudosan.sashiosae_kikan_banchigohyoki := NULL;
               -- 差押執行機関住所方書
               rec_shokai_fudosan.sashiosae_kikan_jusho_katagaki := NULL;
               -- 差押執行機関確定住所
               rec_shokai_fudosan.sashiosae_kikan_kakutei_jusho := NULL;
               -- 差押執行機関国名コード
               rec_shokai_fudosan.sashiosae_kikan_kokumei_cd := NULL;
               -- 差押執行機関国名等
               rec_shokai_fudosan.kojsashiosae_kikan_kokumeitoin_no := NULL;
               -- 差押執行機関国外住所
               rec_shokai_fudosan.sashiosae_kikan_kokugai_jusho := NULL;
               -- 差押執行機関名カナ
               rec_shokai_fudosan.sashiosae_kikan_kana := NULL;
               -- 差押執行機関所属部署
               rec_shokai_fudosan.sashiosae_busho := NULL;
               -- 差押執行機関電話番号
               rec_shokai_fudosan.sashiosae_denwa_no := NULL;
               -- 売却フラグ
               rec_shokai_fudosan.baikyaku_flg := 0;
               -- 不動産番号
               rec_shokai_fudosan.fudosan_no := NULL;
               -- 持分（○○／○○）
               rec_shokai_fudosan.mochibun := NULL;
               -- 差押可否フラグ
               rec_shokai_fudosan.sashiosae_kahi_flg := 0;
               -- 関連照会SEQ
               rec_shokai_fudosan.kanren_seq_no_shokai := 0;
               -- 連携フラグ
               rec_shokai_fudosan.renkei_flg := 1;
               -- データ作成日時
               rec_shokai_fudosan.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
               -- データ更新日時
               rec_shokai_fudosan.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
               -- 更新担当者コード
               rec_shokai_fudosan.upd_tantosha_cd := rec_main.sosasha_cd;
               -- 更新端末名称
               rec_shokai_fudosan.upd_tammatsu := 'SERVER';
               -- 削除フラグ
               rec_shokai_fudosan.del_flg := rec_main.del_flg::numeric;

               -- ロック情報取得
               OPEN cur_lock;
                  FETCH cur_lock INTO rec_lock;
               CLOSE cur_lock;

               IF rec_lock.kojin_no IS NULL THEN
                  BEGIN
                     INSERT INTO f_shokai_fudosan (
                           busho_cd
                           ,kojin_no
                           ,seq_no_shokai
                           ,bukken_no
                           ,bukken_shurui_cd
                           ,bukken_shozai
                           ,bukken_shozai_chiban
                           ,kaoku_no
                           ,bukken_fugo
                           ,chimoku
                           ,kozo
                           ,chiseki_yuka_menseki
                           ,shikichiken_cd
                           ,shikichiken_wariai
                           ,itto_tatemono_no
                           ,itto_tatemono_kozo
                           ,senyu_no
                           ,senyu_menseki
                           ,teitoken_flg
                           ,sashiosae_kbn
                           ,toki_ymd
                           ,uketsuke_no
                           ,sashiosae_kikan
                           ,sashiosae_kikan_jusho
                           ,sashiosae_kikan_yubin_no
                           ,sashiosae_kikan_nyuryoku_kbn
                           ,sashiosae_kikan_shikuchoson_cd
                           ,sashiosae_kikan_machiaza_cd
                           ,sashiosae_kikan_todofuken
                           ,sashiosae_kikan_shikugunchoson
                           ,sashiosae_kikan_machiaza
                           ,sashiosae_kikan_banchigohyoki
                           ,sashiosae_kikan_jusho_katagaki
                           ,sashiosae_kikan_kakutei_jusho
                           ,sashiosae_kikan_kokumei_cd
                           ,sashiosae_kikan_kokumeito
                           ,sashiosae_kikan_kokugai_jusho
                           ,sashiosae_kikan_kana
                           ,sashiosae_busho
                           ,sashiosae_denwa_no
                           ,baikyaku_flg
                           ,fudosan_no
                           ,mochibun
                           ,sashiosae_kahi_flg
                           ,kanren_seq_no_shokai
                           ,renkei_flg
                           ,ins_datetime
                           ,upd_datetime
                           ,upd_tantosha_cd
                           ,upd_tammatsu
                           ,del_flg
                        ) VALUES (
                           rec_shokai_fudosan.busho_cd
                           ,rec_shokai_fudosan.kojin_no
                           ,rec_shokai_fudosan.seq_no_shokai
                           ,rec_shokai_fudosan.bukken_no
                           ,rec_shokai_fudosan.bukken_shurui_cd
                           ,rec_shokai_fudosan.bukken_shozai
                           ,rec_shokai_fudosan.bukken_shozai_chiban
                           ,rec_shokai_fudosan.kaoku_no
                           ,rec_shokai_fudosan.bukken_fugo
                           ,rec_shokai_fudosan.chimoku
                           ,rec_shokai_fudosan.kozo
                           ,rec_shokai_fudosan.chiseki_yuka_menseki
                           ,rec_shokai_fudosan.shikichiken_cd
                           ,rec_shokai_fudosan.shikichiken_wariai
                           ,rec_shokai_fudosan.itto_tatemono_no
                           ,rec_shokai_fudosan.itto_tatemono_kozo
                           ,rec_shokai_fudosan.senyu_no
                           ,rec_shokai_fudosan.senyu_menseki
                           ,rec_shokai_fudosan.teitoken_flg
                           ,rec_shokai_fudosan.sashiosae_kbn
                           ,rec_shokai_fudosan.toki_ymd
                           ,rec_shokai_fudosan.uketsuke_no
                           ,rec_shokai_fudosan.sashiosae_kikan
                           ,rec_shokai_fudosan.sashiosae_kikan_jusho
                           ,rec_shokai_fudosan.sashiosae_kikan_yubin_no
                           ,rec_shokai_fudosan.sashiosae_kikan_nyuryoku_kbn
                           ,rec_shokai_fudosan.sashiosae_kikan_shikuchoson_cd
                           ,rec_shokai_fudosan.sashiosae_kikan_machiaza_cd
                           ,rec_shokai_fudosan.sashiosae_kikan_todofuken
                           ,rec_shokai_fudosan.sashiosae_kikan_shikugunchoson
                           ,rec_shokai_fudosan.sashiosae_kikan_machiaza
                           ,rec_shokai_fudosan.sashiosae_kikan_banchigohyoki
                           ,rec_shokai_fudosan.sashiosae_kikan_jusho_katagaki
                           ,rec_shokai_fudosan.sashiosae_kikan_kakutei_jusho
                           ,rec_shokai_fudosan.sashiosae_kikan_kokumei_cd
                           ,rec_shokai_fudosan.sashiosae_kikan_kokumeito
                           ,rec_shokai_fudosan.sashiosae_kikan_kokugai_jusho
                           ,rec_shokai_fudosan.sashiosae_kikan_kana
                           ,rec_shokai_fudosan.sashiosae_busho
                           ,rec_shokai_fudosan.sashiosae_denwa_no
                           ,rec_shokai_fudosan.baikyaku_flg
                           ,rec_shokai_fudosan.fudosan_no
                           ,rec_shokai_fudosan.mochibun
                           ,rec_shokai_fudosan.sashiosae_kahi_flg
                           ,rec_shokai_fudosan.kanren_seq_no_shokai
                           ,rec_shokai_fudosan.renkei_flg
                           ,rec_shokai_fudosan.ins_datetime
                           ,rec_shokai_fudosan.upd_datetime
                           ,rec_shokai_fudosan.upd_tantosha_cd
                           ,rec_shokai_fudosan.upd_tammatsu
                           ,rec_shokai_fudosan.del_flg
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
                     UPDATE f_shokai_fudosan
                     SET bukken_shozai = rec_shokai_fudosan.bukken_shozai
                        , chimoku = rec_shokai_fudosan.chimoku
                        , chiseki_yuka_menseki = rec_shokai_fudosan.chiseki_yuka_menseki
                        , kanren_seq_no_shokai = rec_lock.kanren_seq_no_shokai
                        , upd_datetime = concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp
                        , upd_tantosha_cd = rec_main.sosasha_cd
                        , upd_tammatsu = rec_shokai_fudosan.upd_tammatsu
                        , del_flg = rec_shokai_fudosan.del_flg
                     WHERE busho_cd = rec_busho.busho_cd
                        AND kojin_no = rec_shokai_fudosan.kojin_no 
                        AND seq_no_shokai = SEQ_SHOKAI.NEXTVAL
                        AND bukken_no = rec_shokai_fudosan.bukken_no
                        AND bukken_shurui_cd = rec_shokai_fudosan.bukken_shurui_cd;

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

          END LOOP;
      CLOSE cur_busho;
      
      -- 中間テーブルの「削除フラグ」が「1」のデータは「3：削除」を指定する
      IF rec_kojin.del_flg = 1 THEN
          ln_result_cd := ln_result_cd_del;
      END IF;

      -- 中間テーブル更新
      UPDATE dlgrenkei.i_r4g_tochi 
      SET result_cd = ln_result_cd
          , error_cd = lc_err_cd
          , error_text = lc_err_text
      WHERE shikuchoson_cd = rec_main.shikuchoson_cd
          AND bukken_no = rec_main.bukken_no
          AND kazei_nendo = rec_main.kazei_nendo
          AND tochi_kihon_rireki_no = rec_main.tochi_kihon_rireki_no;

      -- 他処理と合わせてコミットのタイミングは「10,000件ごと」に固定とする
      IF MOD( ln_shori_count, 10000 ) = 0 THEN
            COMMIT;
      END IF;

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