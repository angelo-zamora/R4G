--------------------------------------------------------
--  DDL for Procedure proc_r4g_sharyo
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_sharyo (
   in_n_renkei_data_cd IN numeric,
   in_n_renkei_seq IN numeric,
   in_n_shori_ymd IN numeric,
   io_c_err_code INOUT character varying,
   io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : f_車両_連携（f_sharyo_renkei）の追加／更新／削除を実施する                                              */
/* 引数 IN  :  in_n_renkei_data_cd … 連携データコード                                                                */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :  CRESS-INFO.Angelo     新規作成     013o005「車両情報管理」の取込を行う                                 */
/**********************************************************************************************************************/

DECLARE
   rec_f_sharyo_renkei                 dlgrenkei.f_sharyo_renkei%ROWTYPE;
   ln_para01                           numeric DEFAULT 0;
   ln_para02                           numeric DEFAULT 0;

   ln_shori_count                      numeric;
   ln_ins_count                        numeric;
   ln_upd_count                        numeric;
   ln_del_count                        numeric;
   ln_err_count                        numeric;
   lc_err_text                         character varying(100);
   ln_result_cd                        numeric DEFAULT 0;
   lc_err_cd                           character varying;
   lc_sql                              character varying(1000);

   lc_bukken_no                        character varying;
   ln_kazeinendo                       numeric;
   ln_rireki_no                        numeric;

   rec_log                             dlgrenkei.f_renkei_log%ROWTYPE;

   cur_main CURSOR FOR
   SELECT *
	FROM dlgrenkei.i_r4g_sharyo
	WHERE saishin_flg = '1'
	AND keiji_rireki_no = (
		SELECT MAX(keiji_rireki_no)
		FROM dlgrenkei.i_r4g_sharyo
		)
	AND result_cd < 8;

   rec_main                          dlgrenkei.i_r4g_sharyo%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                     dlgrenkei.f_renkei_parameter%ROWTYPE;

   cur_lock CURSOR FOR
   SELECT *
   FROM f_sharyo_renkei
   WHERE keiji_kanri_no = rec_main.keiji_kanri_no;

   rec_lock                       f_sharyo_renkei%ROWTYPE;

BEGIN
   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   --パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;

      END LOOP;
   CLOSE cur_parameter;

   --連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_sharyo_renkei;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_sharyo_renkei';
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

         -- 軽自管理番号
         rec_f_sharyo_renkei.keiji_kanri_no := rec_main.keiji_kanri_no;
         -- 納税義務者_宛名番号
         rec_f_sharyo_renkei.gimusha_kojin_no := rec_main.gimusha_atena_no;
         -- 一括納税対象者区分
         rec_f_sharyo_renkei.ikkatsu_nozei_kbn := rec_main.ikkatsu_nozei_taishosha_kbn::numeric;
         -- 使用者_宛名番号
         rec_f_sharyo_renkei.shiyosha_kojin_no := rec_main.shiyosha_atena_no;
         -- 使用者_宛名番号
         rec_f_sharyo_renkei.shoyusha_kojin_no := rec_main.shoyusha_atena_no;
         -- 申告区分
         rec_f_sharyo_renkei.shinkoku_kbn := rec_main.shinkoku_kbn::numeric;
         -- 申告事由
         rec_f_sharyo_renkei.shinkoku_jiyu := rec_main.shinkoku_jiyu;
         -- 申告年月日
         rec_f_sharyo_renkei.shinkoku_ymd := get_date_to_num(rec_main.shinkoku_ymd);
         -- 異動（登録・取得）年月日
         rec_f_sharyo_renkei.ido_ymd := get_date_to_num(rec_main.ido_ymd);
         -- 車両情報の異動年月日
         rec_f_sharyo_renkei.syaryo_ido_ymd := get_date_to_num(rec_main.sharyo_ido_ymd);
         -- 異動事由
         rec_f_sharyo_renkei.ido_jiyu := rec_main.ido_jiyu::numeric;
         -- 処理年月日
         rec_f_sharyo_renkei.shori_ymd := get_date_to_num(rec_main.keiji_shori_ymd);
         -- 種別コード
         rec_f_sharyo_renkei.keiji_syubetsu_cd := rec_main.shubetsu_cd::numeric;
         -- 排気区分
         rec_f_sharyo_renkei.haiki_kbn := rec_main.haiki_kbn::numeric;
         -- 総排気量又は定格出力
         rec_f_sharyo_renkei.so_haikiryo := rec_main.sohaikiryo;
         -- 車台番号
         rec_f_sharyo_renkei.shadai_no := get_trimmed_space(rec_main.shatai_no);
         -- 初度検査（届出）年月
         rec_f_sharyo_renkei.shodo_kensa_ym := TO_NUMBER(TO_CHAR(rec_main.shodo_ym, 'YYYYMM'));
         -- 所有形態区分
         rec_f_sharyo_renkei.shoyu_keitai_kbn := rec_main.shoyukeitai_kbn::numeric;
         -- 車両番号（標識番号）_標板文字
         rec_f_sharyo_renkei.hyoban_moji := rec_main.hyoban_moji;
         -- 車両番号（標識番号）_分類番号
         rec_f_sharyo_renkei.bunrui_no := rec_main.bunrui_no;
         -- 車両番号（標識番号）_かな文字
         rec_f_sharyo_renkei.kana_moji := rec_main.kana_moji;
         -- 車両番号（標識番号）_一連指定番号
         rec_f_sharyo_renkei.ichiren_shitei_no := rec_main.shiteibango;
         -- 課税区分
         rec_f_sharyo_renkei.kazei_kbn := rec_main.kazei_kbn::numeric;
         -- 廃車年月日
         rec_f_sharyo_renkei.haisha_ymd := get_date_to_num(rec_main.haisha_ymd);
         -- 軽自履歴番号
         rec_f_sharyo_renkei.keiji_rireki_no := rec_main.keiji_rireki_no::numeric;
         -- データ作成日時
         rec_f_sharyo_renkei.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- データ更新日時
         rec_f_sharyo_renkei.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
         -- 更新担当者コード
         rec_f_sharyo_renkei.upd_tantosha_cd := rec_main.upd_tantosha_cd;
         -- 更新端末名称
         rec_f_sharyo_renkei.upd_tammatsu := 'SERVER';
         -- 削除フラグ
         rec_f_sharyo_renkei.del_flg := rec_main.del_flg::numeric;

            OPEN cur_lock;
               FETCH cur_lock INTO rec_lock;
            CLOSE cur_lock;

            IF rec_lock IS NULL THEN
               BEGIN
                  -- 登録処理
                  INSERT INTO f_sharyo_renkei(
                     keiji_kanri_no
                     , gimusha_kojin_no
                     , ikkatsu_nozei_kbn
                     , shiyosha_kojin_no
                     , shoyusha_kojin_no
                     , shinkoku_kbn
                     , shinkoku_jiyu
                     , shinkoku_ymd
                     , ido_ymd
                     , syaryo_ido_ymd
                     , ido_jiyu
                     , shori_ymd
                     , keiji_syubetsu_cd
                     , haiki_kbn
                     , so_haikiryo
                     , shadai_no
                     , shodo_kensa_ym
                     , shoyu_keitai_kbn
                     , hyoban_moji
                     , bunrui_no
                     , kana_moji
                     , ichiren_shitei_no
                     , kazei_kbn
                     , haisha_ymd
                     , keiji_rireki_no
                     , ins_datetime
                     , upd_datetime
                     , upd_tantosha_cd
                     , upd_tammatsu
                     , del_flg
                  )
                  VALUES (
                     rec_f_sharyo_renkei.keiji_kanri_no
                     , rec_f_sharyo_renkei.gimusha_kojin_no
                     , rec_f_sharyo_renkei.ikkatsu_nozei_kbn
                     , rec_f_sharyo_renkei.shiyosha_kojin_no
                     , rec_f_sharyo_renkei.shoyusha_kojin_no
                     , rec_f_sharyo_renkei.shinkoku_kbn
                     , rec_f_sharyo_renkei.shinkoku_jiyu
                     , rec_f_sharyo_renkei.shinkoku_ymd
                     , rec_f_sharyo_renkei.ido_ymd
                     , rec_f_sharyo_renkei.syaryo_ido_ymd
                     , rec_f_sharyo_renkei.ido_jiyu
                     , rec_f_sharyo_renkei.shori_ymd
                     , rec_f_sharyo_renkei.keiji_syubetsu_cd
                     , rec_f_sharyo_renkei.haiki_kbn
                     , rec_f_sharyo_renkei.so_haikiryo
                     , rec_f_sharyo_renkei.shadai_no
                     , rec_f_sharyo_renkei.shodo_kensa_ym
                     , rec_f_sharyo_renkei.shoyu_keitai_kbn
                     , rec_f_sharyo_renkei.hyoban_moji
                     , rec_f_sharyo_renkei.bunrui_no
                     , rec_f_sharyo_renkei.kana_moji
                     , rec_f_sharyo_renkei.ichiren_shitei_no
                     , rec_f_sharyo_renkei.kazei_kbn
                     , rec_f_sharyo_renkei.haisha_ymd
                     , rec_f_sharyo_renkei.keiji_rireki_no
                     , rec_f_sharyo_renkei.ins_datetime
                     , rec_f_sharyo_renkei.upd_datetime
                     , rec_f_sharyo_renkei.upd_tantosha_cd
                     , rec_f_sharyo_renkei.upd_tammatsu
                     , rec_f_sharyo_renkei.del_flg
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
               -- 連携データの作成・更新
               BEGIN
                  UPDATE f_sharyo_renkei
                     SET gimusha_kojin_no = rec_f_sharyo_renkei.gimusha_kojin_no
                     , ikkatsu_nozei_kbn = rec_f_sharyo_renkei.ikkatsu_nozei_kbn
                     , shiyosha_kojin_no = rec_f_sharyo_renkei.shiyosha_kojin_no
                     , shoyusha_kojin_no = rec_f_sharyo_renkei.shoyusha_kojin_no
                     , shinkoku_kbn = rec_f_sharyo_renkei.shinkoku_kbn
                     , shinkoku_jiyu = rec_f_sharyo_renkei.shinkoku_jiyu
                     , shinkoku_ymd = rec_f_sharyo_renkei.shinkoku_ymd
                     , ido_ymd = rec_f_sharyo_renkei.ido_ymd
                     , syaryo_ido_ymd = rec_f_sharyo_renkei.syaryo_ido_ymd
                     , ido_jiyu = rec_f_sharyo_renkei.ido_jiyu
                     , shori_ymd = rec_f_sharyo_renkei.shori_ymd
                     , keiji_syubetsu_cd = rec_f_sharyo_renkei.keiji_syubetsu_cd
                     , haiki_kbn = rec_f_sharyo_renkei.haiki_kbn
                     , so_haikiryo = rec_f_sharyo_renkei.so_haikiryo
                     , shadai_no = rec_f_sharyo_renkei.shadai_no
                     , shodo_kensa_ym = rec_f_sharyo_renkei.shodo_kensa_ym
                     , shoyu_keitai_kbn = rec_f_sharyo_renkei.shoyu_keitai_kbn
                     , hyoban_moji = rec_f_sharyo_renkei.hyoban_moji
                     , bunrui_no = rec_f_sharyo_renkei.bunrui_no
                     , kana_moji = rec_f_sharyo_renkei.kana_moji
                     , ichiren_shitei_no = rec_f_sharyo_renkei.ichiren_shitei_no
                     , kazei_kbn = rec_f_sharyo_renkei.kazei_kbn
                     , haisha_ymd = rec_f_sharyo_renkei.haisha_ymd
                     , keiji_rireki_no = rec_f_sharyo_renkei.keiji_rireki_no
                     , upd_datetime = rec_f_sharyo_renkei.upd_datetime
                     , upd_tantosha_cd = rec_f_sharyo_renkei.upd_tantosha_cd
                     , upd_tammatsu = rec_f_sharyo_renkei.upd_tammatsu
                     , del_flg = rec_f_sharyo_renkei.del_flg
                  WHERE 
                     keiji_kanri_no = rec_f_sharyo_renkei.keiji_kanri_no;

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
         END IF;

         -- 中間テーブル更新
         UPDATE dlgrenkei.i_r4g_sharyo
            SET result_cd = ln_result_cd
            , error_cd = lc_err_cd
            , error_text = lc_err_text
            , seq_no_renkei = in_n_renkei_seq
            , shori_ymd     = in_n_shori_ymd
            WHERE  
            shikuchoson_cd = rec_main.shikuchoson_cd
            keiji_kanri_no = rec_main.keiji_kanri_no
            keiji_rireki_no = rec_main.keiji_rireki_no

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
