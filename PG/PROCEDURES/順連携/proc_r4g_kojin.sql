--------------------------------------------------------
--  DDL for Procedure  proc_r4g_kojin
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_kojin ( in_n_renkei_data_cd IN numeric, 
                                                       in_n_renkei_seq IN numeric, 
                                                       in_n_shori_ymd IN numeric, 
                                                       io_c_err_code INOUT character varying, 
                                                       io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 個人情報（f_kojin）の追加／更新／削除を実施する                                                             */
/* 引数 IN  : in_n_renkei_data_cd  … 連携データコード                                                                   */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                  */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                       */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                        */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/22  CRESS-INFO.Drexler   001o006「住民情報（個人番号あり）」の取込を行う                            */
/**********************************************************************************************************************/

DECLARE
   rec_kojin                      f_kojin%ROWTYPE;
   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;

   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_del_diag_count              numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100):='';
   ln_result_cd                   numeric DEFAULT 0;
   ln_result_del                  numeric DEFAULT 3;
   lc_err_cd                      character varying;

   ln_para01                      numeric DEFAULT 0;
   ln_para02                      numeric DEFAULT 0;
   ln_para09                      numeric DEFAULT 0;
   ln_para12                      numeric DEFAULT 0;

   ln_result_cd_add               numeric DEFAULT 1; -- 追加
   ln_result_cd_upd               numeric DEFAULT 2; -- 更新
   ln_result_cd_del               numeric DEFAULT 3; -- 削除
   ln_result_cd_warning           numeric DEFAULT 7; -- 警告
   ln_result_cd_err               numeric DEFAULT 9; -- エラー

   lc_err_cd_normal               character varying = '0'; -- 通常
   lc_err_cd_err                  character varying = '9'; -- エラー

   lc_sql                         character varying(1000);

   cur_main CURSOR FOR
   SELECT *
   FROM i_r4g_atena AS tbl_atena
   WHERE tbl_atena.saishin_flg = '1'
   AND tbl_atena.rireki_no = (
      SELECT MAX(rireki_no)
      FROM i_r4g_atena
      WHERE atena_no = tbl_atena.atena_no
   )
   AND tbl_atena.rireki_no_eda = (
      SELECT MAX(rireki_no_eda)
      FROM i_r4g_atena
      WHERE atena_no = tbl_atena.atena_no
        AND rireki_no = tbl_atena.rireki_no
   )
   AND tbl_atena.result_cd < 8;

   rec_main                       dlgrenkei.i_r4g_atena%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;
   
   cur_lock CURSOR FOR
   SELECT *
   FROM f_kojin
   WHERE kojin_no = rec_kojin.kojin_no;

   rec_lock                       f_kojin%ROWTYPE;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   -- １．パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;
         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 9 THEN ln_para09 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 12 THEN ln_para12 := rec_parameter.parameter_value; END IF;
      END LOOP;
   CLOSE cur_parameter;

   -- ２．連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_kojin;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_kojin';
         EXECUTE lc_sql;
      EXCEPTION WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text := SQLERRM;
         RETURN;
      END;
   END IF;

   -- ３．中間テーブルデータのエラーチェック
   CALL proc_error_check( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );

   IF io_c_err_code <> '0' THEN
      RETURN;
   END IF;
   
   -- ４．桁数設定情報取得
   -- r4gでは不要

   -- ５．連携データの作成・更新
   ln_shori_count := 0;
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

            ln_shori_count                 := ln_shori_count + 1;
            lc_err_cd                      := '0';
            lc_err_text                    := '';
            ln_result_cd                   := 0;
            rec_kojin                      := NULL;
            rec_lock                       := NULL;
            
            -- 個人番号
            rec_kojin.kojin_no := rec_main.atena_no;
            -- 世帯番号
            rec_kojin.setai_no := rec_main.setai_no;
            -- 氏名表示区分
            rec_kojin.shimei_kbn := rec_main.shimei_yusen_kbn;
            -- 管内管外の区分
            rec_kojin.kannai_kbn := 1;
            -- 住民区分
            rec_kojin.jumin_kbn := 1;
            -- 住民種別
            rec_kojin.jumin_shubetsu_cd := rec_main.jumin_shubetsu_cd::numeric;
            -- 住民状態
            rec_kojin.jumin_jotai_cd := rec_main.jumin_jotai_cd::numeric;
            -- 連携氏名
            rec_kojin.renkei_shimei := get_trimmed_space(rec_main.shimei);
            -- 連携氏名カタカナ
            rec_kojin.renkei_shimei_kana := get_trimmed_space(rec_main.shimei_kana);
            -- 連携通称名
            rec_kojin.renkei_tsushomei := get_trimmed_space(rec_main.tsushomei);
            -- 連携通称名カタカナ
            rec_kojin.renkei_tsushomei_kana := get_trimmed_space(rec_main.tsushomei_kana);
            -- 連携通称名_フリガナ確認状況
            rec_kojin.renkei_tsushomei_kana_flg := CASE WHEN rec_main.tsushomei_kana_flg IS NULL OR rec_main.tsushomei_kana_flg = '' THEN 0 ELSE rec_main.tsushomei_kana_flg::numeric END;
            -- 氏名
            rec_kojin.shimei := get_trimmed_space(rec_main.shimei);
            -- 氏_日本人
            rec_kojin.nihonjin_uji := get_trimmed_space(rec_main.uji_nihonjin);
            -- 名_日本人
            rec_kojin.nihonjin_mei := get_trimmed_space(rec_main.mei_nihonjin);
            -- 氏名カタカナ
            rec_kojin.shimei_kana := get_trimmed_space(rec_main.shimei_kana);
            -- 氏名_フリガナ公証確認状況
            rec_kojin.shimei_kana_kosho_kakunin_kbn := rec_main.shimei_kana_kosho_kakunin_kbn::numeric;
            -- 氏名カタカナ(検索用)
            rec_kojin.shimei_kensaku_kana := get_trimmed_space(get_kensaku_kana(rec_main.tsushomei_kana_flg, 2));
            -- 氏_日本人_フリガナ
            rec_kojin.nihonjin_uji_kana := get_trimmed_space(rec_main.uji_nihonjin_kana);
            -- 名_日本人_フリガナ
            rec_kojin.nihonjin_mei_kana := get_trimmed_space(rec_main.mei_nihonjin_kana);
            -- 氏名_外国人ローマ字
            rec_kojin.shimei_gaikokujin_romaji := get_trimmed_space(rec_main.shimei_gaikokujin_romaji);
            -- 氏名_外国人漢字
            rec_kojin.shimei_gaikokujin_kanji := get_trimmed_space(rec_main.shimei_gaikokujin_kanji);
            -- 旧氏
            rec_kojin.old_uji := rec_main.old_uji;
            -- 旧氏カタカナ
            rec_kojin.old_uji_kana := rec_main.old_uji_kana;
            -- 旧氏_フリガナ確認状況
            rec_kojin.old_uji_kana_flg := rec_main.old_uji_kana_flg;
            -- 郵便番号
            rec_kojin.yubin_no := get_trimmed_space(rec_main.jusho_yubin_no);
            -- 住所
            rec_kojin.jusho := rec_main.jusho_todofuken || rec_main.jusho_shikugunchoson || rec_main.jusho_machiaza_cd || rec_main.jusho_banchigohyoki;
            -- 住所コード
            rec_kojin.jusho_cd := LPAD(rec_main.jusho_shikuchoson_cd::character varying, 6, '0') ||  LPAD(rec_main.jusho_machiaza_cd::character varying, 7, '0') ;
            -- 市区町村コード
            rec_kojin.shikuchoson_cd := rec_main.tennyumae_shikuchoson_cd;
            -- 町字コード
            rec_kojin.machiaza_cd := rec_main.jusho_machiaza_cd;
            -- 都道府県
            rec_kojin.todofuken := get_trimmed_space(rec_main.jusho_todofuken);
            -- 市区町村名
            rec_kojin.shikugunchoson := get_trimmed_space(rec_main.jusho_shikugunchoson);
            -- 町字
            rec_kojin.machiaza := get_trimmed_space(rec_main.jusho_machiaza);
            -- 番地号表記
            rec_kojin.banchigohyoki := get_trimmed_space(rec_main.jusho_banchigohyoki);
            -- 番地枝番数値
            rec_kojin.banchi_edaban := get_trimmed_space(rec_main.jusho_banchi_eda);
            -- 方書コード
            rec_kojin.jusho_katagaki_cd := get_trimmed_space(rec_main.jusho_katagaki_cd);
            -- 住所方書
            rec_kojin.jusho_katagaki := get_trimmed_space(rec_main.jusho_katagaki);
            -- 住所_方書_フリガナ
            rec_kojin.jusho_katagaki_kana := get_trimmed_space(rec_main.jusho_katagaki_kana);
            -- 住所_国名コード
            rec_kojin.kokumei_cd := NULL;
            -- 住所_国名等
            rec_kojin.kokumeito := NULL;
            -- 住所_国外住所
            rec_kojin.kokugai_jusho := NULL;
            -- 戸籍本籍
            rec_kojin.koseki_honseki := CASE WHEN rec_main.honseki IS NULL OR rec_main.honseki = '' 
                                       THEN  rec_main.honseki_todofuken || rec_main.honseki_shikugunchoson|| rec_main.honseki_machiaza || rec_main.honseki_chibango 
                                       ELSE get_trimmed_space(rec_main.honseki) END;
            -- 本籍_市区町村コード
            rec_kojin.honseki_shikuchoson_cd := rec_main.honseki_shikuchoson_cd;
            -- 本籍_町字コード
            rec_kojin.honseki_machiaza_cd := rec_main.honseki_machiaza_cd;
            -- 本籍_都道府県
            rec_kojin.honseki_todofuken := get_trimmed_space(rec_main.honseki_todofuken);
            -- 本籍_市区郡町村名
            rec_kojin.honseki_shikugunchoson := get_trimmed_space(rec_main.honseki_shikugunchoson);
            -- 本籍_町字
            rec_kojin.honseki_machiaza := get_trimmed_space(rec_main.honseki_machiaza);
            -- 本籍_地番号または、街区符号
            rec_kojin.honseki_banchigohyoki := get_trimmed_space(rec_main.honseki_chibango);
            -- 戸籍筆頭者
            rec_kojin.koseki_hittosha := get_trimmed_space(rec_main.koseki_hitto);
            -- 戸籍_筆頭者_氏
            rec_kojin.koseki_hittosha_uji := get_trimmed_space(rec_main.koseki_hitto_uji);
            -- 戸籍_筆頭者_名
            rec_kojin.koseki_hittosha_mei := get_trimmed_space(rec_main.koseki_hitto_mei);
            -- 生年月日
            rec_kojin.birth_ymd := CASE WHEN ymd_NULL_check(rec_main.birth_ymd) = TRUE THEN 0 ELSE get_trimmed_date(rec_main.birth_ymd) END;
            -- 生年月日_不詳フラグ
            rec_kojin.birth_fusho_flg := rec_main.birth_fusho_flg::numeric;
            -- 生年月日_不詳表記
            rec_kojin.birth_fusho := rec_main.birth_fusho;
            -- 続柄１
            rec_kojin.zokugara_cd1 := get_r4g_code_conv(0, 4, rec_main.zokugara_cd1, NULL);
            -- 続柄２
            rec_kojin.zokugara_cd2 := get_r4g_code_conv(0, 4, rec_main.zokugara_cd2, NULL);
            -- 続柄３
            rec_kojin.zokugara_cd3 := get_r4g_code_conv(0, 4, rec_main.zokugara_cd3, NULL);
            -- 続柄４
            rec_kojin.zokugara_cd4 := get_r4g_code_conv(0, 4, rec_main.zokugara_cd4, NULL);
            -- 続柄表記
            rec_kojin.zokugara := rec_main.zokugara;
            -- 性別コード
            rec_kojin.seibetsu_cd := rec_main.seibetsu_cd::numeric;
            -- 性別表記
            rec_kojin.seibetsu := rec_main.seibetsu;
            -- 地区コード
            rec_kojin.chiku_cd := get_chiku_cd(0, rec_main.mynumber, rec_main.shikuchoson_cd);
            -- 行政区コード
            rec_kojin.gyoseiku_cd := NULL;
            -- 自治体コード
            rec_kojin.jichitai_cd := rec_main.jusho_shikuchoson_cd;
            -- 処理年月日
            rec_kojin.shori_ymd := get_trimmed_date(rec_main.atena_shori_ymd);
            -- 異動年月日
            rec_kojin.ido_ymd := get_trimmed_date(rec_main.ido_ymd);
            -- 異動年月日_不詳フラグ
            rec_kojin.ido_ymd_fusho_flg := rec_main.ido_fusho_flg::numeric;
            -- 異動年月日_不詳表記
            rec_kojin.ido_ymd_fusho := rec_main.ido_fusho;
            -- 異動届出年月日
            rec_kojin.ido_todoke_ymd := CASE WHEN ymd_NULL_check(rec_main.ido_todoke_ymd) = TRUE THEN 0 ELSE get_trimmed_date(rec_main.ido_todoke_ymd) END;
            -- 異動事由コード
            rec_kojin.ido_jiyu_cd := rec_main.ido_jiyu_cd;
            -- 死亡年月日
            rec_kojin.shibo_ymd := CASE WHEN rec_main.ido_jiyu_cd  = '23' THEN get_trimmed_date(rec_main.ido_ymd) ELSE 0 END;
            -- 住民日
            rec_kojin.jumin_ymd := CASE WHEN ymd_NULL_check(rec_main.jumin_ymd) = TRUE THEN 0 ELSE get_trimmed_date(rec_main.jumin_ymd) END;
            -- 住民日_不詳フラグ
            rec_kojin.jumin_ymd_fusho_flg := CASE WHEN rec_main.jumin_fusho_flg IS NULL OR rec_main.jumin_fusho_flg = '' THEN 0 ELSE rec_main.jumin_fusho_flg::numeric END;
            -- 住民日_不詳表記
            rec_kojin.jumin_ymd_fusho := rec_main.jumin_fusho;
            -- 外国人住民日
            rec_kojin.gaikokujin_jumin_ymd := CASE WHEN ymd_NULL_check(rec_main.gaikokujin_jumin_ymd) = TRUE THEN 0 ELSE get_trimmed_date(rec_main.gaikokujin_jumin_ymd) END;
            -- 外国人住民日_不詳フラグ
            rec_kojin.gaikokujin_jumin_ymd_fusho_flg := CASE WHEN rec_main.gaikokujin_jumin_fusho_flg IS NULL OR rec_main.gaikokujin_jumin_fusho_flg = '' THEN 0 ELSE rec_main.gaikokujin_jumin_fusho_flg::numeric END;
            -- 外国人住民日_不詳表記
            rec_kojin.gaikokujin_jumin_ymd_fusho := rec_main.gaikokujin_jumin_fusho;
            -- 住定日
            rec_kojin.jutei_ymd := CASE WHEN ymd_NULL_check(rec_main.jutei_ymd) = TRUE THEN 0 ELSE get_trimmed_date(rec_main.jutei_ymd) END;
            -- 住定日_不詳フラグ
            rec_kojin.jutei_ymd_fusho_flg := rec_main.jutei_fusho_flg;
            -- 住定日_不詳表記
            rec_kojin.jutei_ymd_fusho := rec_main.jutei_fusho;
            -- 転入通知年月日
            rec_kojin.tennyu_tsuchi_ymd := CASE WHEN ymd_NULL_check(rec_main.tennyu_tsuchi_ymd) = TRUE THEN 0 ELSE get_trimmed_date(rec_main.tennyu_tsuchi_ymd) END;
            -- 転出届出年月日
            rec_kojin.tenshutsu_todoke_ymd := CASE WHEN ymd_NULL_check(rec_main.tenshutsu_todoke_ymd) = TRUE THEN 0 ELSE get_trimmed_date(rec_main.tenshutsu_todoke_ymd) END;
            -- 転出予定年月日
            rec_kojin.tenshutsu_yotei_ymd := CASE WHEN ymd_NULL_check(rec_main.tenshutsu_yotei_ymd) = TRUE THEN 0 ELSE get_trimmed_date(rec_main.tenshutsu_yotei_ymd) END;
            -- 転出年月日（確定）
            rec_kojin.tenshutsu_ymd := CASE WHEN ymd_NULL_check(rec_main.tenshutsu_ymd) = TRUE THEN 0 ELSE get_trimmed_date(rec_main.tenshutsu_ymd) END;
            -- 通称名カタカナ(検索用)
            rec_kojin.tsushomei_kensaku_kana := get_kensaku_kana(rec_main.tsushomei_kana, 2);
            -- 旧氏カタカナ(検索用)
            rec_kojin.old_uji_kensaku_kana := get_kensaku_kana(rec_main.old_uji_kana, 2);
            -- 在留カード等番号
            rec_kojin.zairyu_card_no := rec_main.zairyu_card_no;
            -- 在留カード等番号区分コード
            rec_kojin.zairyu_card_no_kbn_cd := rec_main.zairyu_card_no_kbn_cd;
            -- 国籍コード
            rec_kojin.kokuseki_cd := rec_main.kokumei_cd::numeric;
            -- 国籍名等
            rec_kojin.kokusekimeito := rec_main.kokusekimeito;
            -- 第30条45規定区分コード
            rec_kojin.zairyu_kitei_kbn_cd := rec_main.zairyu_kitei_kbn_cd::numeric;
            -- 在留資格コード
            rec_kojin.zairyu_shikaku_cd := rec_main.zairyu_shikaku_cd;
            -- 在留資格等名称
            rec_kojin.zairyu_shikaku := NULL;
            -- 在留期間等年コード
            rec_kojin.zairyu_kikan_nen_cd := rec_main.zairyu_kikan_nen_cd::numeric;
            -- 在留期間等月コード
            rec_kojin.zairyu_kikan_tsuki_cd := rec_main.zairyu_kikan_tsuki_cd::numeric;
            -- 在留期間等日コード
            rec_kojin.zairyu_kikan_hi_cd := rec_main.zairyu_kikan_hi_cd::numeric;
            -- 在留期間等満了年月日
            rec_kojin.zairyu_manryo_ymd := get_trimmed_date(rec_main.zairyu_manryo_ymd);
            -- 記載順位
            rec_kojin.kisai_juni := rec_main.kisai_juni::numeric;
            -- 法第30条46又は47区分
            rec_kojin.zairyu_todoke_kbn_cd := rec_main.zairyu_todoke_kbn_cd::numeric;
            -- 業務コード
            rec_kojin.gyomu_cd := 0;
            -- 予備項目１
            rec_kojin.yobi_komoku1 := NULL;
            -- 予備項目２
            rec_kojin.yobi_komoku2 := NULL;
            -- 予備項目３
            rec_kojin.yobi_komoku3 := NULL;
            -- 予備項目４
            rec_kojin.yobi_komoku4 := NULL;
            -- 予備項目５
            rec_kojin.yobi_komoku5 := NULL;
            -- 同一人番号
            rec_kojin.doitsunin_kojin_no := rec_main.mynumber;
            -- 同一人地区コード
            rec_kojin.doitsunin_chiku_cd := get_chiku_cd(0, rec_main.mynumber, rec_main.shikuchoson_cd);
            -- データ作成日時
            rec_kojin.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            -- データ更新日時
            rec_kojin.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            -- 更新担当者コード
            rec_kojin.upd_tantosha_cd := rec_main.upd_tantosha_cd;
            -- 更新端末名称
            rec_kojin.upd_tammatsu := 'SERVER';
            -- 削除フラグ
            rec_kojin.del_flg := 0;
            
         OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
         CLOSE cur_lock;
         
         -- 削除フラグが「1」の場合は対象データを物理削除する
         IF rec_kojin.del_flg = 1 THEN
            BEGIN 
               DELETE FROM f_kojin
               WHERE kojin_no = rec_kojin.kojin_no;
               GET DIAGNOSTICS ln_del_diag_count := ROW_COUNT;
               ln_del_count = ln_del_count + ln_del_diag_count;
               
               lc_err_text := '';
               lc_err_cd := lc_err_cd_normal;
               ln_result_cd := ln_result_cd_del; 

            EXCEPTION WHEN OTHERS THEN
               ln_err_count := ln_err_count + 1;
               lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
               lc_err_cd := lc_err_cd_err;
               ln_result_cd := ln_result_cd_err;
            END;
         ELSE
            IF rec_lock IS NULL THEN
               BEGIN
                  INSERT INTO f_kojin(
                     kojin_no
                     , setai_no
                     , shimei_kbn
                     , kannai_kbn
                     , jumin_kbn
                     , jumin_shubetsu_cd
                     , jumin_jotai_cd
                     , renkei_shimei
                     , renkei_shimei_kana
                     , renkei_tsushomei
                     , renkei_tsushomei_kana
                     , renkei_tsushomei_kana_flg
                     , shimei
                     , nihonjin_uji
                     , nihonjin_mei
                     , shimei_kana
                     , shimei_kana_kosho_kakunin_kbn
                     , shimei_kensaku_kana
                     , nihonjin_uji_kana
                     , nihonjin_mei_kana
                     , shimei_gaikokujin_romaji
                     , shimei_gaikokujin_kanji
                     , old_uji
                     , old_uji_kana
                     , old_uji_kana_flg
                     , yubin_no
                     , jusho
                     , jusho_cd
                     , shikuchoson_cd
                     , machiaza_cd
                     , todofuken
                     , shikugunchoson
                     , machiaza
                     , banchigohyoki
                     , banchi_edaban
                     , jusho_katagaki_cd
                     , jusho_katagaki
                     , jusho_katagaki_kana
                     , kokumei_cd
                     , kokumeito
                     , kokugai_jusho
                     , koseki_honseki
                     , honseki_shikuchoson_cd
                     , honseki_machiaza_cd
                     , honseki_todofuken
                     , honseki_shikugunchoson
                     , honseki_machiaza
                     , honseki_banchigohyoki
                     , koseki_hittosha
                     , koseki_hittosha_uji
                     , koseki_hittosha_mei
                     , birth_ymd
                     , birth_fusho_flg
                     , birth_fusho
                     , zokugara_cd1
                     , zokugara_cd2
                     , zokugara_cd3
                     , zokugara_cd4
                     , zokugara
                     , seibetsu_cd
                     , seibetsu
                     , chiku_cd
                     , gyoseiku_cd
                     , jichitai_cd
                     , shori_ymd
                     , ido_ymd
                     , ido_ymd_fusho_flg
                     , ido_ymd_fusho
                     , ido_todoke_ymd
                     , ido_jiyu_cd
                     , shibo_ymd
                     , jumin_ymd
                     , jumin_ymd_fusho_flg
                     , jumin_ymd_fusho
                     , gaikokujin_jumin_ymd
                     , gaikokujin_jumin_ymd_fusho_flg
                     , gaikokujin_jumin_ymd_fusho
                     , jutei_ymd
                     , jutei_ymd_fusho_flg
                     , jutei_ymd_fusho
                     , tennyu_tsuchi_ymd
                     , tenshutsu_todoke_ymd
                     , tenshutsu_yotei_ymd
                     , tenshutsu_ymd
                     , tsushomei_kensaku_kana
                     , old_uji_kensaku_kana
                     , zairyu_card_no
                     , zairyu_card_no_kbn_cd
                     , kokuseki_cd
                     , kokusekimeito
                     , zairyu_kitei_kbn_cd
                     , zairyu_shikaku_cd
                     , zairyu_shikaku
                     , zairyu_kikan_nen_cd
                     , zairyu_kikan_tsuki_cd
                     , zairyu_kikan_hi_cd
                     , zairyu_manryo_ymd
                     , kisai_juni
                     , zairyu_todoke_kbn_cd
                     , gyomu_cd
                     , yobi_komoku1
                     , yobi_komoku2
                     , yobi_komoku3
                     , yobi_komoku4
                     , yobi_komoku5
                     , doitsunin_kojin_no
                     , doitsunin_chiku_cd
                     , ins_datetime
                     , upd_datetime
                     , upd_tantosha_cd
                     , upd_tammatsu
                     , del_flg
                     )
                  VALUES (
                     rec_kojin.kojin_no 
                     , rec_kojin.setai_no
                     , rec_kojin.shimei_kbn
                     , rec_kojin.kannai_kbn
                     , rec_kojin.jumin_kbn
                     , rec_kojin.jumin_shubetsu_cd
                     , rec_kojin.jumin_jotai_cd
                     , rec_kojin.renkei_shimei
                     , rec_kojin.renkei_shimei_kana
                     , rec_kojin.renkei_tsushomei
                     , rec_kojin.renkei_tsushomei_kana
                     , rec_kojin.renkei_tsushomei_kana_flg
                     , rec_kojin.shimei
                     , rec_kojin.nihonjin_uji
                     , rec_kojin.nihonjin_mei
                     , rec_kojin.shimei_kana
                     , rec_kojin.shimei_kana_kosho_kakunin_kbn
                     , rec_kojin.shimei_kensaku_kana
                     , rec_kojin.nihonjin_uji_kana
                     , rec_kojin.nihonjin_mei_kana
                     , rec_kojin.shimei_gaikokujin_romaji
                     , rec_kojin.shimei_gaikokujin_kanji
                     , rec_kojin.old_uji
                     , rec_kojin.old_uji_kana
                     , rec_kojin.old_uji_kana_flg
                     , rec_kojin.yubin_no
                     , rec_kojin.jusho
                     , rec_kojin.jusho_cd
                     , rec_kojin.shikuchoson_cd
                     , rec_kojin.machiaza_cd
                     , rec_kojin.todofuken
                     , rec_kojin.shikugunchoson
                     , rec_kojin.machiaza
                     , rec_kojin.banchigohyoki
                     , rec_kojin.banchi_edaban
                     , rec_kojin.jusho_katagaki_cd
                     , rec_kojin.jusho_katagaki
                     , rec_kojin.jusho_katagaki_kana
                     , rec_kojin.kokumei_cd
                     , rec_kojin.kokumeito
                     , rec_kojin.kokugai_jusho
                     , rec_kojin.koseki_honseki
                     , rec_kojin.honseki_shikuchoson_cd
                     , rec_kojin.honseki_machiaza_cd
                     , rec_kojin.honseki_todofuken
                     , rec_kojin.honseki_shikugunchoson
                     , rec_kojin.honseki_machiaza
                     , rec_kojin.honseki_banchigohyoki
                     , rec_kojin.koseki_hittosha
                     , rec_kojin.koseki_hittosha_uji
                     , rec_kojin.koseki_hittosha_mei
                     , rec_kojin.birth_ymd
                     , rec_kojin.birth_fusho_flg
                     , rec_kojin.birth_fusho
                     , rec_kojin.zokugara_cd1
                     , rec_kojin.zokugara_cd2
                     , rec_kojin.zokugara_cd3
                     , rec_kojin.zokugara_cd4
                     , rec_kojin.zokugara
                     , rec_kojin.seibetsu_cd
                     , rec_kojin.seibetsu
                     , rec_kojin.chiku_cd
                     , rec_kojin.gyoseiku_cd
                     , rec_kojin.jichitai_cd
                     , rec_kojin.shori_ymd
                     , rec_kojin.ido_ymd
                     , rec_kojin.ido_ymd_fusho_flg
                     , rec_kojin.ido_ymd_fusho
                     , rec_kojin.ido_todoke_ymd
                     , rec_kojin.ido_jiyu_cd
                     , rec_kojin.shibo_ymd
                     , rec_kojin.jumin_ymd
                     , rec_kojin.jumin_ymd_fusho_flg
                     , rec_kojin.jumin_ymd_fusho
                     , rec_kojin.gaikokujin_jumin_ymd
                     , rec_kojin.gaikokujin_jumin_ymd_fusho_flg
                     , rec_kojin.gaikokujin_jumin_ymd_fusho
                     , rec_kojin.jutei_ymd
                     , rec_kojin.jutei_ymd_fusho_flg
                     , rec_kojin.jutei_ymd_fusho
                     , rec_kojin.tennyu_tsuchi_ymd
                     , rec_kojin.tenshutsu_todoke_ymd
                     , rec_kojin.tenshutsu_yotei_ymd
                     , rec_kojin.tenshutsu_ymd
                     , rec_kojin.tsushomei_kensaku_kana
                     , rec_kojin.old_uji_kensaku_kana
                     , rec_kojin.zairyu_card_no
                     , rec_kojin.zairyu_card_no_kbn_cd
                     , rec_kojin.kokuseki_cd
                     , rec_kojin.kokusekimeito
                     , rec_kojin.zairyu_kitei_kbn_cd
                     , rec_kojin.zairyu_shikaku_cd
                     , rec_kojin.zairyu_shikaku
                     , rec_kojin.zairyu_kikan_nen_cd
                     , rec_kojin.zairyu_kikan_tsuki_cd
                     , rec_kojin.zairyu_kikan_hi_cd
                     , rec_kojin.zairyu_manryo_ymd
                     , rec_kojin.kisai_juni
                     , rec_kojin.zairyu_todoke_kbn_cd
                     , rec_kojin.gyomu_cd
                     , rec_kojin.yobi_komoku1
                     , rec_kojin.yobi_komoku2
                     , rec_kojin.yobi_komoku3
                     , rec_kojin.yobi_komoku4
                     , rec_kojin.yobi_komoku5
                     , rec_kojin.doitsunin_kojin_no
                     , rec_kojin.doitsunin_chiku_cd
                     , rec_kojin.ins_datetime
                     , rec_kojin.upd_datetime
                     , rec_kojin.upd_tantosha_cd
                     , rec_kojin.upd_tammatsu
                     , rec_kojin.del_flg
                     );
                     ln_ins_count := ln_ins_count + 1;
                     lc_err_text := '';
                     lc_err_cd := lc_err_cd_normal;
                     ln_result_cd := ln_result_cd_add;

                  EXCEPTION WHEN OTHERS THEN
                     ln_err_count := ln_err_count + 1;
                     lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                     lc_err_cd := '9';
                     ln_result_cd := 9;
               END;
            ELSE
               BEGIN
                  UPDATE f_kojin
                  SET 
                     kojin_no = rec_kojin.kojin_no
                     ,setai_no = rec_kojin.setai_no
                     ,shimei_kbn = rec_kojin.shimei_kbn
                     ,setai_no = rec_kojin.setai_no
                     ,jumin_shubetsu_cd = rec_kojin.jumin_shubetsu_cd
                     ,jumin_jotai_cd = rec_kojin.jumin_jotai_cd
                     ,renkei_shimei = rec_kojin.renkei_shimei
                     ,renkei_shimei_kana = rec_kojin.renkei_shimei_kana
                     ,renkei_tsushomei = rec_kojin.renkei_tsushomei
                     ,renkei_tsushomei_kana = rec_kojin.renkei_tsushomei_kana
                     ,renkei_tsushomei_kana_flg = rec_kojin.renkei_tsushomei_kana_flg
                     ,shimei = rec_kojin.shimei
                     ,nihonjin_uji = rec_kojin.nihonjin_uji
                     ,nihonjin_mei = rec_kojin.nihonjin_mei
                     ,shimei_kana = rec_kojin.shimei_kana
                     ,shimei_kana_kosho_kakunin_kbn = rec_kojin.shimei_kana_kosho_kakunin_kbn
                     ,shimei_kensaku_kana = rec_kojin.shimei_kensaku_kana
                     ,nihonjin_uji_kana = rec_kojin.nihonjin_uji_kana
                     ,nihonjin_mei_kana = rec_kojin.nihonjin_mei_kana
                     ,shimei_gaikokujin_romaji = rec_kojin.shimei_gaikokujin_romaji
                     ,shimei_gaikokujin_kanji = rec_kojin.shimei_gaikokujin_kanji
                     ,old_uji = rec_kojin.old_uji
                     ,old_uji_kana = rec_kojin.old_uji_kana
                     ,old_uji_kana_flg = rec_kojin.old_uji_kana_flg
                     ,yubin_no = rec_kojin.yubin_no
                     ,jusho = rec_kojin.jusho
                     ,jusho_cd = rec_kojin.jusho_cd
                     ,shikuchoson_cd = rec_kojin.shikuchoson_cd
                     ,machiaza_cd = rec_kojin.machiaza_cd
                     ,todofuken = rec_kojin.todofuken
                     ,shikugunchoson = rec_kojin.shikugunchoson
                     ,machiaza = rec_kojin.machiaza
                     ,banchi_edaban = rec_kojin.banchi_edaban
                     ,jusho_katagaki_cd = rec_kojin.jusho_katagaki_cd
                     ,jusho_katagaki = rec_kojin.jusho_katagaki
                     ,jusho_katagaki_kana = rec_kojin.jusho_katagaki_kana
                     ,kokumei_cd = rec_kojin.kokumei_cd
                     ,kokumeito = rec_kojin.kokumeito
                     ,kokugai_jusho = rec_kojin.kokugai_jusho
                     ,honseki_shikuchoson_cd = rec_kojin.honseki_shikuchoson_cd
                     ,honseki_machiaza_cd = rec_kojin.honseki_machiaza_cd
                     ,honseki_todofuken = rec_kojin.honseki_todofuken
                     ,honseki_shikugunchoson = rec_kojin.honseki_shikugunchoson
                     ,honseki_machiaza = rec_kojin.honseki_machiaza
                     ,honseki_banchigohyoki = rec_kojin.honseki_banchigohyoki
                     ,koseki_hittosha = rec_kojin.koseki_hittosha
                     ,koseki_hittosha_uji = rec_kojin.koseki_hittosha_uji
                     ,koseki_hittosha_mei = rec_kojin.koseki_hittosha_mei
                     ,zokugara_cd2 = rec_kojin.zokugara_cd2
                     ,birth_fusho_flg = rec_kojin.birth_fusho_flg
                     ,birth_fusho = rec_kojin.birth_fusho
                     ,zokugara_cd1 = rec_kojin.zokugara_cd1
                     ,zokugara_cd2 = rec_kojin.zokugara_cd2
                     ,zokugara_cd3 = rec_kojin.zokugara_cd3
                     ,zokugara_cd4 = rec_kojin.zokugara_cd4
                     ,zokugara = rec_kojin.zokugara
                     ,seibetsu_cd = rec_kojin.seibetsu_cd
                     ,seibetsu = rec_kojin.seibetsu
                     ,chiku_cd = rec_kojin.chiku_cd
                     ,jichitai_cd = rec_kojin.jichitai_cd
                     ,shori_ymd = rec_kojin.shori_ymd
                     ,ido_ymd = rec_kojin.ido_ymd
                     ,ido_ymd_fusho_flg = rec_kojin.ido_ymd_fusho_flg
                     ,ido_ymd_fusho = rec_kojin.ido_ymd_fusho
                     ,ido_todoke_ymd = rec_kojin.ido_todoke_ymd
                     ,ido_jiyu_cd = rec_kojin.ido_jiyu_cd
                     ,shibo_ymd = rec_kojin.shibo_ymd
                     ,jumin_ymd = rec_kojin.jumin_ymd
                     ,jumin_ymd_fusho_flg = rec_kojin.jumin_ymd_fusho_flg
                     ,jumin_ymd_fusho = rec_kojin.jumin_ymd_fusho
                     ,gaikokujin_jumin_ymd = rec_kojin.gaikokujin_jumin_ymd
                     ,gaikokujin_jumin_ymd_fusho_flg = rec_kojin.gaikokujin_jumin_ymd_fusho_flg
                     ,gaikokujin_jumin_ymd_fusho = rec_kojin.gaikokujin_jumin_ymd_fusho
                     ,jutei_ymd_fusho_flg = rec_kojin.jutei_ymd_fusho_flg
                     ,jutei_ymd_fusho = rec_kojin.jutei_ymd_fusho
                     ,tennyu_tsuchi_ymd = rec_kojin.tennyu_tsuchi_ymd
                     ,tenshutsu_todoke_ymd = rec_kojin.tenshutsu_todoke_ymd
                     ,tenshutsu_yotei_ymd = rec_kojin.tenshutsu_yotei_ymd
                     ,tenshutsu_ymd = rec_kojin.tenshutsu_ymd
                     ,tsushomei_kensaku_kana = rec_kojin.tsushomei_kensaku_kana
                     ,old_uji_kensaku_kana = rec_kojin.old_uji_kensaku_kana
                     ,zairyu_card_no = rec_kojin.zairyu_card_no
                     ,zairyu_card_no_kbn_cd = rec_kojin.zairyu_card_no_kbn_cd
                     ,kokuseki_cd = rec_kojin.kokuseki_cd
                     ,kokusekimeito = rec_kojin.kokusekimeito
                     ,zairyu_kitei_kbn_cd = rec_kojin.zairyu_kitei_kbn_cd
                     ,zairyu_shikaku_cd = rec_kojin.zairyu_shikaku_cd
                     ,zairyu_kikan_nen_cd = rec_kojin.zairyu_kikan_nen_cd
                     ,zairyu_kikan_tsuki_cd = rec_kojin.zairyu_kikan_tsuki_cd
                     ,zairyu_kikan_hi_cd = rec_kojin.zairyu_kikan_hi_cd
                     ,zairyu_manryo_ymd = rec_kojin.zairyu_manryo_ymd
                     ,kisai_juni = rec_kojin.kisai_juni
                     ,zairyu_todoke_kbn_cd = rec_kojin.zairyu_todoke_kbn_cd
                     ,upd_datetime = rec_kojin.upd_datetime
                     ,upd_tantosha_cd = rec_kojin.upd_tantosha_cd
                     ,upd_tammatsu = rec_kojin.upd_tammatsu
                  WHERE kojin_no = rec_kojin.kojin_no;

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
         END IF;
         
         UPDATE i_r4g_atena 
            SET result_cd = ln_result_cd
               , error_cd = lc_err_cd
               , error_text = lc_err_text
         WHERE shikuchoson_cd = rec_main.shikuchoson_cd
               AND atena_no = rec_main.atena_no
               AND rireki_no = rec_main.rireki_no
               AND rireki_no_eda = rec_main.rireki_no_eda;

      END LOOP;
   CLOSE cur_main;

   -- proc_r4g_kojin_doitsunin_updを実行する
   CALL dlgrenkei.proc_r4g_kojin_doitsunin_upd( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );
   
   --proc_r4g_kojin_jushoを実行する
   CALL dlgrenkei.proc_r4g_kojin_jusho( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );

   -- proc_r4g_mynumbrを実行する
   CALL dlgrenkei.proc_r4g_mynumbr( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );
   
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
