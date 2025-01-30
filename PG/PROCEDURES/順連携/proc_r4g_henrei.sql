--------------------------------------------------------
--  DDL for Procedure proc_r4g_henrei
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_henrei( 
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
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : 2025/01/30 CRESS-INFO.Angelo     新規作成     036o012「返戻情報（統合収滞納）」の取込を行う             */
/**********************************************************************************************************************/

DECLARE
   rec_f_henrei_renkei              f_henrei_renkei%ROWTYPE;
   ln_para01                        numeric DEFAULT 0;
   lc_sql                           character varying;
   ln_del_flg                       numeric;
   lc_tantosha                      character varying;
   
   ln_shori_count                   numeric DEFAULT 0;
   ln_ins_count                     numeric DEFAULT 0;
   ln_upd_count                     numeric DEFAULT 0;
   ln_del_count                     numeric DEFAULT 0;
   ln_err_count                     numeric DEFAULT 0;
   lc_err_text                      character varying(100);
   ln_result_cd                     numeric DEFAULT 0;
   lc_err_cd                        character varying;

   ln_result_cd_add                 numeric DEFAULT 1; -- 追加
   ln_result_cd_upd                 numeric DEFAULT 2; -- 更新
   ln_result_cd_err                 numeric DEFAULT 9; -- エラー

   lc_err_cd_normal                 character varying = '0'; -- 通常
   lc_err_cd_err                    character varying = '9'; -- エラー

   rec_log                          dlgrenkei.f_renkei_log%ROWTYPE;

   cur_parameter CURSOR FOR
      SELECT * FROM dlgrenkei.f_renkei_parameter
      WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                     dlgrenkei.f_renkei_parameter%ROWTYPE;
   
   cur_main CURSOR FOR
      SELECT * FROM dlgrenkei.i_r4g_henrei
      WHERE saishin_flg = '1' 
      AND result_cd < 8;
   
   rec_main                          dlgrenkei.i_r4g_henrei%ROWTYPE;

   cur_lock CURSOR FOR
      SELECT * FROM f_henrei_renkei
      WHERE kojin_no = rec_f_henrei_renkei.kojin_no
         AND gyomu_cd = rec_f_henrei_renkei.gyomu_cd
         AND zeimoku_cd = rec_f_henrei_renkei.zeimoku_cd
         AND henrei_shubetsu_cd = rec_f_henrei_renkei.henrei_shubetsu_cd
         AND rireki_no = rec_f_henrei_renkei.rireki_no
         AND fuka_nendo = rec_f_henrei_renkei.fuka_nendo
         AND soto_nendo = rec_f_henrei_renkei.soto_nendo
         AND kibetsu_cd = rec_f_henrei_renkei.kibetsu_cd
         AND tsuchisho_no = ref_f_henrei_renkei.tsuchisho_no;
         
   rec_lock              f_henrei_renkei%ROWTYPE;
      
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
        lc_sql := 'TRUNCATE TABLE dlgmain.f_henrei_renkei';
        EXECUTE lc_sql;
      EXCEPTION WHEN OTHERS THEN
        io_c_err_code    := SQLSTATE;
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
        lc_err_cd                      := lc_err_cd_normal;
        ln_result_cd                   := 0;
        lc_err_text                    := NULL;
        rec_lock                       := NULL;

        -- 個人番号
        rec_f_henrei_renkei.kojin_no := rec_main.atena_no;
        -- 業務コード
        rec_f_henrei_renkei.gyomu_cd := rec_main.gyomu_id;
        -- 税目コード
        rec_f_henrei_renkei.zeimoku_cd := get_r4g_code_conv(0, 3, rec_f_henrei_renkei.zeimoku_cd, NULL)::numeric;
        -- 返戻物種別コード
        rec_f_henrei_renkei.henrei_shubetsu_cd := rec_main.henrei_syubetsu;
        -- 履歴番号
        rec_f_henrei_renkei.rireki_no := get_str_to_num(rec_main.rireki_no);
        -- 賦課年度
        rec_f_henrei_renkei.fuka_nendo := get_str_to_num(rec_main.fuka_nendo);
        -- 相当年度
        rec_f_henrei_renkei.soto_nendo := get_str_to_num(rec_main.soto_nendo);
        -- 期別
        rec_f_henrei_renkei.kibetsu_cd := get_str_to_num(rec_main.kibetsu_cd);
        -- 通知書番号
        rec_f_henrei_renkei.tsuchisho_no := rec_main.tsuchisho_no;
        -- 被保険者番号
        rec_f_henrei_renkei.hihokensha_no := rec_main.hihokensha_no;
        -- 児童宛名番号
        rec_f_henrei_renkei.jido_kojin_no := rec_main.jido_atena_no;
        -- 返戻調査番号
        rec_f_henrei_renkei.henrei_chosa_no := get_str_to_num(rec_main.henrei_chosa_no);
        -- 調査・返戻処理段階の区分
        rec_f_henrei_renkei.chosa_henrei_kbn := get_str_to_num(rec_main.chosa_henrei_kbn);
        -- 文書番号
        rec_f_henrei_renkei.bunsho_no := rec_main.bunsho_no;
        -- 帳票名
        rec_f_henrei_renkei.list_name := rec_main.list_name;
        -- 返戻登録日
        rec_f_henrei_renkei.henrei_toroku_ymd := get_ymd_str_to_num(rec_main.henrei_toroku_ymd);
        -- 返戻日
        rec_f_henrei_renkei.henrei_ymd := get_ymd_str_to_num(rec_main.henrei_ymd);
        -- 返戻事由コード
        rec_f_henrei_renkei.henrei_jiyu_cd := rec_main.henrei_jiyu;
        -- 再発送日
        rec_f_henrei_renkei.saihasso_ymd := get_ymd_str_to_num(rec_main.rec_main.re_hasso);
        -- 公示日
        rec_f_henrei_renkei.koji_ymd := get_ymd_str_to_num(rec_main.rec_main.kouji_ymd);
        -- 公示送達日
        rec_f_henrei_renkei.koji_sotatsu_ymd := get_ymd_str_to_num(rec_main.rec_main.koji_sotatsu_ymd);
        -- 納期限（変更前）
        rec_f_henrei_renkei.henkomae_noki_ymd := get_ymd_str_to_num(rec_main.rec_main.noki_henko_mae);
        -- 納期限（変更後）
        rec_f_henrei_renkei.henkogo_noki_ymd := get_ymd_str_to_num(rec_main.rec_main.noki_henko_ato);
        -- 返戻担当者コード
        rec_f_henrei_renkei.tantosha_cd_henrei := rec_main.tanto_id;
        -- 返戻担当者
        rec_f_henrei_renkei.tantosha_henrei := lc_tantosha;
        -- データ作成日時
        rec_f_henrei_renkei.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
        -- データ更新日時
        rec_f_henrei_renkei.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
        -- 更新担当者コード
        rec_f_henrei_renkei.upd_tantosha_cd := rec_main.sosasha_cd;
        -- 更新端末名称
        rec_f_henrei_renkei.upd_tammatsu := 'SERVER';
        -- 削除フラグ
        rec_f_henrei_renkei.del_flg := get_str_to_num(rec_main.del_flg);

        OPEN cur_lock;
            FETCH cur_lock INTO rec_lock;
        CLOSE cur_lock;

        IF rec_lock IS NULL THEN
            BEGIN
                INSERT INTO f_henrei_renkei (
                     kojin_no	
                    , gyomu_cd
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
                ) VALUES (
                     rec_f_henrei_renkei.kojin_no	
                    , rec_f_henrei_renkei.gyomu_cd
                    , rec_f_henrei_renkei.zeimoku_cd
                    , rec_f_henrei_renkei.henrei_shubetsu_cd
                    , rec_f_henrei_renkei.rireki_no
                    , rec_f_henrei_renkei.fuka_nendo
                    , rec_f_henrei_renkei.soto_nendo
                    , rec_f_henrei_renkei.kibetsu_cd
                    , rec_f_henrei_renkei.tsuchisho_no
                    , rec_f_henrei_renkei.hihokensha_no
                    , rec_f_henrei_renkei.jido_kojin_no
                    , rec_f_henrei_renkei.henrei_chosa_no
                    , rec_f_henrei_renkei.chosa_henrei_kbn
                    , rec_f_henrei_renkei.bunsho_no
                    , rec_f_henrei_renkei.list_name
                    , rec_f_henrei_renkei.henrei_toroku_ymd
                    , rec_f_henrei_renkei.henrei_ymd
                    , rec_f_henrei_renkei.henrei_jiyu_cd
                    , rec_f_henrei_renkei.saihasso_ymd
                    , rec_f_henrei_renkei.koji_ymd
                    , rec_f_henrei_renkei.koji_sotatsu_ymd
                    , rec_f_henrei_renkei.henkomae_noki_ymd
                    , rec_f_henrei_renkei.henkogo_noki_ymd
                    , rec_f_henrei_renkei.tantosha_cd_henrei
                    , rec_f_henrei_renkei.tantosha_henrei
                    , rec_f_henrei_renkei.ins_datetime
                    , rec_f_henrei_renkei.upd_datetime
                    , rec_f_henrei_renkei.upd_tantosha_cd
                    , rec_f_henrei_renkei.upd_tammatsu
                    , rec_f_henrei_renkei.del_flg
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
                UPDATE f_henrei_renkei
                SET  hihokensha_no = rec_f_henrei_renkei.hihokensha_no
                   , jido_kojin_no = rec_f_henrei_renkei.jido_kojin_no
                   , henrei_chosa_no = rec_f_henrei_renkei.henrei_chosa_no
                   , chosa_henrei_kbn = rec_f_henrei_renkei.chosa_henrei_kbn
                   , bunsho_no = rec_f_henrei_renkei.bunsho_no
                   , list_name = rec_f_henrei_renkei.list_name
                   , henrei_toroku_ymd = rec_f_henrei_renkei.henrei_toroku_ymd
                   , henrei_ymd = rec_f_henrei_renkei.henrei_ymd
                   , henrei_jiyu_cd = rec_f_henrei_renkei.henrei_jiyu_cd
                   , saihasso_ymd = rec_f_henrei_renkei.saihasso_ymd
                   , koji_ymd = rec_f_henrei_renkei.koji_ymd
                   , koji_sotatsu_ymd = rec_f_henrei_renkei.koji_sotatsu_ymd
                   , henkomae_noki_ymd = rec_f_henrei_renkei.henkomae_noki_ymd
                   , henkogo_noki_ymd = rec_f_henrei_renkei.henkogo_noki_ymd
                   , tantosha_cd_henrei = rec_f_henrei_renkei.tantosha_cd_henrei
                   , tantosha_henrei = rec_f_henrei_renkei.tantosha_henrei
                   , ins_datetime = rec_f_henrei_renkei.ins_datetime
                   , upd_datetime = rec_f_henrei_renkei.upd_datetime
                   , upd_tantosha_cd = rec_f_henrei_renkei.upd_tantosha_cd
                   , upd_tammatsu = rec_f_henrei_renkei.upd_tammatsu
                   , del_flg = rec_f_henrei_renkei.del_flg
                WHERE kojin_no = rec_main.atena_no
                   AND gyomu_cd = rec_main.gyomu_id
                   AND zeimoku_cd = rec_f_henrei_renkei.zeimoku_cd
                   AND henrei_shubetsu_cd = rec_main.henrei_syubetsu
                   AND rireki_no = rec_f_henrei_renkei.rireki_no
                   AND fuka_nendo = rec_f_henrei_renkei.fuka_nendo
                   AND soto_nendo = rec_f_henrei_renkei.soto_nendo
                   AND kibetsu_cd = rec_f_henrei_renkei.kibetsu_cd
                   AND tsuchisho_no = rec_main.tsuchisho_no;

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
        IF get_str_to_num(rec_main.del_flg) = 1 THEN
            ln_del_count := ln_del_count + 1;
            ln_result_cd := ln_result_cd_del;
        END IF;

        BEGIN 
            UPDATE dlgrenkei.i_r4g_henrei
            SET result_cd   = ln_result_cd
            , error_cd      = lc_err_cd
            , error_text    = lc_err_text
            , seq_no_renkei = in_n_renkei_seq
            , shori_ymd     = in_n_shori_ymd
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
   CALL dlgrenkei.proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

EXCEPTION
   WHEN OTHERS THEN
   io_c_err_code := SQLSTATE;
   io_c_err_text := SQLERRM;
   RETURN;
END;
$$;