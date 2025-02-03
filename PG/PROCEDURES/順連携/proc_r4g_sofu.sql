--------------------------------------------------------
--  DDL for Procedure proc_kojin_sofu_chofuku_upd
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_sofu( 
    in_n_renkei_data_cd IN numeric, 
    in_n_renkei_seq IN numeric, 
    in_n_shori_ymd IN numeric, 
    io_c_err_code INOUT character varying, 
    io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 送付先・連絡先情報（統合収滞納）                                                                        */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : 新規作成                                                                                                */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : 2025/01/31 CRESS-INFO.Angelo     新規作成     036o014「送付先・連絡先情報（統合収滞納）」の取込を行う   */
/**********************************************************************************************************************/

DECLARE
    rec_f_sofu                       f_sofu%ROWTYPE;
    rec_remban                       RECORD;
    ln_commit_count                  numeric DEFAULT 10000;
    ln_para01                        numeric DEFAULT 0;
    ln_para06                        numeric DEFAULT 0;
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
    ln_yusen_flg                     numeric DEFAULT 0;

    lc_err_cd_normal                 character varying = '0'; -- 通常
    lc_err_cd_err                    character varying = '9'; -- エラー

    rec_log                          dlgrenkei.f_renkei_log%ROWTYPE;

    cur_parameter CURSOR FOR
    SELECT * FROM dlgrenkei.f_renkei_parameter
    WHERE renkei_data_cd = in_n_renkei_data_cd;

    rec_parameter                     dlgrenkei.f_renkei_parameter%ROWTYPE;

    cur_main CURSOR FOR
    WITH RankedData AS ( 
            SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY shikuchoson_cd, atena_no, gyomu_id 
                ORDER BY sofu_rireki_no DESC
            ) AS rn
    FROM dlgrenkei.i_r4g_sofu_renrakusaki
    WHERE saishin_flg = '1'
        AND zeimoku_cd = '00'
        AND result_cd < 8
        AND (COALESCE(yubin_no, '') <> '' OR COALESCE(jusho, '') <> '')
    )
    SELECT *
    FROM RankedData
    WHERE rn = 1;
        
    rec_main                            dlgrenkei.i_r4g_sofu_renrakusaki%ROWTYPE;

    cur_lock CURSOR FOR
    SELECT * FROM f_sofu
    WHERE busho_cd = rec_busho.busho_cd
        AND gyomu_cd = rec_f_sofu.gyomu_cd
        AND kojin_no = rec_f_sofu.zeimoku_cd
        AND gyomu_cd = rec_f_sofu.henrei_shubetsu_cd
        AND remban = rec_f_sofu.rireki_no;
         
    rec_lock              f_sofu%ROWTYPE;

    cur_remban CURSOR (p_busho_cd character varying, p_kojin_no character varying) IS
    SELECT COALESCE(SUM(CASE WHEN del_flg = 0 THEN 1 ELSE 0 END), 0) AS yuko_count,
        COALESCE(SUM(renkei_flg), 0) AS renkei_count,
        COALESCE(CASE WHEN SUM(renkei_flg) > 0 THEN MAX(renkei_flg * remban) ELSE MAX(remban) + 1 END, 1) AS renkei_remban,
        COALESCE(MIN(CASE 
                WHEN del_flg = 0 AND yusen_flg = 1 AND renkei_flg = 0 THEN 1
                WHEN del_flg = 0 AND yusen_flg = 1 AND renkei_flg = 1 THEN 2
                ELSE 9
            END), 9) AS yusen_kbn
    FROM f_sofu
    WHERE busho_cd = p_busho_cd
    AND kojin_no = p_kojin_no;

    cur_busho CURSOR FOR
    SELECT *
    FROM t_busho
    WHERE del_flg = 0
    ORDER BY busho_cd;

    rec_busho                           t_busho%ROWTYPE;

BEGIN

    rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

    OPEN cur_parameter;
        LOOP
            FETCH cur_parameter INTO rec_parameter;
            EXIT WHEN NOT FOUND;

            IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
            IF rec_parameter.parameter_no = 6 THEN ln_para06 := rec_parameter.parameter_value; END IF;
        END LOOP;
    CLOSE cur_parameter;

    --連携先データの削除
    IF ln_para01 = 1 THEN
        BEGIN
            lc_sql := 'TRUNCATE TABLE dlgmain.f_sofu';
            EXECUTE lc_sql;
            EXCEPTION
                WHEN OTHERS THEN
                io_c_err_code := SQLSTATE;
                io_c_err_text := SQLERRM;
                RETURN;
        END; 
    END IF;

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

        OPEN cur_busho;
            LOOP

            FETCH cur_busho INTO rec_busho;
            EXIT WHEN NOT FOUND;
                ln_shori_count := ln_shori_count + 1;
            
                -- 部署コード
                rec_f_sofu.busho_cd := rec_busho.busho_cd;
                -- 個人番号
                rec_f_sofu.kojin_no := rec_main.atena_no;
                -- 業務コード
                rec_f_sofu.gyomu_cd := get_str_to_num(rec_main.gyomu_id);

                OPEN cur_remban(rec_f_sofu.busho_cd, rec_f_sofu.kojin_no);
                    FETCH cur_remban INTO rec_remban;
                CLOSE cur_remban;

                IF ln_para06 = 1 THEN
                    ln_yusen_flg := 1;
                ELSIF rec_remban.yuko_count = 0 OR rec_remban.yusen_kbn = 9 THEN
                    ln_yusen_flg := 1;
                ELSE
                    BEGIN
                        SELECT yusen_flg
                        INTO ln_yusen_flg
                        FROM f_sofu
                        WHERE busho_cd = rec_f_sofu.busho_cd
                        AND kojin_no = rec_f_sofu.kojin_no
                        AND remban = rec_remban.renkei_remban
                        AND del_flg = 0;
                    EXCEPTION
                        WHEN OTHERS THEN
                        ln_yusen_flg := 0;
                    END;
                END IF;
                -- 連番
                rec_f_sofu.remban := rec_remban.renkei_remban;

                IF ln_para06 = 1 OR rec_f_sofu.yusen_flg = 1 THEN
                    UPDATE f_sofu
                    SET yusen_flg = 0
                    WHERE busho_cd = rec_f_sofu.busho_cd
                    AND kojin_no = rec_f_sofu.kojin_no;
                END IF;
                -- 送付先参照個人番号
                rec_f_sofu.sofu_sansho_kojin_no := NULL;
                -- 優先フラグ
                rec_f_sofu.yusen_flg := ln_yusen_flg;
                -- 調査年月日
                rec_f_sofu.chosa_ymd := NULL;
                -- 送付先地区コード
                rec_f_sofu.sofa_chiku_cd := get_chiku_cd(0, rec_f_sofu.kojin_no, rec_main.jusho_shikuchoson_cd);
                -- 送付先住所コード
                rec_f_sofu.sofu_jusho_cd := concat(LPAD(jusho_shikuchoson_cd, 6, '0'), LPAD(jusho_machi_cd, 7, '0'));
                -- 送付先郵便番号
                rec_f_sofu.sofu_yubin_no := rec_main.yubin_no;
                -- 送付先住所
                rec_f_sofu.sofu_jusho := CASE WHEN (rec_main.jusho = '' OR rec_main.jusho IS NULL)
                                            THEN CONCAT(get_trimmed_space(rec_main.ken), get_trimmed_space(rec_main.shikuchoson), get_trimmed_space(rec_main.jusho_machi_cd), get_trimmed_space(rec_main.banchi))
                                            ELSE rec_main.jusho
                                            END;
                -- rec_f_sofu.sofu_jusho_mojisu
                -- rec_f_sofu.sofu_jusho_gaiji_flg
                -- 送付先住所方書
                rec_f_sofu.sofu_jusho_katagaki := get_trimmed_space(rec_main.jusho_katagaki);
                -- rec_f_sofu.sofu_jusho_katagaki_mojisu
                -- rec_f_sofu.sofu_jusho_katagaki_gaiji_flg
                -- 送付先入力区分
                rec_f_sofu.nyuryoku_kbn := CASE WHEN (rec_main.jusho = '' OR rec_main.jusho IS NULL) THEN 1 ELSE 4 END;
                -- 送付先市区町村コード
                rec_f_sofu.shikuchoson_cd := rec_main.jusho_shikuchoson_cd;
                -- 送付先町字コード
                rec_f_sofu.machiaza_cd := rec_main.jusho_machi_cd;
                -- 送付先都道府県
                rec_f_sofu.todofuken := get_trimmed_space(rec_main.ken);
                -- 送付先市区郡町村名
                rec_f_sofu.shikugunchoson := get_trimmed_space(rec_main.shikuchoson);
                -- 送付先町字
                rec_f_sofu.machiaza := get_trimmed_space(rec_main.machi);
                -- 送付先番地号表記
                rec_f_sofu.banchigohyoki := get_trimmed_space(rec_main.banchi);
                -- 送付先国名コード
                rec_f_sofu.kokumei_cd := NULL;
                -- 送付先国名等
                rec_f_sofu.kokumeito := NULL;
                -- 送付先国外住所
                rec_f_sofu.kokugai_jusho := NULL;
                -- 送付先氏名
                rec_f_sofu.sofu_shimei := get_trimmed_space(rec_main.simei_meisho);
                -- rec_f_sofu.sofu_shimei_mojisu
                -- rec_f_sofu.sofu_shimei_gaiji_flg
                -- 送付先氏名カナ
                rec_f_sofu.sofu_shimei_kana := get_trimmed_space(rec_main.simei_meisho_katakana);
                -- 送付先区分
                rec_f_sofu.sofu_kbn := rec_main.sofu_kbn;
                -- 送付先を設定する理由
                rec_f_sofu.sofu_setti_riyu := rec_main.sofu_setti_riyu;
                -- 連絡先区分
                rec_f_sofu.renrakusaki_kbn := get_str_to_num(rec_main.renrakusaki_kbn);
                -- 電話番号
                rec_f_sofu.denwa_no := rec_main.tel_no;
                -- 開始年月日
                rec_f_sofu.sofu_kaishi_ymd := get_ymd_str_to_num(rec_main.toroku_ymd);
                -- 終了年月日
                rec_f_sofu.sofu_shuryo_ymd := get+ymd_str_to_num(rec_main.riyou_haishi_ymd);
                -- 送付備考
                rec_f_sofu.biko_sofu := rec_main.memo;
                -- 連携フラグ
                rec_f_sofu.renkei_flg := 1;
                -- 送付先履歴番号
                rec_f_sofu.sofurireki_no := rec_main.sofu_rireki_no;
                -- データ作成日時
                rec_f_sofu.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
                -- データ更新日時
                rec_f_sofu.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
                -- 更新担当者コード
                rec_f_sofu.upd_tantosha_cd := rec_main.sosasha_cd;
                -- 更新端末名称
                rec_f_sofu.upd_tammatsu := 'SERVER';
                -- 削除フラグ
                rec_f_sofu.del_flg :=  get_str_to_num(rec_main.del_flg);

                OPEN cur_lock;
                    FETCH cur_lock INTO rec_lock;
                CLOSE cur_lock;

                IF rec_lock IS NULL THEN
                    BEGIN
                        INSERT INTO f_sofu (
                            busho_cd
                            , kojin_no
                            , gyomu_cd
                            , remban
                            , sofu_sansho_kojin_no
                            , yusen_flg
                            , chosa_ymd
                            , sofu_chiku_cd
                            , sofu_jusho_cd
                            , sofu_yubin_no
                            , sofu_jusho
                            -- , sofu_jusho_mojisu
                            -- , sofu_jusho_gaiji_flg
                            , sofu_jusho_katagaki
                            -- , sofu_jusho_katagaki_mojisu
                            -- , sofu_jusho_katagaki_gaiji_flg
                            , nyuryoku_kbn
                            , shikuchoson_cd
                            , machiaza_cd
                            , todofuken
                            , shikugunchoson
                            , machiaza
                            , banchigohyoki
                            , kokumei_cd
                            , kokumeito
                            , kokugai_jusho
                            , sofu_shimei
                            -- , sofu_shimei_mojisu
                            -- , sofu_shimei_gaiji_flg
                            , sofu_shimei_kana
                            , sofu_kbn
                            , sofu_setti_riyu
                            , renrakusaki_kbn
                            , denwa_no
                            , sofu_kaishi_ymd
                            , sofu_shuryo_ymd
                            , biko_sofu
                            , renkei_flg
                            , sofurireki_no
                            , ins_datetime
                            , upd_datetime
                            , upd_tantosha_cd
                            , upd_tammatsu
                            , del_flg
                        ) VALUES (
                            rec_f_sofu.busho_cd
                            , rec_f_sofu.kojin_no
                            , rec_f_sofu.gyomu_cd
                            , rec_f_sofu.remban
                            , rec_f_sofu.sofu_sansho_kojin_no
                            , rec_f_sofu.yusen_flg
                            , rec_f_sofu.chosa_ymd
                            , rec_f_sofu.sofu_chiku_cd
                            , rec_f_sofu.sofu_jusho_cd
                            , rec_f_sofu.sofu_yubin_no
                            , rec_f_sofu.sofu_jusho
                            -- , rec_f_sofu.sofu_jusho_mojisu
                            -- , rec_f_sofu.sofu_jusho_gaiji_flg
                            , rec_f_sofu.sofu_jusho_katagaki
                            -- , rec_f_sofu.sofu_jusho_katagaki_mojisu
                            -- , rec_f_sofu.sofu_jusho_katagaki_gaiji_flg
                            , rec_f_sofu.nyuryoku_kbn
                            , rec_f_sofu.shikuchoson_cd
                            , rec_f_sofu.machiaza_cd
                            , rec_f_sofu.todofuken
                            , rec_f_sofu.shikugunchoson
                            , rec_f_sofu.machiaza
                            , rec_f_sofu.banchigohyoki
                            , rec_f_sofu.kokumei_cd
                            , rec_f_sofu.kokumeito
                            , rec_f_sofu.kokugai_jusho
                            , rec_f_sofu.sofu_shimei
                            -- , rec_f_sofu.sofu_shimei_mojisu
                            -- , rec_f_sofu.sofu_shimei_gaiji_flg
                            , rec_f_sofu.sofu_shimei_kana
                            , rec_f_sofu.sofu_kbn
                            , rec_f_sofu.sofu_setti_riyu
                            , rec_f_sofu.renrakusaki_kbn
                            , rec_f_sofu.denwa_no
                            , rec_f_sofu.sofu_kaishi_ymd
                            , rec_f_sofu.sofu_shuryo_ymd
                            , rec_f_sofu.biko_sofu
                            , rec_f_sofu.renkei_flg
                            , rec_f_sofu.sofurireki_no
                            , rec_f_sofu.ins_datetime
                            , rec_f_sofu.upd_datetime
                            , rec_f_sofu.upd_tantosha_cd
                            , rec_f_sofu.upd_tammatsu
                            , rec_f_sofu.del_flg
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
                        UPDATE f_sofu
                        SET yusen_flg = rec_f_sofu.yusen_flg
                            , sofu_chiku_cd = rec_f_sofu.sofu_chiku_cd
                            , sofu_yubin_no = rec_f_sofu.sofu_yubin_no
                            , sofu_jusho = rec_f_sofu.sofu_jusho
                            , sofu_jusho_katagaki = rec_f_sofu.sofu_jusho_katagaki
                            , nyuryoku_kbn = rec_f_sofu.nyuryoku_kbn
                            , shikuchoson_cd = rec_f_sofu.shikuchoson_cd
                            , machiaza_cd = rec_f_sofu.machiaza_cd
                            , todofuken = rec_f_sofu.todofuken
                            , shikugunchoson = rec_f_sofu.shikugunchoson
                            , machiaza = rec_f_sofu.machiaza
                            , banchigohyoki = rec_f_sofu.banchigohyoki
                            , sofu_shimei = rec_f_sofu.sofu_shimei
                            , sofu_shimei_kana = rec_f_sofu.sofu_shimei_kana
                            , sofu_kbn = rec_f_sofu.sofu_kbn
                            , sofu_setti_riyu = rec_f_sofu.sofu_setti_riyu
                            , renrakusaki_kbn = rec_f_sofu.renrakusaki_kbn
                            , denwa_no = rec_f_sofu.denwa_no
                            , sofu_kaishi_ymd = rec_f_sofu.sofu_kaishi_ymd
                            , sofu_shuryo_ymd = rec_f_sofu.sofu_shuryo_ymd
                            , biko_sofu = rec_f_sofu.biko_sofu
                            , sofurireki_no = rec_f_sofu.sofurireki_no
                            , upd_datetime = rec_f_sofu.upd_datetime
                            , upd_tantosha_cd = rec_f_sofu.upd_tantosha_cd
                            , upd_tammatsu = rec_f_sofu.upd_tammatsu
                            , del_flg = rec_f_sofu.del_flg
                        WHERE busho_cd = rec_f_sofu.busho_cd
                        AND kojin_no = rec_f_sofu.kojin_no
                        AND gyomu_cd = rec_f_sofu.gyomu_cd
                        AND remban = rec_f_sofu.remban;

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
                    -- 中間テーブル更新
                    UPDATE dlgrenkei.i_r4g_sofu_renrakusaki
                        SET result_cd     = ln_result_cd
                        , error_cd      = ln_err_cd
                        , error_text    = lc_err_text
                        , seq_no_renkei = in_n_renkei_seq
                        , shori_ymd     = in_n_shori_ymd
                        WHERE shikuchoson_cd = rec_main.shikuchoson_cd
                            AND atena_no = rec_main.atena_no
                            AND gyomu_id = rec_main.gyomu_id
                            AND zeimoku_cd = rec_main.zeimoku_cd
                            AND keiji_kanri_no = rec_main.keiji_kanri_no
                            AND sofu_rireki_no = rec_main.sofu_rireki_no;
                EXCEPTION
                    WHEN OTHERS THEN NULL;
                END;

            END LOOP;

            IF MOD( ln_shori_count, ln_commit_count) = 0 THEN
               COMMIT;
            END IF;
        CLOSE cur_busho;

       END LOOP;
    CLOSE cur_main;

    CALL proc_kojin_sofu_chofuku_upd(io_c_err_code, io_c_err_text);
    CALL proc_r4g_sofu_zeimoku(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);
    CALL proc_r4g_denwa(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

    rec_log.seq_no_renkei := in_n_renkei_seq;
    rec_log.proc_shuryo_datetime := CURRENT_TIMESTAMP;
    rec_log.proc_shori_count := ln_shori_count;
    rec_log.proc_ins_count := ln_ins_count;
    rec_log.proc_upd_count := ln_upd_count;
    rec_log.proc_del_count := ln_del_count;
    rec_log.proc_err_count := ln_err_count;

    -- データ連携ログ更新
    CALL dlgrenkei.proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

EXCEPTION
    WHEN OTHERS THEN
    io_c_err_code := SQLSTATE;
    io_c_err_text := SQLERRM;
    RETURN;
END;
$$;
