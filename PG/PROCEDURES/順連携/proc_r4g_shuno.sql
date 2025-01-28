--------------------------------------------------------
--  DDL for Procedure proc_r4g_shuno
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_shuno(
    IN in_n_renkei_data_cd numeric,
    IN in_n_renkei_seq numeric,
    IN in_n_shori_ymd numeric,
    INOUT io_c_err_code character varying,
    INOUT io_c_err_text character varying)
LANGUAGE plpgsql
AS $$
/**********************************************************************************************************************/
/* 処理概要 : f_収納（f_shuno）の追加／更新／削除を実施する                                                               */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                    */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                   */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                 */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                       */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                         */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/27  CRESS-INFO.Drexler     新規作成     036o006「収納履歴情報（統合収滞納）」の取込を行う            */
/**********************************************************************************************************************/
DECLARE
    rec_f_shuno f_shuno % ROWTYPE;
    rec_log dlgrenkei.f_renkei_log % ROWTYPE;

    ln_shori_count                 numeric DEFAULT 0;             -- 処理件数用変数
    ln_ins_count                   numeric DEFAULT 0;             -- 追加件数用変数
    ln_upd_count                   numeric DEFAULT 0;             -- 更新件数用変数
    ln_del_count                   numeric DEFAULT 0;             -- 削除件数用変数
    ln_err_count                   numeric DEFAULT 0;             -- エラー件数用変数
    lc_err_cd                      character varying;             -- エラーコード用変数
    lc_err_text                    character varying(100):='';    -- エラー内容用変数
    ln_result_cd                   numeric DEFAULT 0;             -- 結果区分更新用変数

    lc_kibetsu                     character varying;
    ln_para01                      numeric DEFAULT 0;
    ln_para02                      numeric DEFAULT 0;
    ln_para05                      numeric DEFAULT 0;
    ln_para07                      numeric DEFAULT 0;
    lc_sql                         character varying;             -- SQL文用変数
    ln_shuno_count                 numeric DEFAULT 0;
    ln_err_kbn                     numeric DEFAULT 0;-- 対象データ 0:エラーなし 1:エラーあり
    ln_del_count_tmp               numeric DEFAULT 0;

-- メインカーソル
cur_main CURSOR FOR
SELECT *
FROM dlgrenkei.i_r4g_shuno_rireki
WHERE result_cd < 8;

rec_main dlgrenkei.i_r4g_shuno_rireki%ROWTYPE;

-- パラメータ取得カーソル
cur_parameter CURSOR FOR
SELECT *
FROM dlgrenkei.f_renkei_parameter
WHERE renkei_data_cd = in_n_renkei_data_cd;

rec_parameter dlgrenkei.f_renkei_parameter % ROWTYPE;

-- 行ロック用カーソル
cur_lock CURSOR FOR
SELECT *
FROM f_shuno
WHERE kibetsu_key = rec_f_shuno.kibetsu_key
    AND shuno_keshikomi_key = rec_f_shuno.shuno_keshikomi_key;

rec_lock f_shuno%ROWTYPE;

BEGIN

    rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

    -- １．パラメータ情報の取得
    OPEN cur_parameter;
    LOOP

        FETCH cur_parameter
        INTO rec_parameter;

        EXIT WHEN NOT FOUND;

        IF rec_parameter.parameter_no = 1 THEN
            ln_para01 := rec_parameter.parameter_no;
        END IF;
        IF rec_parameter.parameter_no = 2 THEN
            ln_para02 := rec_parameter.parameter_value;
        END IF;
        IF rec_parameter.parameter_no = 5 THEN
            ln_para05 := rec_parameter.parameter_value;
        END IF;
        IF rec_parameter.parameter_no = 7 THEN
            ln_para07 := rec_parameter.parameter_value;
        END IF;
    END LOOP;
    CLOSE cur_parameter;

    -- ２．連携先データの削除
    IF ln_para01 = 1 THEN
        BEGIN
            SELECT COUNT(*)
            INTO ln_del_count
            FROM f_shuno;

            lc_sql := 'TRUNCATE TABLE dlgmain.f_shuno';

            EXECUTE lc_sql;

            EXCEPTION
                WHEN OTHERS THEN
                io_c_err_code := SQLSTATE;
                io_c_err_text := SQLERRM;
            RETURN;
        END;
    END IF;
    -- ３．中間テーブルデータのエラーチェック
    CALL proc_error_check(in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text);

    IF io_c_err_code <> '0' THEN
        RETURN;
    END IF;

    IF ln_para02 = 1 THEN
        CALL proc_shuno_drop_index();
    END IF;

    ln_shori_count := 0;

    -- ５．連携データの作成・更新
    OPEN cur_main;
    LOOP
        FETCH cur_main INTO rec_main;
        EXIT WHEN NOT FOUND;

        ln_shori_count                             := ln_shori_count + 1;
        lc_err_cd                                  := '0';
        ln_result_cd                               := 0;
        lc_err_text                                := NULL;
        rec_f_shuno                                := NULL;
        rec_lock                                   := NULL;
        ln_err_kbn                                 := 0;

        CALL proc_shuno_fill_key_columns(rec_main, rec_f_shuno);

        SELECT kibetsu INTO lc_kibetsu
        FROM t_kibetsu
        WHERE fuka_nendo = rec_f_shuno.fuka_nendo
            AND nendo_kbn = rec_f_shuno.nendo_kbn
            AND kankatsu_cd = rec_f_shuno.kankatsu_cd
            AND zeimoku_cd = rec_f_shuno.zeimoku_cd
            AND kibetsu_cd = rec_f_shuno.kibetsu_cd;
        
        -- 期別KEYの取得
        rec_f_shuno.kibetsu_key := get_kibetsu_key(rec_main.fuka_nendo
                                      ,rec_main.soto_nendo
                                      ,rec_main.zeimoku_cd
                                      ,rec_main.kibetsu_cd
                                      ,rec_main.tokucho_shitei_no);

        rec_f_shuno.shuno_keshikomi_key := rec_main.shuno_rireki_no;
        rec_f_shuno.nofu_shurui_cd      := rec_main.shuno_kbn;
        rec_f_shuno.fuka_nendo          := rec_main.fuka_nendo::numeric;
        rec_f_shuno.soto_nendo          := rec_main.soto_nendo::numeric;
        rec_f_shuno.zeimoku_cd          := get_r4g_code_conv(0, 3, rec_main.zeimoku_cd, null)::numeric;
        rec_f_shuno.kibetsu_cd          := rec_main.kibetsu_cd::numeric;
        rec_f_shuno.kibetsu             := lc_kibetsu;
        rec_f_shuno.kojin_no            := rec_main.atena_no;
        rec_f_shuno.tsuchisho_no        := rec_main.tsuchisho_no;
        rec_f_shuno.jigyo_kaishi_ymd    :=  CASE 
                                                WHEN ymd_null_check(rec_main.jigyo_kaishi_ymd) THEN 0
                                                ELSE get_date_to_num(to_date(rec_main.jigyo_kaishi_ymd, 'YYYY-MM-DD'))
                                            END;
        rec_f_shuno.jigyo_shuryo_ymd    :=  CASE 
                                                WHEN ymd_null_check(rec_main.jigyo_shuryo_ymd) THEN 0
                                                ELSE get_date_to_num(to_date(rec_main.jigyo_shuryo_ymd, 'YYYY-MM-DD'))
                                            END;
        rec_f_shuno.shinkoku_cd         := CASE 
                                                WHEN rec_main.shinkoku_cd = NULL OR rec_main.shinkoku_cd = '' THEN 0
                                                ELSE rec_main.shinkoku_cd::numeric
                                            END;
        rec_f_shuno.shusei_kaisu        := 0;
        rec_f_shuno.nendo_kbn           := 0;
        rec_f_shuno.kankatsu_cd         := 0;
        rec_f_shuno.kasankin_cd         := 0;
        rec_f_shuno.kaikei_nendo        := rec_main.kaikei_nendo::numeric;  
        rec_f_shuno.zeigaku_shuno       := rec_main.zeigaku_shuno::numeric; 
        rec_f_shuno.tokusoku_shuno      := rec_main.tokusoku_shuno::numeric; 
        rec_f_shuno.entaikin_shuno      := rec_main.entaikin_shuno::numeric; 
        rec_f_shuno.kintowari_shuno     := rec_main.kintowari_shuno::numeric; 
        rec_f_shuno.hojinzeiwari_shuno  := rec_main.hojinzeiwari_shuno::numeric; 
        rec_f_shuno.karikeshi_flg       := CASE
                                                WHEN rec_main.karikeshi_kbn = '0' THEN 1 
                                                WHEN rec_main.karikeshi_kbn = '1' THEN 0 
                                                ELSE 2 
                                            END;
        rec_f_shuno.nofu_jiyu_cd        := rec_main.nofu_kbn::numeric;
        rec_f_shuno.nofu_shubetsu_cd    := rec_main.nofu_shubetsu_cd::numeric;
        rec_f_shuno.kumikae_kbn         := rec_main.kumikae_kbn::numeric;
        rec_f_shuno.nofu_channel_kbn    := rec_main.nofu_channel_kbn::numeric;
        rec_f_shuno.shuno_ymd           := get_date_to_num(to_date(rec_main.ryoshu_ymd, 'YYYY-MM-DD'));
        rec_f_shuno.nikkei_ymd          := get_date_to_num(to_date(rec_main.shunyu_ymd, 'YYYY-MM-DD'));
        rec_f_shuno.shotokuwari_shuno   := 0;
        rec_f_shuno.fukakachiwari_shuno := 0;
        rec_f_shuno.shihonwari_shuno    := 0;
        rec_f_shuno.shunyuwari_shuno    := 0;
        rec_f_shuno.doitsunin_kojin_no  := rec_main.atena_no;
        rec_f_shuno.nenkin_shurui_cd    := rec_main.nenkin_shurui_cd::numeric;
        rec_f_shuno.tokutei_key1        := rec_main.tokutei_key1;
        rec_f_shuno.tokutei_key2        := rec_main.tokutei_key2;
        rec_f_shuno.nofuzumi_no         := rec_main.nofuzumi_no;
        rec_f_shuno.encho_kbn           := rec_main.encho_kbn::numeric;
        rec_f_shuno.shuno_tenpo_cd      := rec_main.shuno_tenpo_cd;
        rec_f_shuno.shuno_shiten_cd     := rec_main.shuno_shiten_cd;
        rec_f_shuno.ins_datetime        := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
        rec_f_shuno.upd_datetime        := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
        rec_f_shuno.upd_tantosha_cd     := rec_main.upd_tantosha_cd;
        rec_f_shuno.upd_tammatsu        := 'SERVER';
        rec_f_shuno.del_flg             := rec_main.del_flg::numeric;

        IF rec_f_shuno.karikeshi_flg = 2 THEN
            DELETE
                FROM f_shuno
                WHERE kibetsu_key = rec_f_shuno.kibetsu_key
                    AND shuno_keshikomi_key = rec_f_shuno.shuno_keshikomi_key;

            GET DIAGNOSTICS ln_del_count_tmp := ROW_COUNT;
            ln_del_count := ln_del_count + ln_del_count_tmp;

            ln_err_count := ln_err_count + 1;
            lc_err_text := '仮消区分取消';
            ln_err_kbn := 1;
            lc_err_cd := lc_err_cd_err;
            ln_result_cd := ln_result_cd_err;
        END IF;

        IF ln_para07 = 1 AND ln_err_kbn = 0 THEN
            BEGIN
                SELECT COUNT(*)
                INTO ln_shuno_count
                FROM f_shuno
                WHERE kibetsu_key = rec_f_shuno.kibetsu_key;

                EXCEPTION
                    WHEN OTHERS THEN NULL;
            END;

            IF ln_shuno_count = 0 THEN
                ln_err_count := ln_err_count + 1;
                lc_err_text := '調定情報なし';
                ln_err_kbn := 1;
                lc_err_cd := lc_err_cd_err;
                ln_result_cd := ln_result_cd_err;
            END IF;
        END IF;
                
        IF ln_err_kbn = 0 THEN
            IF rec_main.del_flg = '1' THEN
                
                DELETE
                FROM f_shuno
                WHERE kibetsu_key = rec_f_shuno.kibetsu_key
                    AND shuno_keshikomi_key = rec_f_shuno.shuno_keshikomi_key;

                GET DIAGNOSTICS ln_del_count_tmp := ROW_COUNT;
                ln_del_count := ln_del_count + ln_del_count_tmp;
                
                lc_err_text := '';
                lc_err_cd := lc_err_cd_normal;
                ln_result_cd := ln_result_cd_del; 

            ELSE
                OPEN cur_lock;
                    FETCH cur_lock INTO rec_lock;
                CLOSE cur_lock;

                IF rec_lock IS NULL THEN
                    BEGIN
                        INSERT INTO f_shuno (
                            kibetsu_key
                            ,shuno_keshikomi_key
                            ,nofu_shurui_cd
                            ,fuka_nendo
                            ,soto_nendo
                            ,zeimoku_cd
                            ,kibetsu_cd
                            ,kibetsu
                            ,kojin_no
                            ,tsuchisho_no
                            ,jigyo_kaishi_ymd
                            ,jigyo_shuryo_ymd
                            ,shinkoku_cd
                            ,shusei_kaisu
                            ,nendo_kbn
                            ,kankatsu_cd
                            ,kasankin_cd
                            ,kaikei_nendo
                            ,zeigaku_shuno
                            ,tokusoku_shuno
                            ,entaikin_shuno
                            ,kintowari_shuno
                            ,hojinzeiwari_shuno
                            ,karikeshi_flg
                            ,nofu_jiyu_cd
                            ,nofu_shubetsu_cd
                            ,kumikae_kbn
                            ,nofu_channel_kbn
                            ,shuno_ymd
                            ,nikkei_ymd
                            ,shotokuwari_shuno
                            ,fukakachiwari_shuno
                            ,shihonwari_shuno
                            ,shunyuwari_shuno
                            ,doitsunin_kojin_no
                            ,nenkin_shurui_cd
                            ,tokutei_key1
                            ,tokutei_key2
                            ,nofuzumi_no
                            ,encho_kbn
                            ,shuno_tenpo_cd
                            ,shuno_shiten_cd
                            ,ins_datetime
                            ,upd_datetime
                            ,upd_tantosha_cd
                            ,upd_tammatsu
                            ,del_flg
                            )
                        VALUES (
                            rec_f_shuno.kibetsu_key
                            ,rec_f_shuno.shuno_keshikomi_key
                            ,rec_f_shuno.nofu_shurui_cd
                            ,rec_f_shuno.fuka_nendo
                            ,rec_f_shuno.soto_nendo
                            ,rec_f_shuno.zeimoku_cd
                            ,rec_f_shuno.kibetsu_cd
                            ,rec_f_shuno.kibetsu
                            ,rec_f_shuno.kojin_no
                            ,rec_f_shuno.tsuchisho_no
                            ,rec_f_shuno.jigyo_kaishi_ymd
                            ,rec_f_shuno.jigyo_shuryo_ymd
                            ,rec_f_shuno.shinkoku_cd
                            ,rec_f_shuno.shusei_kaisu
                            ,rec_f_shuno.nendo_kbn
                            ,rec_f_shuno.kankatsu_cd
                            ,rec_f_shuno.kasankin_cd
                            ,rec_f_shuno.kaikei_nendo
                            ,rec_f_shuno.zeigaku_shuno
                            ,rec_f_shuno.tokusoku_shuno
                            ,rec_f_shuno.entaikin_shuno
                            ,rec_f_shuno.kintowari_shuno
                            ,rec_f_shuno.hojinzeiwari_shuno
                            ,rec_f_shuno.karikeshi_flg
                            ,rec_f_shuno.nofu_jiyu_cd
                            ,rec_f_shuno.nofu_shubetsu_cd
                            ,rec_f_shuno.kumikae_kbn
                            ,rec_f_shuno.nofu_channel_kbn
                            ,rec_f_shuno.shuno_ymd
                            ,rec_f_shuno.nikkei_ymd
                            ,rec_f_shuno.shotokuwari_shuno
                            ,rec_f_shuno.fukakachiwari_shuno
                            ,rec_f_shuno.shihonwari_shuno
                            ,rec_f_shuno.shunyuwari_shuno
                            ,rec_f_shuno.doitsunin_kojin_no
                            ,rec_f_shuno.nenkin_shurui_cd
                            ,rec_f_shuno.tokutei_key1
                            ,rec_f_shuno.tokutei_key2
                            ,rec_f_shuno.nofuzumi_no
                            ,rec_f_shuno.encho_kbn
                            ,rec_f_shuno.shuno_tenpo_cd
                            ,rec_f_shuno.shuno_shiten_cd
                            ,rec_f_shuno.ins_datetime
                            ,rec_f_shuno.upd_datetime
                            ,rec_f_shuno.upd_tantosha_cd
                            ,rec_f_shuno.upd_tammatsu
                            ,rec_f_shuno.del_flg
                        );

                        ln_ins_count := ln_ins_count + 1;
                        lc_err_text := '';
                        lc_err_cd := lc_err_cd_normal;
                        ln_result_cd := ln_result_cd_add;

                    EXCEPTION WHEN OTHERS THEN
                        ln_err_count := ln_err_count + 1;
                        lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                        lc_err_cd := lc_err_cd_err;
                        ln_result_cd := ln_result_cd_err;
                    END;
                ELSE

                    SELECT doitsunin_kojin_no 
                        INTO rec_main.doitsunin_kojin_no
                    FROM f_kojin
                        WHERE kojin_no = rec_f_shuno.kojin_no;

                    BEGIN
                        UPDATE f_shuno
                        SET nofu_shurui_cd = rec_f_shuno.nofu_shurui_cd
                            , kibetsu = rec_f_shuno.kibetsu
                            , jigyo_shuryo_ymd = rec_f_shuno.jigyo_shuryo_ymd
                            , kaikei_nendo = rec_f_shuno.kaikei_nendo
                            , zeigaku_shuno = rec_f_shuno.zeigaku_shuno
                            , tokusoku_shuno = rec_f_shuno.tokusoku_shuno
                            , entaikin_shuno = rec_f_shuno.entaikin_shuno
                            , kintowari_shuno = rec_f_shuno.kintowari_shuno
                            , hojinzeiwari_shuno = rec_f_shuno.hojinzeiwari_shuno
                            , karikeshi_flg = rec_f_shuno.karikeshi_flg
                            , nofu_jiyu_cd = rec_f_shuno.nofu_jiyu_cd
                            , nofu_shubetsu_cd = rec_f_shuno.nofu_shubetsu_cd
                            , kumikae_kbn = rec_f_shuno.kumikae_kbn
                            , nofu_channel_kbn = rec_f_shuno.nofu_channel_kbn
                            , shuno_ymd = rec_f_shuno.shuno_ymd
                            , nikkei_ymd = rec_f_shuno.nikkei_ymd
                            , doitsunin_kojin_no = rec_f_shuno.doitsunin_kojin_no
                            , nenkin_shurui_cd = rec_f_shuno.nenkin_shurui_cd
                            , tokutei_key1 = rec_f_shuno.tokutei_key1
                            , tokutei_key2 = rec_f_shuno.tokutei_key2
                            , nofuzumi_no = rec_f_shuno.nofuzumi_no
                            , encho_kbn = rec_f_shuno.encho_kbn
                            , shuno_tenpo_cd = rec_f_shuno.shuno_tenpo_cd
                            , shuno_shiten_cd = rec_f_shuno.shuno_shiten_cd
                            , upd_datetime = rec_f_shuno.upd_datetime
                            , upd_tantosha_cd = rec_f_shuno.upd_tantosha_cd
                            , upd_tammatsu = rec_f_shuno.upd_tammatsu
                        WHERE kibetsu_key = rec_f_shuno.kibetsu_key
                            AND shuno_keshikomi_key = rec_f_shuno.shuno_keshikomi_key;

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
        END IF;
        
        BEGIN
            UPDATE dlgrenkei.i_r4g_shuno_rireki
            SET result_cd = ln_result_cd
                , error_cd = lc_err_cd
                , error_text = lc_err_text
                , seq_no_renkei = in_n_renkei_seq
                , shori_ymd     = in_n_shori_ymd
            WHERE shikuchoson_cd = rec_main.shikuchoson_cd
                AND fuka_nendo = rec_main.fuka_nendo
                AND soto_nendo = rec_main.soto_nendo
                AND tsuchisho_no = rec_main.tsuchisho_no
                AND zeimoku_cd = rec_main.zeimoku_cd
                AND tokucho_shitei_no = rec_main.tokucho_shitei_no
                AND kibetsu_cd = rec_main.kibetsu_cd
                AND shuno_rireki_no = rec_main.shuno_rireki_no
                AND shinkoku_rireki_no = rec_main.shinkoku_rireki_no
                AND jigyo_nendo_no = rec_main.jigyo_nendo_no
                AND jido_atena_no = rec_main.jido_atena_no;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

    END LOOP;
    CLOSE cur_main;

    IF ln_para02 = 1 THEN
        CALL proc_shuno_create_index();
    END IF;

    rec_log.seq_no_renkei := in_n_renkei_seq;
    rec_log.proc_shuryo_datetime := CURRENT_TIMESTAMP;
    rec_log.proc_shori_count := ln_shori_count;
    rec_log.proc_ins_count := ln_ins_count;
    rec_log.proc_upd_count := ln_upd_count;
    rec_log.proc_del_count := ln_del_count;
    rec_log.proc_err_count := ln_err_count;

   -- データ連携ログ更新
   CALL dlgrenkei.proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;

EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;