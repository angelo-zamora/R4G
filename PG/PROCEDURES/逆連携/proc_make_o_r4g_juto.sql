CREATE OR REPLACE PROCEDURE proc_make_o_r4g_juto ( in_n_renkei_data_cd IN numeric,
                                                    inout_n_result_code INOUT character varying,
                                                    inout_c_err_text INOUT character varying)
AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 充当明細（統合収滞納管理）                                                                                */
 /* 引数　　 : in_n_renkei_data_cd                   … 逆連携データコード                                                */
 /*         : inout_n_result_code              　　　… 例外エラー発生時のエラーコード                                     */
 /*         : inout_c_err_text               　　　　… 例外エラー発生時のエラー内容                                       */
 /* 戻り値　 :                                                                                                          */
 /*-------------------------------------------------------------------------------------------------------------------- */
 /* 履歴　　 : 新規作成                                                                                                  */
 /*                                                                                                                     */
 /**********************************************************************************************************************/

DECLARE
    l_renkei_data                               OF_RENKEI_DATA%ROWTYPE;
    ln_diff_data_kbn                            OF_RENKEI_DATA.DIFF_DATA_KBN%TYPE;
    ld_last_exec_datetime                       OF_RENKEI_DATA.LAST_EXEC_DATETIME%TYPE;
    lc_renkei_data                              OF_RENKEI_DATA.RENKEI_DATA%TYPE;

    -- 処理開始日時（システム日付）
    ld_kaishi_datetime                          TIMESTAMP;

    ln_data_count                               NUMERIC DEFAULT 0;

    lc_sql                                      character varying;
    lc_jichitai_cd                              character varying;

    -- デフォルト値
    lc_default_date                             character varying := '0000-00-00';
    lc_default_time                             character varying := '00:00:00';
    lc_default_last_exec_dt                     character varying := '1900-01-01';

    --SQL文
    cur_main CURSOR FOR
        SELECT
            f_taino.mae_shikuchoson_cd,
            f_juto.busho_cd,
            f_juto.seq_no_shobun,
            f_juto.shobun_kojin_no,
            f_taino.fuka_nendo,
            f_taino.soto_nendo,
            f_taino.tsuchisho_no,
            f_taino.zeimoku_cd,
            f_taino.tokucho_shitei_no,
            f_taino.kibetsu_cd,
            f_taino.shinkoku_rireki_no,
            f_taino.jigyo_nendo_no,
            f_taino.jido_kojin_no,
            f_juto.juto_ymd,
            f_juto.juto_nyuryoku_ymd,
            f_taino.kojin_no,
            f_taino.hihokensha_no,
            f_taino.shinkoku_cd,
            f_taino.jigyo_kaishi_ymd,
            f_taino.jigyo_shuryo_ymd,
            f_juto.entaikin_keisan_ymd,
            f_juto_kibetsu.zeigaku_jutomae,
            f_juto_kibetsu.entaikin_jutomae,
            f_juto_kibetsu.tokusoku_jutomae,
            f_taino.zeigaku_kintowari,
            f_taino.zeigaku_hojinwari,
            f_juto.juto_taino_shobunhi,
            f_juto.del_flg,
            f_juto.upd_tantosha_cd,
            f_juto.upd_datetime
        FROM
            f_juto
        INNER JOIN
            f_juto_kibetsu ON f_juto.seq_no_juto = f_juto_kibetsu.seq_no_juto
        INNER JOIN
            f_taino ON f_juto_kibetsu.kibetsu_key = f_taino.kibetsu_key
        WHERE
            f_juto.upd_datetime > ld_last_exec_datetime;

    rec_main                                    record;
    rec_o_r4g_juto_diff                        o_r4g_juto_diff%ROWTYPE;

BEGIN

    --２．処理開始日時の取得
    ld_kaishi_datetime := CURRENT_TIMESTAMP;

    --３．処理対象の連携情報を取得
    BEGIN
    CALL proc_make_get_renkei_data( in_n_renkei_data_cd, l_renkei_data );

    ln_diff_data_kbn      := l_renkei_data.diff_data_kbn;
    ld_last_exec_datetime := l_renkei_data.last_exec_datetime;
        lc_renkei_data        := l_renkei_data.renkei_data;

        IF ln_diff_data_kbn = 1 OR ld_last_exec_datetime IS NULL THEN
            ld_last_exec_datetime := lc_default_last_exec_dt::TIMESTAMP WITHOUT TIME ZONE;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            inout_c_err_text := SQLERRM;
            inout_n_result_code := SQLSTATE;
            CALL proc_make_add_renkei_log(in_n_renkei_data_cd
                                           ,lc_renkei_data
                                           ,ld_kaishi_datetime
                                           ,CURRENT_TIMESTAMP::TIMESTAMP WITHOUT TIME ZONE
                                           ,ln_data_count );
            RETURN;
    END;

    --４．中間テーブルデータの初期化
    BEGIN
        lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_juto_diff';
        EXECUTE lc_sql;
    EXCEPTION
         WHEN OTHERS THEN
            inout_c_err_text := SQLERRM;
            inout_n_result_code := SQLSTATE;
            CALL proc_make_add_renkei_log(in_n_renkei_data_cd
                                           ,lc_renkei_data
                                           ,ld_kaishi_datetime
                                           ,CURRENT_TIMESTAMP::TIMESTAMP WITHOUT TIME ZONE
                                           ,ln_data_count );
            RETURN;
    END;

    --５．対象データの抽出（全件 or 異動分）
    IF ln_diff_data_kbn IN (1, 2) THEN
        OPEN cur_main;
            LOOP
                FETCH cur_main INTO rec_main;
                EXIT WHEN NOT FOUND;

                SELECT jichitai_cd INTO lc_jichitai_cd FROM t_kanri ORDER BY ins_datetime, upd_datetime DESC LIMIT 1;

                rec_o_r4g_juto_diff.shikuchoson_cd := lc_jichitai_cd;                                                                                                       --市区町村コード
                rec_o_r4g_juto_diff.mae_shikuchoson_cd := rec_main.mae_shikuchoson_cd;                                                                                      --合併前_市区町村コード
                rec_o_r4g_juto_diff.kanri_no := '72' || LPAD(rec_main.busho_cd::character varying, 2, '0') || LPAD(rec_main.seq_no_shobun::character varying, 10, '0');     --充当管理番号
                rec_o_r4g_juto_diff.atena_no := rec_main.shobun_kojin_no;                                                                                                   --宛名番号
                rec_o_r4g_juto_diff.shobun_cd := '99';                                                                                                                      --処分コード
                rec_o_r4g_juto_diff.fuka_nendo := rec_main.fuka_nendo;                                                                                                      --賦課年度
                rec_o_r4g_juto_diff.soto_nendo := rec_main.soto_nendo;                                                                                                      --相当年度
                rec_o_r4g_juto_diff.tsuchisho_no := rec_main.tsuchisho_no;                                                                                                  --通知書番号
                rec_o_r4g_juto_diff.zeimoku_cd := get_r4g_code_conv(1,3,null,rec_main.zeimoku_cd::character varying);                                                       --業務詳細（科目）コード
                rec_o_r4g_juto_diff.tokucho_shitei_no := rec_main.tokucho_shitei_no;                                                                                        --特別徴収義務者指定番号
                rec_o_r4g_juto_diff.kibetsu_cd := LPAD(rec_main.kibetsu_cd::character varying, 2, '0');                                                                     --期別
                rec_o_r4g_juto_diff.shinkoku_rireki_no := rec_main.shinkoku_rireki_no;                                                                                      --申告履歴番号
                rec_o_r4g_juto_diff.jigyo_nendo_no := rec_main.jigyo_nendo_no;                                                                                              --事業年度番号
                rec_o_r4g_juto_diff.jido_atena_no := rec_main.jido_kojin_no;                                                                                                --児童_宛名番号
                rec_o_r4g_juto_diff.saishin_flg := '1';                                                                                                                     --最新フラグ
                rec_o_r4g_juto_diff.shiteitoshi_gyoseiku_cd := '';                                                                                                          --指定都市_行政区等コード
                rec_o_r4g_juto_diff.juto_ymd := COALESCE(get_formatted_date(rec_main.juto_ymd), lc_default_date);                                                           --充当日（予定日）
                rec_o_r4g_juto_diff.juto_nyuryoku_ymd := COALESCE(get_formatted_date(rec_main.juto_nyuryoku_ymd), lc_default_date);                                         --充当入力年月日
                rec_o_r4g_juto_diff.gimusha_atena_no := rec_main.kojin_no;                                                                                                  --義務者宛名番号
                rec_o_r4g_juto_diff.hihokensha_no := rec_main.hihokensha_no::character varying;                                                                             --被保険者番号
                rec_o_r4g_juto_diff.shinkoku_cd := LPAD(rec_main.shinkoku_cd::character varying, 2, '0');                                                                   --申告区分
                rec_o_r4g_juto_diff.jigyo_kaishi_ymd := COALESCE(get_formatted_date(rec_main.jigyo_kaishi_ymd), lc_default_date);                                           --事業年度開始日
                rec_o_r4g_juto_diff.jigyo_shuryo_ymd := COALESCE(get_formatted_date(rec_main.jigyo_shuryo_ymd), lc_default_date);                                           --事業年度終了日
                rec_o_r4g_juto_diff.entaikin_keisan_ymd := COALESCE(get_formatted_date(rec_main.entaikin_keisan_ymd), lc_default_date);                                     --延滞金計算年月日
                rec_o_r4g_juto_diff.zeigaku := rec_main.zeigaku_jutomae;                                                                                                    --充当額_本税（料）
                rec_o_r4g_juto_diff.entaikin := rec_main.entaikin_jutomae;                                                                                                  --充当額_延滞金
                rec_o_r4g_juto_diff.tokusoku := rec_main.tokusoku_jutomae;                                                                                                  --充当額_督促手数料
                rec_o_r4g_juto_diff.zeigaku_kintowari := rec_main.zeigaku_kintowari;                                                                                        --充当額_法人住民税内訳_均等割額
                rec_o_r4g_juto_diff.zeigaku_hojinwari := rec_main.zeigaku_hojinwari;                                                                                        --充当額_法人住民税内訳_法人税割額
                rec_o_r4g_juto_diff.shobunhi_juto_flg := CASE                                                                                                               --滞納処分費への充当有無
                                                        WHEN rec_main.juto_taino_shobunhi = 0 THEN '0'
                                                        ELSE '1'
                                                        END;
                rec_o_r4g_juto_diff.shobunhi := rec_main.juto_taino_shobunhi;                                                                                               --充当額_滞納処分費
                rec_o_r4g_juto_diff.del_flg := rec_main.del_flg::character varying;                                                                                         --削除フラグ
                rec_o_r4g_juto_diff.sosasha_cd := rec_main.upd_tantosha_cd;                                                                                                 --操作者ID
                rec_o_r4g_juto_diff.sosa_ymd := COALESCE(SUBSTRING(rec_main.upd_datetime::character varying, 1, 10), lc_default_date);                                      --操作年月日
                rec_o_r4g_juto_diff.sosa_time:= COALESCE(SUBSTRING(rec_main.upd_datetime::character varying, 12, 8), lc_default_time);                                      --操作時刻

                BEGIN

                    INSERT INTO o_r4g_juto_diff VALUES (rec_o_r4g_juto_diff.*);

                    EXCEPTION
                        WHEN OTHERS THEN
                            inout_c_err_text := SQLERRM;
                            inout_n_result_code := SQLSTATE;
                            CALL proc_make_add_renkei_log(in_n_renkei_data_cd
                                                        ,lc_renkei_data
                                                        ,ld_kaishi_datetime
                                                        ,CURRENT_TIMESTAMP::TIMESTAMP WITHOUT TIME ZONE
                                                        ,ln_data_count );
                            RETURN;
                END;
            END LOOP;
        CLOSE cur_main;
    END IF;



    --６．データ件数の取得
    SELECT COUNT( 1 ) INTO ln_data_count
        FROM o_r4g_juto_diff;

    --８．逆連携データ作成
    PERFORM get_csv_output(in_n_renkei_data_cd);

    --９．最終実行日時の更新
    CALL proc_make_set_renkei_data( in_n_renkei_data_cd, ld_kaishi_datetime );

    --１０ and １１（ログ追加）
    CALL proc_make_add_renkei_log( in_n_renkei_data_cd
                                        ,lc_renkei_data
                                        ,ld_kaishi_datetime
                                        ,CURRENT_TIMESTAMP::TIMESTAMP WITHOUT TIME ZONE
                                        ,ln_data_count );

    EXCEPTION
        WHEN OTHERS THEN
                inout_c_err_text := SQLERRM;
            inout_n_result_code := SQLSTATE;
            RETURN;
END;
$$ LANGUAGE plpgsql;
