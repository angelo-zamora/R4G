CREATE OR REPLACE PROCEDURE proc_make_o_r4g_shobun ( in_n_renkei_data_cd IN numeric,
                                                    inout_n_result_code INOUT character varying,
                                                    inout_c_err_text INOUT character varying)
AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 差押情報（統合収滞納管理）                                                                                 */
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
            (SELECT jichitai_cd FROM t_kanri ORDER BY ins_datetime, upd_datetime DESC LIMIT 1) AS shikuchoson_cd,                                                   -- 市区町村コード
            COALESCE(f_taino.mae_shikuchoson_cd, '') AS mae_shikuchoson_cd,                                                                                         -- 合併前_市区町村コード
            CASE 
                WHEN f_shobun.shobun_shubetsu_cd = '11' THEN '04'
                WHEN f_shobun.sashiosae_kbn = '1' THEN '02'
                WHEN f_shobun.sashiosae_kbn IN ('2', '3') THEN '03'
                ELSE NULL
            END AS shobun_cd,                                                                                                                                       -- 処分コード
            LPAD(f_shobun.shobun_shubetsu_cd ||''|| f_shobun.busho_cd, 2, '0') || LPAD(f_shobun.seq_no_shobun::character varying, 10, '0') AS shobun_kanri_no,      -- 滞納処分管理番号
            f_shobun.shobun_kojin_no AS atena_no,                                                                                                                   -- 宛名番号
            f_taino.fuka_nendo AS fuka_nendo,                                                                                                                       -- 賦課年度
            f_taino.soto_nendo AS soto_nendo,                                                                                                                       -- 相当年度
            f_taino.tsuchisho_no AS tsuchisho_no,                                                                                                                   -- 通知書番号
            get_r4g_code_conv(1, 3, NULL, f_taino.zeimoku_cd::character varying) AS zeimoku_cd,                                                                     -- 業務詳細（科目）コード
            f_taino.tokucho_shitei_no AS tokucho_shitei_no,                                                                                                         -- 特別徴収義務者指定番号
            LPAD(f_taino.kibetsu_cd::character varying, 2, '0') AS kibetsu_cd,                                                                                      -- 期別
            f_taino.shinkoku_rireki_no AS shinkoku_rireki_no,                                                                                                       -- 申告履歴番号
            f_taino.jigyo_nendo_no AS jigyo_nendo_no,                                                                                                               -- 事業年度番号
            f_taino.jido_kojin_no AS jido_atena_no,                                                                                                                 -- 児童_宛名番号
            '1' AS saishin_flg,                                                                                                                                     -- 最新フラグ
            '' AS shiteitoshi_gyoseiku_cd,                                                                                                                          -- 指定都市_行政区等コード
            SUBSTRING(MAX(f_shobun.saishu_shikko_ymd)::character varying, 1, 10) AS hasso_ymd,                                                                      -- 発送年月日
            SUBSTRING(MAX(f_shobun.shobun_ymd)::character varying, 1, 10) AS shobun_ymd,                                                                            -- 到達年月日
            COALESCE(
                SUBSTRING(MAX(f_shobun.kaijo_ymd)::character varying, 1, 10),
                SUBSTRING(MAX(f_shobun.shuryo_ymd)::character varying, 1, 10),
                '0000-00-00'
            ) AS kaijo_ymd,                                                                                                                                         -- 解除年月日
            MAX(f_taino.hihokensha_no) AS hihokensha_no,                                                                                                            -- 被保険者番号
            LPAD(MAX(f_taino.shinkoku_cd)::character varying, 2, '0') AS shinkoku_cd,                                                                               -- 申告区分
            SUBSTRING(MAX(f_taino.jigyo_kaishi_ymd)::character varying, 1, 10) AS jigyo_kaishi_ymd,                                                                 -- 事業年度開始日
            SUBSTRING(MAX(f_taino.jigyo_shuryo_ymd)::character varying, 1, 10) AS jigyo_shuryo_ymd,                                                                 -- 事業年度終了日
            SUM(f_shobun_kibetsu.zeigaku) AS zeigaku,                                                                                                               -- 本税（料）
            SUM(f_shobun_kibetsu.entaikin) AS entaikin,                                                                                                             -- 延滞金
            SUM(f_shobun_kibetsu.tokusoku) AS tokusoku,                                                                                                             -- 督促手数料
            MAX(f_taino.zeigaku_kintowari) AS zeigaku_kintowari,                                                                                                    -- 法人住民税内訳_均等割額
            MAX(f_taino.zeigaku_hojinwari) AS zeigaku_hojinwari,                                                                                                    -- 法人住民税内訳_法人税割額
            MAX(f_shobun.del_flg) AS del_flg,                                                                                                                       -- 削除フラグ
            MAX(f_shobun.upd_tantosha_cd) AS sosasha_cd,                                                                                                            -- 操作者ID
            SUBSTRING(MAX(f_shobun.upd_datetime)::character varying, 1, 10) AS sosa_ymd,                                                                            -- 操作年月日
            SUBSTRING(MAX(f_shobun.upd_datetime)::character varying, 12, 8) AS sosa_time                                                                            -- 操作時刻
        FROM 
            f_shobun
        INNER JOIN 
            f_shobun_kibetsu ON f_shobun.seq_no_shobun = f_shobun_kibetsu.seq_no_shobun
        INNER JOIN 
            f_taino ON f_shobun_kibetsu.kibetsu_key = f_taino.kibetsu_key
        WHERE 
            f_shobun.shobun_jotai_cd BETWEEN 10 AND 30
            AND f_shobun.del_flg = f_shobun_kibetsu.del_flg
            AND f_shobun.upd_datetime > ld_last_exec_datetime
        GROUP BY
            f_taino.mae_shikuchoson_cd,
            CASE 
                WHEN f_shobun.shobun_shubetsu_cd = '11' THEN '04'
                WHEN f_shobun.sashiosae_kbn = '1' THEN '02'
                WHEN f_shobun.sashiosae_kbn IN ('2', '3') THEN '03'
                ELSE NULL
            END,
            LPAD(f_shobun.shobun_shubetsu_cd ||''|| f_shobun.busho_cd, 2, '0') || LPAD(f_shobun.seq_no_shobun::character varying, 10, '0'),
            f_shobun.shobun_kojin_no,
            f_taino.fuka_nendo,
            f_taino.soto_nendo,
            f_taino.tsuchisho_no,
            f_taino.zeimoku_cd,
            f_taino.tokucho_shitei_no,
            LPAD(f_taino.kibetsu_cd::character varying, 2, '0'),
            f_taino.shinkoku_rireki_no,
            f_taino.jigyo_nendo_no,
            f_taino.jido_kojin_no;

    rec_main                          o_r4g_shobun_diff%ROWTYPE;

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
        lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_shobun_diff';
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

                BEGIN

                    INSERT INTO o_r4g_shobun_diff VALUES (rec_main.*);

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
    SELECT COUNT(1) INTO ln_data_count
        FROM o_r4g_shobun_diff;

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
