CREATE OR REPLACE PROCEDURE proc_make_o_r4g_jiko ( in_n_renkei_data_cd IN numeric,
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

    -- デフォルト値
    lc_default_date                             character varying := '0000-00-00';
    lc_default_time                             character varying := '00:00:00';
    lc_default_last_exec_dt                     character varying := '1900-01-01';
    lc_jichitai_cd                              character varying;
	lc_jiko_nensu                               numeric;
	
    --SQL文
    cur_main CURSOR FOR
        SELECT 
            mae_shikuchoson_cd
            ,fuka_nendo
            ,soto_nendo
            ,tsuchisho_no
            ,zeimoku_cd
            ,tokucho_shitei_no
            ,kibetsu_cd
            ,shinkoku_rireki_no
            ,jigyo_nendo_no
            ,jido_kojin_no
            ,kojin_no
            ,hihokensha_no
            ,shinkoku_cd
            ,jigyo_kaishi_ymd
            ,jigyo_shuryo_ymd
            ,kanno_cd
            ,ho18_jiko_ymd
            ,shometsu_yotei_ymd
            ,kibetsu_key
            ,del_flg
            ,upd_tantosha_cd
            ,upd_datetime
 
        FROM f_taino
        WHERE 
            f_taino.kanno_cd <> 4
            AND (f_taino.noki_kuriage_ymd <> 0 
                AND LENGTH(f_taino.noki_kuriage_ymd::TEXT) = 8
                AND TO_DATE(f_taino.noki_kuriage_ymd::TEXT, 'YYYYMMDD') <= CURRENT_DATE)
            AND f_taino.upd_datetime >ld_last_exec_datetime;
        
    rec_main                                    record;
    rec_o_r4g_jiko_diff                         o_r4g_jiko_diff%ROWTYPE;

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
        lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_jiko_diff';
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

                -- 一番最初のレコードに登録されているものを取得
                SELECT jichitai_cd INTO lc_jichitai_cd FROM t_kanri ORDER BY ins_datetime, upd_datetime DESC LIMIT 1;
                
                SELECT jiko_nensu INTO lc_jiko_nensu FROM t_zeimoku ORDER BY ins_datetime, upd_datetime LIMIT 1;

                rec_o_r4g_jiko_diff.shikuchoson_cd := lc_jichitai_cd;                                                                                       --市区町村コード
                rec_o_r4g_jiko_diff.mae_shikuchoson_cd := COALESCE(rec_main.mae_shikuchoson_cd, lc_jichitai_cd);                                            --合併前_市区町村コード
                rec_o_r4g_jiko_diff.fuka_nendo := COALESCE(rec_main.fuka_nendo, 0);                                                                         --賦課年度
                rec_o_r4g_jiko_diff.soto_nendo := COALESCE(rec_main.soto_nendo, 0);                                                                         --相当年度
                rec_o_r4g_jiko_diff.tsuchisho_no := COALESCE(rec_main.tsuchisho_no,'');                                                                     --通知書番号
                rec_o_r4g_jiko_diff.zeimoku_cd := get_r4g_code_conv(1, 3, null, rec_main.zeimoku_cd::character varying);                                    --業務詳細（科目）コード
                rec_o_r4g_jiko_diff.tokucho_shitei_no := COALESCE(rec_main.tokucho_shitei_no,'');                                                           --特別徴収義務者指定番号
                rec_o_r4g_jiko_diff.kibetsu_cd := LPAD(rec_main.kibetsu_cd::character varying, 2, '0');                                                     --期別
                rec_o_r4g_jiko_diff.shinkoku_rireki_no := COALESCE(rec_main.shinkoku_rireki_no, 0);                                                         --申告履歴番号
                rec_o_r4g_jiko_diff.jigyo_nendo_no := COALESCE(rec_main.jigyo_nendo_no, 0);                                                                 --事業年度番号
                rec_o_r4g_jiko_diff.jido_atena_no := COALESCE(rec_main.jido_kojin_no,'');                                                                   --児童_宛名番号
                rec_o_r4g_jiko_diff.saishin_flg := '1';                                                                                                     --最新フラグ
                rec_o_r4g_jiko_diff.shiteitoshi_gyoseiku_cd := '';                                                                                          --指定都市_行政区等コード
                rec_o_r4g_jiko_diff.atena_no :=  COALESCE(rec_main.kojin_no, '');                                                                           --宛名番号
                rec_o_r4g_jiko_diff.hihokensha_no := COALESCE(rec_main.hihokensha_no, '');                                                                  --被保険者番号
                rec_o_r4g_jiko_diff.shinkoku_cd := LPAD(rec_main.shinkoku_cd::text, 2, '0');                                                                --申告区分
                rec_o_r4g_jiko_diff.jigyo_kaishi_ymd := COALESCE(get_formatted_date(rec_main.jigyo_kaishi_ymd), lc_default_date);                           --事業年度開始日
                rec_o_r4g_jiko_diff.jigyo_shuryo_ymd := COALESCE(get_formatted_date(rec_main.jigyo_shuryo_ymd), lc_default_date);                           --事業年度終了日
                rec_o_r4g_jiko_diff.year2_jiko_ymd :=                                                                                                       --2年時効完成年月日
                    CASE
                        WHEN lc_jiko_nensu = 2 AND rec_main.kanno_cd = 0 THEN
                            CASE 
                                WHEN rec_main.ho18_jiko_ymd IS NOT NULL AND LENGTH(rec_main.ho18_jiko_ymd::TEXT) = 8 THEN
                                    get_formatted_date(rec_main.ho18_jiko_ymd)
                                ELSE 
                                    lc_default_date
                            END
                        ELSE
                            lc_default_date
                    END;
                rec_o_r4g_jiko_diff.year5_jiko_ymd :=                                                                                                       --5年時効完成年月日
                    CASE 
                        WHEN lc_jiko_nensu = 2 AND rec_main.kanno_cd = 0 THEN
                            lc_default_date
                        ELSE
                            CASE 
                                WHEN rec_main.ho18_jiko_ymd IS NOT NULL AND LENGTH(rec_main.ho18_jiko_ymd::TEXT) = 8 THEN
                                    get_formatted_date(rec_main.ho18_jiko_ymd)
                                ELSE
                                    lc_default_date
                            END
                    END;
				rec_o_r4g_jiko_diff.shittei_jiko_ymd := COALESCE(SUBSTRING(rec_main.shometsu_yotei_ymd::character varying, 1, 10), lc_default_date);        --執行停止時効完成年月日
                rec_o_r4g_jiko_diff.saikoku_encho_ymd := get_saikoku_encho_kigen(rec_main.kibetsu_key, rec_main.zeimoku_cd::character varying);             --催告延長期限年月日
                rec_o_r4g_jiko_diff.del_flg := COALESCE(rec_main.del_flg::character varying,'');                                                            --削除フラグ
                rec_o_r4g_jiko_diff.sosasha_cd := COALESCE(rec_main.upd_tantosha_cd,'');                                                                    --操作者ID
                rec_o_r4g_jiko_diff.sosa_ymd := COALESCE(SUBSTRING(rec_main.upd_datetime::character varying, 1, 10), lc_default_date);                      --操作年月日
                rec_o_r4g_jiko_diff.sosa_time := COALESCE(SUBSTRING(rec_main.upd_datetime::character varying, 12, 8), lc_default_time);                     --操作時刻

                BEGIN

                    INSERT INTO o_r4g_jiko_diff VALUES (rec_o_r4g_jiko_diff.*);

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
        FROM o_r4g_jiko_diff;

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