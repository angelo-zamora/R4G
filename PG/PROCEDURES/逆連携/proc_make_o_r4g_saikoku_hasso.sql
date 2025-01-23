CREATE OR REPLACE PROCEDURE proc_make_o_r4g_saikoku_hasso ( in_n_renkei_data_cd IN numeric,
                                                    inout_n_result_code INOUT character varying, 
                                                    inout_c_err_text INOUT character varying )
AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 催告発送管理情報（統合収滞納管理）                                                                             */
 /* 引数　　 : in_n_renkei_data_cd                   … 逆連携データコード                                                */
 /*         : inout_n_result_code              　　　… 例外エラー発生時のエラーコード                                     */
 /*         : inout_c_err_text               　　　　… 例外エラー発生時のエラー内容                                       */
 /* 戻り値　 :                                                                                                          */
 /*-------------------------------------------------------------------------------------------------------------------- */
 /* 履歴　　 : 新規作成                                                                                                  */
 /*                                                                                                                     */
 /**********************************************************************************************************************/

--1．処理開始日時の取得
DECLARE
    l_renkei_data                               OF_RENKEI_DATA%ROWTYPE; 
    ln_diff_data_kbn                            OF_RENKEI_DATA.DIFF_DATA_KBN%TYPE;    
    ld_last_exec_datetime                       OF_RENKEI_DATA.LAST_EXEC_DATETIME%TYPE;         
    lc_renkei_data                              OF_RENKEI_DATA.RENKEI_DATA%TYPE;           

    -- 処理開始日時（システム日付）
    ld_kaishi_datetime                          TIMESTAMP;   

    lc_sql                                      character varying;

    -- デフォルト値
    lc_default_date                             character varying := '0000-00-00';
    lc_default_time                             character varying := '00:00:00';
    ln_data_count                               NUMERIC DEFAULT 0;
    lc_jichitai_cd                              character varying;
    --SQL文
    cur_main CURSOR FOR
        SELECT
            f_taino.mae_shikuchoson_cd
            , f_taino.fuka_nendo
            , f_taino.soto_nendo
            , f_taino.tsuchisho_no
            , f_taino.zeimoku_cd
            , f_taino.tokucho_shitei_no
            , f_taino.kibetsu_cd
            , f_taino.shinkoku_rireki_no
            , f_taino.jigyo_nendo_no
            , f_taino.jido_kojin_no
            , f_saikoku_rireki.hakko_ymd
            , f_saikoku_rireki.kojin_no
            , f_taino.hihokensha_no
            , f_taino.shinkoku_cd
            , f_taino.jigyo_kaishi_ymd
            , f_taino.jigyo_shuryo_ymd
            , f_saikoku_rireki.saikoku_kigen_ymd
            , f_saikoku_rireki.shiharai_kigen_ymd
            , t_saikoku_reibun.saikoku_reibun_cd
            , f_saikoku_rireki.nofusho_shurui_cd
            , f_saikoku_rireki.del_flg
            , f_saikoku_rireki.upd_tantosha_cd
            , f_saikoku_rireki.upd_datetime
        FROM f_saikoku_rireki
        INNER JOIN f_saikoku_rireki_kibetsu 
            ON f_saikoku_rireki.seq_no_saikoku = f_saikoku_rireki_kibetsu.seq_no_saikoku
        INNER JOIN f_taino
            ON f_saikoku_rireki_kibetsu.kibetsu_key = f_taino.kibetsu_key
        INNER JOIN t_saikoku_reibun
            ON f_saikoku_rireki.saikoku_reibun_cd = t_saikoku_reibun.saikoku_reibun_cd
        WHERE f_saikoku_rireki.upd_datetime > ld_last_exec_datetime;

    rec_main                                            record;
    rec_o_r4g_saikoku_hasso_diff                        o_r4g_saikoku_hasso_diff%ROWTYPE;

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
            ld_last_exec_datetime := '1900-01-01'::TIMESTAMP WITHOUT TIME ZONE;
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
        lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_saikoku_hasso_diff';
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

                rec_o_r4g_saikoku_hasso_diff.shikuchoson_cd := lc_jichitai_cd;                                                          			    -- 市区町村コード
                rec_o_r4g_saikoku_hasso_diff.mae_shikuchoson_cd := COALESCE(rec_main.mae_shikuchoson_cd, lc_jichitai_cd);                           -- 合併前_市区町村コード TODO IF NULL, GET VALUE FROM T_KANRI
                rec_o_r4g_saikoku_hasso_diff.fuka_nendo := COALESCE(rec_main.fuka_nendo, 0);                                     					-- 賦課年度
                rec_o_r4g_saikoku_hasso_diff.soto_nendo := COALESCE(rec_main.soto_nendo, 0);                                     					-- 相当年度
                rec_o_r4g_saikoku_hasso_diff.tsuchisho_no := COALESCE(rec_main.tsuchisho_no,'');                                                	-- 通知書番号
                rec_o_r4g_saikoku_hasso_diff.zeimoku_cd := get_r4g_code_conv(1, 3, null, rec_main.zeimoku_cd::character varying);                   -- 業務詳細（科目）コード
                rec_o_r4g_saikoku_hasso_diff.tokucho_shitei_no := COALESCE(rec_main.tokucho_shitei_no,'');                                          -- 特別徴収義務者指定番号
                rec_o_r4g_saikoku_hasso_diff.kibetsu_cd := LPAD(rec_main.kibetsu_cd::character varying, 2, '0');                 					-- 期別
                rec_o_r4g_saikoku_hasso_diff.shinkoku_rireki_no := COALESCE(rec_main.shinkoku_rireki_no, 0);                                        -- 申告履歴番号
                rec_o_r4g_saikoku_hasso_diff.jigyo_nendo_no := COALESCE(rec_main.jigyo_nendo_no, 0);                                				-- 事業年度番号
                rec_o_r4g_saikoku_hasso_diff.jido_atena_no := COALESCE(rec_main.jido_kojin_no,'');                                                  -- 児童_宛名番号
                rec_o_r4g_saikoku_hasso_diff.hasso_ymd := COALESCE(get_formatted_date(rec_main.hakko_ymd), lc_default_date);                        -- 申請年月日
                rec_o_r4g_saikoku_hasso_diff.saishin_flg := '1';                                                                      				-- 最新フラグ
                rec_o_r4g_saikoku_hasso_diff.shiteitoshi_gyoseiku_cd := '';                                                                       	-- 指定都市_行政区等コード
                rec_o_r4g_saikoku_hasso_diff.atena_no := COALESCE(rec_main.kojin_no, '');                                                           -- 宛名番号
                rec_o_r4g_saikoku_hasso_diff.hihokensha_no := COALESCE(rec_main.hihokensha_no, '');                                                 -- 被保険者番号
                rec_o_r4g_saikoku_hasso_diff.shinkoku_cd := LPAD(rec_main.shinkoku_cd::text, 2, '0');                                               -- 申告区分
                rec_o_r4g_saikoku_hasso_diff.jigyo_kaishi_ymd := COALESCE(get_formatted_date(rec_main.jigyo_kaishi_ymd), lc_default_date);          -- 事業年度開始日
                rec_o_r4g_saikoku_hasso_diff.jigyo_shuryo_ymd := COALESCE(get_formatted_date(rec_main.jigyo_shuryo_ymd), lc_default_date);          -- 事業年度終了日
                rec_o_r4g_saikoku_hasso_diff.saikoku_kigen_ymd := COALESCE(get_formatted_date(rec_main.saikoku_kigen_ymd), lc_default_date);        -- 催告期限
                rec_o_r4g_saikoku_hasso_diff.shitei_kigen_ymd := COALESCE(get_formatted_date(rec_main.shiharai_kigen_ymd), lc_default_date);        -- 催告_指定期限
                rec_o_r4g_saikoku_hasso_diff.saikoku_kbn := COALESCE(rec_main.saikoku_reibun_cd::character varying, '');                                  -- 催告区分
                rec_o_r4g_saikoku_hasso_diff.nofusho_shurui_cd :=                                                                                   --納付書種類
                CASE 
                    WHEN rec_main.nofusho_shurui_cd IS NULL THEN ''
                    WHEN rec_main.nofusho_shurui_cd = '2' THEN '2'
                    ELSE '1'
                END;
                rec_o_r4g_saikoku_hasso_diff.del_flg := COALESCE(rec_main.del_flg::character varying,'');                                           -- 削除フラグ
                rec_o_r4g_saikoku_hasso_diff.sosasha_cd := COALESCE(rec_main.upd_tantosha_cd,'');                                               	-- 操作者ID
                rec_o_r4g_saikoku_hasso_diff.sosa_ymd := COALESCE(SUBSTRING(rec_main.upd_datetime::character varying, 1, 10), lc_default_date);    -- 操作年月日 
                rec_o_r4g_saikoku_hasso_diff.sosa_time := COALESCE(SUBSTRING(rec_main.upd_datetime::character varying, 12, 8), lc_default_time);   -- 操作時刻

                BEGIN
                    INSERT INTO 
                        o_r4g_saikoku_hasso_diff VALUES (rec_o_r4g_saikoku_hasso_diff.*);
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
        FROM o_r4g_saikoku_hasso_diff;

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