CREATE OR REPLACE PROCEDURE proc_make_o_r4g_yuyo ( in_n_renkei_data_cd IN numeric,
                                                    inout_n_result_code INOUT character varying, 
                                                    inout_c_err_text INOUT character varying )
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 徴収（換価）猶予情報（統合収滞納管理）                                                                             */
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
    lc_jichitai_cd                              character varying;

    -- デフォルト値
    lc_default_date                             character varying := '0000-00-00';
    lc_default_time                             character varying := '00:00:00';
    ln_data_count                               NUMERIC DEFAULT 0;  

    --SQL文
    cur_main CURSOR FOR
        SELECT
        f_taino.mae_shikuchoson_cd
        , f_yuyo.busho_cd
        , f_yuyo.seq_no_yuyo
        , f_yuyo.shobun_kojin_no
        , f_yuyo.yuyo_shinsei_ymd
        , f_taino.fuka_nendo
        , f_taino.soto_nendo
        , f_taino.tsuchisho_no
        , f_taino.zeimoku_cd
        , f_taino.tokucho_shitei_no
        , f_taino.kibetsu_cd
        , f_taino.shinkoku_rireki_no
        , f_taino.jigyo_nendo_no
        , f_taino.jido_kojin_no
        , f_yuyo.encho_flg
        , f_yuyo.yuyo_kbn
        , f_yuyo.yuyo_kaishi_ymd
        , f_yuyo.yuyo_shuryo_ymd
        , f_yuyo.torikeshi_ymd
        , f_taino.hihokensha_no
        , f_taino.shinkoku_cd
        , f_taino.jigyo_kaishi_ymd
        , f_taino.jigyo_shuryo_ymd
        , f_yuyo_kibetsu.zeigaku
        , f_yuyo_kibetsu.entaikin
        , f_yuyo_kibetsu.tokusoku
        , f_taino.zeigaku_kintowari
        , f_taino.zeigaku_hojinwari
        , f_yuyo.del_flg
        , f_yuyo.upd_tantosha_cd
        , f_yuyo.upd_datetime
        , f_yuyo.shinsei_flg
        FROM f_yuyo
        INNER JOIN f_yuyo_kibetsu ON f_yuyo.seq_no_yuyo = f_yuyo_kibetsu.seq_no_yuyo
        INNER JOIN f_taino ON f_yuyo_kibetsu.kibetsu_key = f_taino.kibetsu_key
        WHERE f_yuyo.yuyo_jotai_cd BETWEEN 10 AND 30
        AND f_yuyo.upd_datetime > ld_last_exec_datetime;

    rec_main                                    record;
    rec_o_r4g_yuyo_diff                         o_r4g_yuyo_diff%ROWTYPE;

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
        lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_yuyo_diff';
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

                rec_o_r4g_yuyo_diff.shikuchoson_cd := lc_jichitai_cd;                                                          			    -- 市区町村コード
                rec_o_r4g_yuyo_diff.mae_shikuchoson_cd := COALESCE(rec_main.mae_shikuchoson_cd, lc_jichitai_cd);                            			-- 合併前_市区町村コード TODO IF NULL, GET VALUE FROM T_KANRI
                rec_o_r4g_yuyo_diff.kanri_no := COALESCE('41' || LPAD(rec_main.busho_cd::character varying, 2, '0') || LPAD(rec_main.seq_no_yuyo::character varying, 10, '0')  || '', '');       -- 徴収（換価）猶予管理番号
                rec_o_r4g_yuyo_diff.atena_no := COALESCE(rec_main.shobun_kojin_no, '');                                                     -- 宛名番号
                rec_o_r4g_yuyo_diff.shinsei_ymd := COALESCE(get_formatted_date(rec_main.yuyo_shinsei_ymd), lc_default_date);                -- 申請年月日
                rec_o_r4g_yuyo_diff.fuka_nendo := COALESCE(rec_main.fuka_nendo, 0);                                     					-- 賦課年度
                rec_o_r4g_yuyo_diff.soto_nendo := COALESCE(rec_main.soto_nendo, 0);                                     					-- 相当年度
                rec_o_r4g_yuyo_diff.tsuchisho_no := COALESCE(rec_main.tsuchisho_no,'');                                                		-- 通知書番号
                rec_o_r4g_yuyo_diff.zeimoku_cd := get_r4g_code_conv(1, 3, null, rec_main.zeimoku_cd::character varying);                    -- 業務詳細（科目）コード
                rec_o_r4g_yuyo_diff.tokucho_shitei_no := COALESCE(rec_main.tokucho_shitei_no,'');                                           -- 特別徴収義務者指定番号
                rec_o_r4g_yuyo_diff.kibetsu_cd := LPAD(rec_main.kibetsu_cd::character varying, 2, '0');                 					-- 期別
                rec_o_r4g_yuyo_diff.shinkoku_rireki_no := COALESCE(rec_main.shinkoku_rireki_no, 0);                                         -- 申告履歴番号
                rec_o_r4g_yuyo_diff.jigyo_nendo_no := COALESCE(rec_main.jigyo_nendo_no, 0);                                				    -- 事業年度番号
                rec_o_r4g_yuyo_diff.jido_atena_no := COALESCE(rec_main.jido_kojin_no,'');                                                   -- 児童_宛名番号
                rec_o_r4g_yuyo_diff.saishin_flg := '1';                                                                      				-- 最新フラグ
                rec_o_r4g_yuyo_diff.shiteitoshi_gyoseiku_cd := '';                                                                       	-- 指定都市_行政区等コード
                rec_o_r4g_yuyo_diff.yuyo_kbn := CASE WHEN rec_main.encho_flg = 0 THEN '1' ELSE '2' END;                                     -- 猶予区分
                rec_o_r4g_yuyo_diff.choshu_kanka_kbn := CASE                                                                                -- 徴収（換価）猶予区分
                WHEN rec_main.yuyo_kbn = 1 THEN '01'
                WHEN rec_main.yuyo_kbn = 2 AND rec_main.shinsei_flg = 1 THEN '02'  
                WHEN rec_main.yuyo_kbn = 2 AND rec_main.shinsei_flg = 0 THEN '03' 
                ELSE NULL 
                END;
                rec_o_r4g_yuyo_diff.yuyo_kaishi_ymd := COALESCE(get_formatted_date(rec_main.yuyo_kaishi_ymd), lc_default_date);           -- 開始年月日
                rec_o_r4g_yuyo_diff.yuyo_shuryo_ymd := COALESCE(get_formatted_date(rec_main.yuyo_shuryo_ymd), lc_default_date);           -- 終了年月日
                rec_o_r4g_yuyo_diff.torikeshi_ymd := COALESCE(get_formatted_date(rec_main.torikeshi_ymd), lc_default_date);               -- 取消決議年月日
                rec_o_r4g_yuyo_diff.hihokensha_no := COALESCE(rec_main.hihokensha_no, '');                                                -- 被保険者番号
                rec_o_r4g_yuyo_diff.shinkoku_cd := LPAD(rec_main.shinkoku_cd::text, 2, '0');                                              -- 申告区分
                rec_o_r4g_yuyo_diff.jigyo_kaishi_ymd := COALESCE(get_formatted_date(rec_main.jigyo_kaishi_ymd), lc_default_date);         -- 事業年度開始日
                rec_o_r4g_yuyo_diff.jigyo_shuryo_ymd := COALESCE(get_formatted_date(rec_main.jigyo_shuryo_ymd), lc_default_date);         -- 事業年度終了日
                rec_o_r4g_yuyo_diff.zeigaku := COALESCE(rec_main.zeigaku, 0);                                                             -- 猶予金額_本税（料）
                rec_o_r4g_yuyo_diff.entaikin := COALESCE(rec_main.entaikin, 0);                                                           -- 猶予金額_延滞金
                rec_o_r4g_yuyo_diff.tokusoku := COALESCE(rec_main.tokusoku, 0);                                                           -- 猶予金額_督促手数料
                rec_o_r4g_yuyo_diff.zeigaku_kintowari := COALESCE(rec_main.zeigaku_kintowari, 0);                                         -- 猶予金額_法人住民税内訳_均等割額
                rec_o_r4g_yuyo_diff.zeigaku_hojinwari := COALESCE(rec_main.zeigaku_hojinwari, 0);                                         -- 猶予金額_法人住民税内訳_法人税割額
                rec_o_r4g_yuyo_diff.del_flg := COALESCE(rec_main.del_flg::character varying,'');                                                             -- 削除フラグ
                rec_o_r4g_yuyo_diff.sosasha_cd := COALESCE(rec_main.upd_tantosha_cd,'');                                               	  -- 操作者ID
                rec_o_r4g_yuyo_diff.sosa_ymd := COALESCE(SUBSTRING(rec_main.upd_datetime::character varying, 1, 10), lc_default_date);    -- 操作年月日 
                rec_o_r4g_yuyo_diff.sosa_time := COALESCE(SUBSTRING(rec_main.upd_datetime::character varying, 12, 8), lc_default_time);   -- 操作時刻

                -- 中間テーブルの登録を行う
                BEGIN
                    INSERT INTO o_r4g_yuyo_diff VALUES (rec_o_r4g_yuyo_diff.*);
                    
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
    SELECT COUNT( 1 ) INTO ln_data_count FROM o_r4g_yuyo_diff;

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