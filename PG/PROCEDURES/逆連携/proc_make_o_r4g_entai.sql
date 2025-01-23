CREATE OR REPLACE PROCEDURE proc_make_o_r4g_entai ( in_n_renkei_data_cd IN numeric,
                                                    inout_n_result_code INOUT character varying, 
                                                    inout_c_err_text INOUT character varying )
AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 延滞金減免情報（統合収滞納管理）                                                                             */
 /* 引数　　 : in_n_renkei_data_cd                   … 逆連携データコード                                                */
 /*         : inout_n_result_code              　　　… 例外エラー発生時のエラーコード                                     */
 /*         : inout_c_err_text               　　　　… 例外エラー発生時のエラー内容                                       */
 /* 戻り値　 :                                                                                                          */
 /*--------------------------------------------------------------------------------------------------------------------*/
 /* 履歴　　 : 新規作成                                                                                                 */
 /*                                                                                                                    */
 /**********************************************************************************************************************/

DECLARE

    l_renkei_data                               OF_RENKEI_DATA%ROWTYPE; 
    ln_diff_data_kbn                            OF_RENKEI_DATA.DIFF_DATA_KBN%TYPE;    
    ld_last_exec_datetime                       OF_RENKEI_DATA.LAST_EXEC_DATETIME%TYPE;         
    lc_renkei_data                              OF_RENKEI_DATA.RENKEI_DATA%TYPE;         

    -- 処理開始日時（システム日付）
    ld_kaishi_datetime                          TIMESTAMP;   

    ln_data_count                               NUMERIC DEFAULT 0;  
    lc_jichitai_cd                              character varying;
    lc_sql                                      character varying;

    -- デフォルト値
    lc_default_date                             character varying := '0000-00-00';
    lc_default_time                             character varying := '00:00:00';

    lc_gemmen_data VARCHAR(255);

    cur_main_rireki CURSOR FOR
    SELECT * FROM  o_r4g_entai_diff;

    --ⅰ 猶予情報
    cur_main_yuyo_all CURSOR FOR
    SELECT 
        y.busho_cd,
        y.seq_no_yuyo,
        t.mae_shikuchoson_cd,
        t.fuka_nendo,
        t.soto_nendo,
        t.tsuchisho_no,
        t.zeimoku_cd,
        t.tokucho_shitei_no,
        t.kibetsu_cd,
        t.shinkoku_rireki_no,
        t.jigyo_nendo_no,
        t.jido_kojin_no,
        y.shobun_kojin_no,
        y.yuyo_kaishi_ymd,
        y.yuyo_shuryo_ymd,
        y.torikeshi_ymd,
        t.hihokensha_no,
        t.shinkoku_cd,
        t.jigyo_kaishi_ymd,
        t.jigyo_shuryo_ymd,
        y.entaikin_ritsu_kbn,
        y.del_flg,
        y.upd_tantosha_cd,
        y.upd_datetime
    FROM f_yuyo AS y
    INNER JOIN f_yuyo_kibetsu AS yk 
        ON y.seq_no_yuyo = yk.seq_no_yuyo
    INNER JOIN f_taino AS t 
        ON yk.kibetsu_key = t.kibetsu_key
    WHERE y.yuyo_jotai_cd BETWEEN 10 AND 30
        AND y.entaikin_ritsu_kbn <> 1
        AND y.upd_datetime > ld_last_exec_datetime;

    --ⅱ 処分情報
    cur_main_shobun_all CURSOR FOR
    SELECT 
        sh.shobun_shubetsu_cd,
        sh.busho_cd,
        sh.seq_no_shobun,
        tn.mae_shikuchoson_cd,
        tn.fuka_nendo,
        tn.soto_nendo,
        tn.tsuchisho_no,
        tn.zeimoku_cd,
        tn.tokucho_shitei_no,
        tn.kibetsu_cd,
        tn.shinkoku_rireki_no,
        tn.jigyo_nendo_no,
        tn.jido_kojin_no,
        sh.shobun_kojin_no,
        sh.shobun_ymd,
        sh.shobun_shubetsu_cd,
        tn.zeimoku_cd,
        sh.kaijo_ymd,
        tn.hihokensha_no,
        tn.shinkoku_cd,
        tn.jigyo_kaishi_ymd,
        tn.jigyo_shuryo_ymd,
        sh.entaikin_ritsu_kbn,
        sh.del_flg,
        sh.upd_tantosha_cd,
        sh.upd_datetime
    FROM 
        f_shobun AS sh
    INNER JOIN 
        f_shobun_kibetsu AS sk 
        ON sh.seq_no_shobun = sk.seq_no_shobun
    INNER JOIN 
        f_taino AS tn 
        ON sk.kibetsu_key = tn.kibetsu_key
    WHERE 
        sh.shobun_shubetsu_cd BETWEEN 1 AND 6 
        AND sh.shobun_jotai_cd BETWEEN 10 AND 30
        AND sh.sashiosae_kbn <> 1
        AND sh.entaikin_ritsu_kbn <> 1
        AND sh.upd_datetime > ld_last_exec_datetime
        AND sh.shobun_ymd <> sh.kaijo_ymd
        AND sh.shobun_ymd <> sh.shuryo_ymd
        AND (sh.del_flg <> 1 AND sh.ins_datetime <> sh.upd_datetime )
        AND sk.remban = 0;
        
    -- ⅲ 延滞金減免情報 
    cur_main_menjo_all CURSOR FOR
    SELECT 
        m.busho_cd,
        m.seq_no_shobun,
        t.mae_shikuchoson_cd,
        t.fuka_nendo,
        t.soto_nendo,
        t.tsuchisho_no,
        t.zeimoku_cd,
        t.tokucho_shitei_no,
        t.kibetsu_cd,
        t.shinkoku_rireki_no,
        t.jigyo_nendo_no,
        t.jido_kojin_no,
        m.shobun_kojin_no,
        m.menjo_kaishi_ymd,
        m.kessai_torikeshi_ymd,
        m.menjo_shuryo_ymd,
        t.hihokensha_no,
        t.shinkoku_cd,
        t.jigyo_kaishi_ymd,
        t.jigyo_shuryo_ymd,
        mk.menjo_kingaku,
        m.menjo_ritsu_cd,
        m.del_flg,
        m.upd_tantosha_cd,
        m.upd_datetime
    FROM 
        f_menjo AS m
    INNER JOIN 
        f_menjo_kibetsu AS mk 
        ON m.seq_no_shobun = mk.seq_no_shobun
    INNER JOIN 
        f_taino AS t 
        ON mk.kibetsu_key = t.kibetsu_key
    WHERE 
        m.gemmen_jotai_cd BETWEEN 10 AND 30
        AND m.upd_datetime > ld_last_exec_datetime;
        
    --ⅳ 執行停止情報
    cur_main_shikkoteishi_all CURSOR FOR
    SELECT 
        st.busho_cd,
        st.seq_no_shikkoteishi,
        tk.mae_shikuchoson_cd,
        tk.fuka_nendo,
        tk.soto_nendo,
        tk.tsuchisho_no,
        tk.zeimoku_cd,
        tk.tokucho_shitei_no,
        tk.kibetsu_cd,
        tk.shinkoku_rireki_no,
        tk.jigyo_nendo_no,
        tk.jido_kojin_no,
        st.shobun_kojin_no,
        st.shobun_ymd,
        st.kaijo_ymd,
        tk.hihokensha_no,
        tk.shinkoku_cd,
        tk.jigyo_kaishi_ymd,
        tk.jigyo_shuryo_ymd,
        st.entaikin_ritsu_kbn,
        st.del_flg,
        st.upd_tantosha_cd,
        st.upd_datetime
    FROM 
        f_shikkoteishi AS st
    INNER JOIN f_shikkoteishi_kibetsu AS sk 
        ON st.seq_no_shikkoteishi = sk.seq_no_shikkoteishi
    INNER JOIN 
        f_taino AS tk 
        ON sk.kibetsu_key = tk.kibetsu_key
    WHERE 
        st.shobun_jotai_cd BETWEEN 10 AND 30
        AND st.entaikin_ritsu_kbn <> 1
        AND st.upd_datetime > ld_last_exec_datetime;


    rec_main_yuyo                   record;
    rec_main_shobun                 record;
    rec_main_menjo                  record;
    rec_main_shikkoteishi           record;
    rec_o_r4g_entai_diff            o_r4g_entai_diff%ROWTYPE;

    ln_kikan_ymd                    NUMERIC;

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
         lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_entai_diff';
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
        OPEN cur_main_yuyo_all;
            LOOP
                    FETCH cur_main_yuyo_all INTO rec_main_yuyo;
                    EXIT WHEN NOT FOUND;

                    -- 一番最初のレコードに登録されているものを取得
                    SELECT jichitai_cd INTO lc_jichitai_cd FROM t_kanri ORDER BY ins_datetime, upd_datetime DESC LIMIT 1;

                    IF (rec_main_yuyo.torikeshi_ymd IS NULL OR rec_main_yuyo.torikeshi_ymd <> 0) 
                    AND (rec_main_yuyo.torikeshi_ymd < rec_main_yuyo.yuyo_shuryo_ymd) THEN
                        ln_kikan_ymd := rec_main_yuyo.torikeshi_ymd;
                    ELSE
                        ln_kikan_ymd := rec_main_yuyo.yuyo_shuryo_ymd;
                    END IF;

                    --　取得データをROWTYPEに設定
                    rec_o_r4g_entai_diff := ROW(
                    lc_jichitai_cd,                                                               				                                                -- 市区町村コード
                    COALESCE(rec_main_yuyo.mae_shikuchoson_cd, lc_jichitai_cd),                            		                                                -- 合併前_市区町村コード 
                    '41' || LPAD(rec_main_yuyo.busho_cd::character varying, 2, '0') || LPAD(rec_main_yuyo.seq_no_yuyo::character varying, 2, '0')  || '',       -- 延滞金減免管理番号
                    COALESCE(rec_main_yuyo.fuka_nendo, 0),                                     					                                                -- 賦課年度
                    COALESCE(rec_main_yuyo.soto_nendo, 0),                                     					                                                -- 相当年度
                    COALESCE(rec_main_yuyo.tsuchisho_no,''),                                                                                                    -- 通知書番号
                    get_r4g_code_conv(1, 3, null, LPAD(rec_main_yuyo.zeimoku_cd::character varying, 2, '0')), 	                                                -- 業務詳細（科目）コード
                    COALESCE(rec_main_yuyo.tokucho_shitei_no,''),                                           		                                            -- 特別徴収義務者指定番号
                    LPAD(rec_main_yuyo.kibetsu_cd::character varying, 2, '0'),                 					                                                -- 期別
                    COALESCE(rec_main_yuyo.shinkoku_rireki_no, 0),                             					                                                -- 申告履歴番号
                    COALESCE(rec_main_yuyo.jigyo_nendo_no, 0),                                 					                                                -- 事業年度番号
                    COALESCE(rec_main_yuyo.jido_kojin_no,''),                                                                                                   -- 児童_宛名番号
                    '1',                                                                      					                                                -- 最新フラグ　（固定）
                    '',                                                                       					                                                -- 指定都市_行政区等コード（固定）
                    COALESCE(rec_main_yuyo.shobun_kojin_no,''),                                                                                                 -- 宛名番号
                    COALESCE(get_formatted_date(rec_main_yuyo.yuyo_kaishi_ymd), lc_default_date),                                                               -- 開始年月日
                    COALESCE(get_formatted_date(ln_kikan_ymd), lc_default_date),                                                                                -- 終了年月日
                    COALESCE(rec_main_yuyo.hihokensha_no,''),                                               		                                            -- 被保険者番号
                    COALESCE(LPAD(rec_main_yuyo.shinkoku_cd::character varying, 2, '0'), ''),                    												-- 申告区分
                    COALESCE(get_formatted_date(rec_main_yuyo.jigyo_kaishi_ymd), lc_default_date),               												-- 事業年度開始日
                    COALESCE(get_formatted_date(rec_main_yuyo.jigyo_shuryo_ymd), lc_default_date),               												-- 事業年度終了日
                    0,                                                                                               											-- 免除金額
                    LPAD(get_r4g_code_conv(1, 5, null, LPAD(rec_main_yuyo.entaikin_ritsu_kbn::character varying, 2, '0')), 1, '0'), 		 					-- 免除区分
                    0,                                                                                               											-- 免除率（手入力）
                    COALESCE(rec_main_yuyo.del_flg::character varying,''),                                               	         							-- 削除フラグ
                    COALESCE(rec_main_yuyo.upd_tantosha_cd,''),                                               	 												-- 操作者ID
                    COALESCE(SUBSTRING(rec_main_yuyo.upd_datetime::character varying, 1, 10), lc_default_date),  												-- 操作年月日 
                    COALESCE(SUBSTRING(rec_main_yuyo.upd_datetime::character varying, 12, 8), lc_default_time)   												-- 操作時刻
                    );
                     
                    BEGIN
                        INSERT INTO o_r4g_entai_diff VALUES (rec_o_r4g_entai_diff.*);
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
        CLOSE cur_main_yuyo_all;

        OPEN cur_main_shobun_all;
            LOOP
                FETCH cur_main_shobun_all INTO rec_main_shobun;
                EXIT WHEN NOT FOUND;

                    lc_gemmen_data := get_r4g_rev_gemmen_kaishi_ymd(rec_main_shobun.shobun_ymd,rec_main_shobun.shobun_shubetsu_cd::CHAR, rec_main_shobun.zeimoku_cd);

                   -- 一番最初のレコードに登録されているものを取得
                    SELECT jichitai_cd INTO lc_jichitai_cd FROM t_kanri ORDER BY ins_datetime, upd_datetime DESC LIMIT 1;

                    IF (rec_main_shobun.kaijo_ymd IS NULL OR rec_main_shobun.kaijo_ymd <> 0) THEN
                        ln_kikan_ymd := rec_main_shobun.kaijo_ymd;

                    ELSEIF (rec_main_shobun.shuryo_ymd IS NULL OR rec_main_shobun.shuryo_ymd <> 0) THEN
                        ln_kikan_ymd := rec_main_shobun.shuryo_ymd;

                    ELSE
                        ln_kikan_ymd := 99991231;
                    END IF;

                    --　取得データをROWTYPEに設定
                    rec_o_r4g_entai_diff := ROW(
                    lc_jichitai_cd,                                                               				                                                                                -- 市区町村コード
                    COALESCE(rec_main_shobun.mae_shikuchoson_cd, lc_jichitai_cd),                            		                                                                            -- 合併前_市区町村コード 
                    rec_main_shobun.shobun_shubetsu_cd || LPAD(rec_main_shobun.busho_cd::character varying, 2, '0') || LPAD(rec_main_shobun.seq_no_shobun::character varying, 2, '0')  || '',   -- 延滞金減免管理番号
                    COALESCE(rec_main_shobun.fuka_nendo, 0),                                     					                                                                            -- 賦課年度
                    COALESCE(rec_main_shobun.soto_nendo, 0),                                     					                                                                            -- 相当年度
                    COALESCE(rec_main_shobun.tsuchisho_no,''),                                                                                                                                  -- 通知書番号
                    get_r4g_code_conv(1, 3, null, LPAD(rec_main_shobun.zeimoku_cd::character varying, 2, '0')),  					                                                            -- 業務詳細（科目）コード
                    COALESCE(rec_main_shobun.tokucho_shitei_no,''),                                           		                                                                            -- 特別徴収義務者指定番号
                    LPAD(rec_main_shobun.kibetsu_cd::character varying, 2, '0'),                 					                                                                            -- 期別
                    COALESCE(rec_main_shobun.shinkoku_rireki_no, 0),                             					                                                                            -- 申告履歴番号
                    COALESCE(rec_main_shobun.jigyo_nendo_no, 0),                                 					                                                                            -- 事業年度番号
                    COALESCE(rec_main_shobun.jido_kojin_no,''),                                                                                                                                 -- 児童_宛名番号
                    '1',                                                                      					                                                                                -- 最新フラグ　（固定）
                    '',                                                                       					                                                                                -- 指定都市_行政区等コード（固定）
                    COALESCE(rec_main_shobun.shobun_kojin_no,''),                                                                                                                               -- 宛名番号
                    CASE WHEN lc_gemmen_data IS NOT NULL THEN get_formatted_date(lc_gemmen_data::numeric) ELSE lc_default_date END,                                                             -- 開始年月日
                    COALESCE(get_formatted_date(ln_kikan_ymd), lc_default_date),                                                                                                                -- 終了年月日
                    COALESCE(rec_main_shobun.hihokensha_no,''),                                               		                                                                            -- 被保険者番号
                    COALESCE(LPAD(rec_main_shobun.shinkoku_cd::character varying, 2, '0'), ''),                    												                                -- 申告区分
                    COALESCE(get_formatted_date(rec_main_shobun.jigyo_kaishi_ymd), lc_default_date),               												                                -- 事業年度開始日 
                    COALESCE(get_formatted_date(rec_main_shobun.jigyo_shuryo_ymd), lc_default_date),               												                                -- 事業年度終了日
                    0,                                                                                               												                            -- 免除金額
                    LPAD(get_r4g_code_conv(1, 5, null, LPAD(rec_main_shobun.entaikin_ritsu_kbn::character varying, 2, '0')), 1, '0'), 		 												    -- 免除区分
                    0,                                                                                               												                            -- 免除率（手入力）
                    COALESCE(rec_main_shobun.del_flg::character varying,'') ,                                               	         												        -- 削除フラグ
                    COALESCE(rec_main_shobun.upd_tantosha_cd,''),                                               	 												                            -- 操作者ID
                    COALESCE(SUBSTRING(rec_main_shobun.upd_datetime::character varying, 1, 10), lc_default_date),  												                                -- 操作年月日 
                    COALESCE(SUBSTRING(rec_main_shobun.upd_datetime::character varying, 12, 8), lc_default_time)   												                                -- 操作時刻
                    );
                     
                    BEGIN
                        INSERT INTO o_r4g_entai_diff VALUES (rec_o_r4g_entai_diff.*);
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
        CLOSE cur_main_shobun_all;

        OPEN cur_main_menjo_all;
            LOOP
                FETCH cur_main_menjo_all INTO rec_main_menjo;
                EXIT WHEN NOT FOUND;

                   -- 一番最初のレコードに登録されているものを取得
                    SELECT jichitai_cd INTO lc_jichitai_cd FROM t_kanri ORDER BY ins_datetime, upd_datetime DESC LIMIT 1;

                    IF (rec_main_menjo.kessai_torikeshi_ymd IS NULL OR rec_main_menjo.kessai_torikeshi_ymd <> 0) 
                    AND (rec_main_menjo.kessai_torikeshi_ymd < rec_main_menjo.menjo_shuryo_ymd) THEN
                        ln_kikan_ymd := rec_main_menjo.kessai_torikeshi_ymd;
                    ELSE
                        ln_kikan_ymd := rec_main_menjo.yuyo_shuryo_ymd;
                    END IF;

                    --　取得データをROWTYPEに設定
                    rec_o_r4g_entai_diff := ROW(
                    lc_jichitai_cd,                                                               				                                             -- 市区町村コード
                    COALESCE(rec_main_menjo.mae_shikuchoson_cd, lc_jichitai_cd),                            		                                         -- 合併前_市区町村コード 
                    '43'|| LPAD(rec_main_menjo.busho_cd::character varying, 2, '0') || LPAD(rec_main_menjo.seq_no_shobun::character varying, 2, '0')  || '', -- 延滞金減免管理番号
                    COALESCE(rec_main_menjo.fuka_nendo, 0),                                     					                                         -- 賦課年度
                    COALESCE(rec_main_menjo.soto_nendo, 0),                                     					                                         -- 相当年度
                    COALESCE(rec_main_menjo.tsuchisho_no,''),                                                                                                -- 通知書番号
                    get_r4g_code_conv(1, 3, null, LPAD(rec_main_menjo.zeimoku_cd::character varying, 2, '0')),					                             -- 業務詳細（科目）コード
                    COALESCE(rec_main_menjo.tokucho_shitei_no,''),                                           		                                         -- 特別徴収義務者指定番号
                    LPAD(rec_main_menjo.kibetsu_cd::character varying, 2, '0'),                 					                                         -- 期別
                    COALESCE(rec_main_menjo.shinkoku_rireki_no, 0),                             					                                         -- 申告履歴番号
                    COALESCE(rec_main_menjo.jigyo_nendo_no, 0),                                 					                                         -- 事業年度番号
                    COALESCE(rec_main_menjo.jido_kojin_no,''),                                                                                               -- 児童_宛名番号
                    '1',                                                                      					                                             -- 最新フラグ　（固定）
                    '',                                                                       					                                             -- 指定都市_行政区等コード（固定）
                    COALESCE(rec_main_menjo.shobun_kojin_no,''),                                                                                             -- 宛名番号
                    COALESCE(get_formatted_date(rec_main_menjo.menjo_kaishi_ymd), lc_default_date),                                                          -- 開始年月日
                    COALESCE(get_formatted_date(ln_kikan_ymd), lc_default_date),                                                                             -- 終了年月日
                    COALESCE(rec_main_menjo.hihokensha_no,''),                                               		                                         -- 被保険者番号
                    COALESCE(LPAD(rec_main_menjo.shinkoku_cd::character varying, 2, '0'), ''),                    											 -- 申告区分
                    COALESCE(get_formatted_date(rec_main_menjo.jigyo_kaishi_ymd), lc_default_date),               											 -- 事業年度開始日 
                    COALESCE(get_formatted_date(rec_main_menjo.jigyo_shuryo_ymd), lc_default_date),               											 -- 事業年度終了日
                    COALESCE(rec_main_menjo.menjo_kingaku, 0),                                                                                            	 -- 免除金額 
                    LPAD(get_r4g_code_conv(1, 5, null, LPAD(rec_main_menjo.menjo_ritsu_cd::character varying, 2, '0')), 1, '0'), 		 					 -- 免除区分
                    0,                                                                                               										 -- 免除率（手入力） 
                    COALESCE(rec_main_menjo.del_flg::character varying,''),                                               	         						 -- 削除フラグ
                    COALESCE(rec_main_menjo.upd_tantosha_cd,''),                                               	 											 -- 操作者ID
                    COALESCE(SUBSTRING(rec_main_menjo.upd_datetime::character varying, 1, 10), lc_default_date),  											 -- 操作年月日 
                    COALESCE(SUBSTRING(rec_main_menjo.upd_datetime::character varying, 12, 8), lc_default_time)   											 -- 操作時刻
                    );
                     
                    BEGIN
                        INSERT INTO o_r4g_entai_diff VALUES (rec_o_r4g_entai_diff.*);
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
        CLOSE cur_main_menjo_all;
        
        OPEN cur_main_shikkoteishi_all;
            LOOP
                FETCH cur_main_shikkoteishi_all INTO rec_main_shikkoteishi;
                EXIT WHEN NOT FOUND;

                   -- 一番最初のレコードに登録されているものを取得
                    SELECT jichitai_cd INTO lc_jichitai_cd FROM t_kanri ORDER BY ins_datetime, upd_datetime DESC LIMIT 1;

                    IF (rec_main_shikkoteishi.kaijo_ymd IS NULL OR rec_main_shikkoteishi.kaijo_ymd <> 0)  THEN
                        ln_kikan_ymd := rec_main_shikkoteishi.kaijo_ymd;
                    ELSE
                        ln_kikan_ymd := 99991231;
                    END IF;

                    --　取得データをROWTYPEに設定
                    rec_o_r4g_entai_diff := ROW(
                    lc_jichitai_cd,                                                               				                                                                -- 市区町村コード
                    COALESCE(rec_main_shikkoteishi.mae_shikuchoson_cd, lc_jichitai_cd),                            		                                                        -- 合併前_市区町村コード 
                    '51'|| LPAD(rec_main_shikkoteishi.busho_cd::character varying, 2, '0') || LPAD(rec_main_shikkoteishi.seq_no_shikkoteishi::character varying, 2, '0')  || '',  -- 延滞金減免管理番号
                    COALESCE(rec_main_shikkoteishi.fuka_nendo, 0),                                     					                                                        -- 賦課年度
                    COALESCE(rec_main_shikkoteishi.soto_nendo, 0),                                     					                                                        -- 相当年度
                    COALESCE(rec_main_shikkoteishi.tsuchisho_no,''),                                                                                                            -- 通知書番号
                    get_r4g_code_conv(1, 3, null, LPAD(rec_main_shikkoteishi.zeimoku_cd::character varying, 2, '0')),					                                        -- 業務詳細（科目）コード
                    COALESCE(rec_main_shikkoteishi.tokucho_shitei_no,''),                                           		                                                    -- 特別徴収義務者指定番号
                    LPAD(rec_main_shikkoteishi.kibetsu_cd::character varying, 2, '0'),                 					                                                        -- 期別
                    COALESCE(rec_main_shikkoteishi.shinkoku_rireki_no, 0),                             					                                                        -- 申告履歴番号
                    COALESCE(rec_main_shikkoteishi.jigyo_nendo_no, 0),                                 					                                                        -- 事業年度番号
                    COALESCE(rec_main_shikkoteishi.jido_kojin_no,''),                                                                                                           -- 児童_宛名番号
                    '1',                                                                      					                                                                -- 最新フラグ　（固定）
                    '',                                                                       					                                                                -- 指定都市_行政区等コード（固定）
                    COALESCE(rec_main_shikkoteishi.shobun_kojin_no,''),                                                                                                         -- 宛名番号
                    COALESCE(get_formatted_date(rec_main_shikkoteishi.shobun_ymd), lc_default_date),                                                                            -- 開始年月日
                    COALESCE(get_formatted_date(ln_kikan_ymd), lc_default_date),                                                                                                -- 終了年月日
                    COALESCE(rec_main_shikkoteishi.hihokensha_no,''),                                               		                                                    -- 被保険者番号
                    COALESCE(LPAD(rec_main_shikkoteishi.shinkoku_cd::character varying, 2, '0'), ''),                    											            -- 申告区分
                    COALESCE(get_formatted_date(rec_main_shikkoteishi.jigyo_kaishi_ymd), lc_default_date),               											            -- 事業年度開始日 
                    COALESCE(get_formatted_date(rec_main_shikkoteishi.jigyo_shuryo_ymd), lc_default_date),               											            -- 事業年度終了日
                    0,                                                                                               										                    -- 免除金額 
					LPAD(get_r4g_code_conv(1, 5, null, LPAD(rec_main_shikkoteishi.entaikin_ritsu_kbn::character varying, 2, '0')), 1, '0'), 	                                -- 免除区分
                    0,                                                                                               										                    -- 免除率（手入力） 
                    COALESCE(rec_main_shikkoteishi.del_flg::character varying,''),                                               	         									-- 削除フラグ
                    COALESCE(rec_main_shikkoteishi.upd_tantosha_cd,''),                                               	 											            -- 操作者ID
                    COALESCE(SUBSTRING(rec_main_shikkoteishi.upd_datetime::character varying, 1, 10), lc_default_date),  											            -- 操作年月日 
                    COALESCE(SUBSTRING(rec_main_shikkoteishi.upd_datetime::character varying, 12, 8), lc_default_time)   											            -- 操作時刻
                    );
                     
                    BEGIN
                        INSERT INTO o_r4g_entai_diff VALUES (rec_o_r4g_entai_diff.*);
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
        CLOSE cur_main_shikkoteishi_all;

    END IF;

    --６．データ件数の取得
    SELECT COUNT( 1 ) INTO ln_data_count
          FROM o_r4g_entai_diff;

   -- ７．履歴テーブルの更新（不要）
   
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
END;
$$ LANGUAGE plpgsql;