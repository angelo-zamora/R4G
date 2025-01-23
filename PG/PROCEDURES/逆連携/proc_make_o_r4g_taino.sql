CREATE OR REPLACE PROCEDURE proc_make_o_r4g_taino ( in_n_renkei_data_cd IN numeric,
                                                    inout_n_result_code INOUT character varying, 
                                                    inout_c_err_text INOUT character varying )
AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 滞納明細管理（統合収滞納管理）                                                                             */
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
    ln_f_bunno_kibetsu_count                    NUMERIC DEFAULT 0; 
    ln_f_shobun_kibetsu_count                   NUMERIC DEFAULT 0; 
    ln_f_shikkoteishi_kibetsu_count             NUMERIC DEFAULT 0; 
    ln_f_kesson_kibetsu_count                   NUMERIC DEFAULT 0; 

    lc_jichitai_cd                              character varying;
    lc_sql                                      character varying;

    -- デフォルト値
    lc_default_date                             character varying := '0000-00-00';

    --SQL文
    cur_main_all CURSOR FOR
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
        ,kokuhokigo_no
        ,shinkoku_cd
        ,jigyo_kaishi_ymd
        ,jigyo_shuryo_ymd
        ,noki_kuriage_ymd
        ,upd_tantosha_cd
        ,upd_datetime
		,kibetsu_key
		,kanno_cd
    FROM 
        f_taino 
    WHERE 
        upd_datetime > ld_last_exec_datetime;

    rec_main_all                                record;

    --中間テーブルから履歴に登録用
    cur_main_rireki CURSOR FOR
    SELECT *
    FROM o_r4g_taino_diff ;

    rec_main_rireki                             o_r4g_taino_rireki%ROWTYPE;
    rec_o_r4g_taino_diff                        o_r4g_taino_diff%ROWTYPE;

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
         lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_taino_diff';
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
        OPEN cur_main_all;
            LOOP
                FETCH cur_main_all INTO rec_main_all;
                EXIT WHEN NOT FOUND;
    
                -- 一番最初のレコードに登録されているものを取得
                SELECT jichitai_cd INTO lc_jichitai_cd FROM t_kanri ORDER BY ins_datetime, upd_datetime DESC LIMIT 1;

                -- 分割納付の有無
                SELECT COUNT(*) INTO ln_f_bunno_kibetsu_count FROM f_bunno_kibetsu WHERE kibetsu_key = get_trimmed_space(rec_main_all.kibetsu_key) 
                AND del_flg = 0 AND  bunno_jotai_cd BETWEEN 10 AND 19;

                -- 処分の有無
                SELECT COUNT(*) INTO ln_f_shobun_kibetsu_count FROM f_shobun_kibetsu WHERE kibetsu_key = get_trimmed_space(rec_main_all.kibetsu_key)
                AND del_flg = 0 AND  shobun_jotai_cd BETWEEN 10 AND 19;

                -- 執行停止の有無
                SELECT COUNT(*) INTO ln_f_shikkoteishi_kibetsu_count FROM f_shikkoteishi_kibetsu WHERE kibetsu_key = get_trimmed_space(rec_main_all.kibetsu_key) 
                AND del_flg = 0 AND  shobun_jotai_cd BETWEEN 10 AND 19;
                
                -- 欠損の有無
                SELECT COUNT(*) INTO ln_f_kesson_kibetsu_count FROM f_kesson_kibetsu WHERE kibetsu_key = get_trimmed_space(rec_main_all.kibetsu_key) 
                AND del_flg = 0 AND  shobun_jotai_cd = 10;

                --　取得データをROWTYPEに設定
                rec_o_r4g_taino_diff := ROW(
                lc_jichitai_cd,                                                               				  -- 市区町村コード
                COALESCE(rec_main_all.mae_shikuchoson_cd, lc_jichitai_cd),                            		  -- 合併前_市区町村コード 
                COALESCE(rec_main_all.fuka_nendo, 0),                                     					  -- 賦課年度
                COALESCE(rec_main_all.soto_nendo, 0),                                     					  -- 相当年度
                COALESCE(rec_main_all.tsuchisho_no,''),                                                		  -- 通知書番号
                get_r4g_code_conv(1, 3, null, LPAD(rec_main_all.zeimoku_cd::character varying, 2, '0')), 	  -- 業務詳細（科目）コード
                COALESCE(rec_main_all.tokucho_shitei_no,''),                                           		  -- 特別徴収義務者指定番号
                LPAD(rec_main_all.kibetsu_cd::character varying, 2, '0'),                 					  -- 期別
                COALESCE(rec_main_all.shinkoku_rireki_no, 0),                             					  -- 申告履歴番号
                COALESCE(rec_main_all.jigyo_nendo_no, 0),                                 					  -- 事業年度番号
                COALESCE(rec_main_all.jido_kojin_no,''),                                                      -- 児童_宛名番号
                '1',                                                                      					  -- 最新フラグ　（固定）
                '',                                                                       					  -- 指定都市_行政区等コード（固定）
                COALESCE(rec_main_all.kojin_no,''),                                                   		  -- 宛名番号
                COALESCE(rec_main_all.hihokensha_no,''),                                               		  -- 被保険者番号
                '',                                                                       					  -- 配偶者_宛名番号（固定）
                COALESCE(rec_main_all.kokuhokigo_no,''),                                      				  -- 国保記号番号
                COALESCE(LPAD(rec_main_all.shinkoku_cd::character varying, 2, '0'), ''),                      -- 申告区分
                COALESCE(get_formatted_date(rec_main_all.jigyo_kaishi_ymd), lc_default_date),                 -- 事業年度開始日
                COALESCE(get_formatted_date(rec_main_all.jigyo_shuryo_ymd), lc_default_date),                 -- 事業年度終了日
                COALESCE(get_formatted_date(rec_main_all.noki_kuriage_ymd), lc_default_date),                 -- 変更納期限
                CASE WHEN ln_f_bunno_kibetsu_count = 0 THEN 0 ELSE 1 END,                 					  -- 分割納付の有無
                CASE WHEN ln_f_shobun_kibetsu_count = 0 THEN 0 ELSE 1 END,                					  -- 処分の有無
                CASE WHEN ln_f_shikkoteishi_kibetsu_count = 0 THEN 0 ELSE 1 END,          					  -- 執行停止の有無
                CASE WHEN ln_f_kesson_kibetsu_count = 0 THEN 0 ELSE 1 END,                					  -- 欠損の有無
                CASE WHEN rec_main_all.kanno_cd = 4 THEN '1' ELSE '0' END,                					  -- 削除フラグ
                COALESCE(rec_main_all.upd_tantosha_cd,''),                                               	  -- 操作者ID
                COALESCE(SUBSTRING(rec_main_all.upd_datetime::character varying, 1, 10), lc_default_date),    -- 操作年月日 
                COALESCE(SUBSTRING(rec_main_all.upd_datetime::character varying, 12, 8), '00:00:00')          -- 操作時刻
                );

                -- 中間テーブルの登録を行う
                BEGIN
                    INSERT INTO o_r4g_taino_diff VALUES (rec_o_r4g_taino_diff.*);
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
        CLOSE cur_main_all;
    END IF;

    --６．データ件数の取得
    SELECT COUNT( 1 ) INTO ln_data_count
          FROM o_r4g_taino_diff;

    --７．履歴テーブルの更新
    BEGIN
        lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_taino_rireki';
        EXECUTE lc_sql;
    EXCEPTION
        WHEN OTHERS THEN
            inout_c_err_text := SQLERRM;
            inout_n_result_code := SQLSTATE;
            RETURN;
    END;
    
    OPEN cur_main_rireki;
        LOOP
            FETCH cur_main_rireki INTO rec_main_rireki;
            EXIT WHEN NOT FOUND;

            -- 履歴テーブルの登録を行う
            BEGIN
                INSERT INTO o_r4g_taino_rireki  VALUES(rec_main_rireki.*);
            EXCEPTION
                WHEN OTHERS THEN
                    inout_c_err_text := SQLERRM;
                    inout_n_result_code := SQLSTATE;
                    CALL proc_make_add_renkei_log(in_n_renkei_data_cd　,lc_renkei_data　,ld_kaishi_datetime　
                                                    ,CURRENT_TIMESTAMP::TIMESTAMP WITHOUT TIME ZONE
                                                    ,ln_data_count );
                                       
                    RETURN;
            END;
        END LOOP;
    CLOSE cur_main_rireki;

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
