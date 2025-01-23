CREATE OR REPLACE PROCEDURE proc_make_o_r4g_kuriage ( in_n_renkei_data_cd IN numeric,
                                                    inout_n_result_code INOUT character varying, 
                                                    inout_c_err_text INOUT character varying )
AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 繰上徴収情報（統合収滞納管理）                                                                             */
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
    ln_f_bunno_kibetsu_count                    NUMERIC DEFAULT 0; 
    ln_f_shobun_kibetsu_count                   NUMERIC DEFAULT 0; 
    ln_f_shikkoteishi_kibetsu_count             NUMERIC DEFAULT 0; 
    ln_f_kesson_kibetsu_count                   NUMERIC DEFAULT 0; 

    lc_jichitai_cd                              character varying;
    lc_sql                                      character varying;

    -- デフォルト値
    lc_default_date                             character varying := '0000-00-00';
    lc_default_time                             character varying := '00:00:00';

    cur_main_all CURSOR FOR
    SELECT 
        tn.mae_shikuchoson_cd,																		
        kc.busho_cd,		
        kc.seq_no_shobun,									
        kc.shobun_kojin_no,											
        tn.fuka_nendo,		
        tn.soto_nendo,										
        tn.tsuchisho_no,			
        tn.zeimoku_cd,									
        tn.tokucho_shitei_no,											
        tn.kibetsu_cd,											
        tn.shinkoku_rireki_no,											
        tn.jigyo_nendo_no,											
        tn.jido_kojin_no,													
        kc.kessai_ymd,		
        kc.torikeshi_ymd,		
        kc.kuriage_noki_ymd,		
        kc.kuriage_noki_time,		
        tn.hihokensha_no,		
        tn.shinkoku_cd,		
        tn.jigyo_kaishi_ymd,		
        tn.jigyo_shuryo_ymd,		
        kc_kibetsu.zeigaku,		
        kc_kibetsu.tokusoku,		
        kc_kibetsu.entaikin,				
        tn.zeigaku_kintowari,		
        tn.zeigaku_hojinwari,		
        kc.del_flg,		
        kc.upd_tantosha_cd,		
        kc.upd_datetime			
    FROM f_kuriage_choshu kc
    INNER JOIN 
        f_kuriage_choshu_kibetsu kc_kibetsu 
        ON kc.seq_no_shobun = kc_kibetsu.seq_no_shobun
    INNER JOIN 
        f_taino tn
        ON kc_kibetsu.kibetsu_key = tn.kibetsu_key 
    WHERE 
        kc.kuriage_jotai_cd BETWEEN 10 AND 30
    AND 
        kc.upd_datetime > ld_last_exec_datetime;

    rec_main_all                                record;
    rec_o_r4g_kuriage_diff                      o_r4g_kuriage_diff%ROWTYPE;
    
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
         lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_kuriage_diff';
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

                --取得データをROWTYPEに設定
                 rec_o_r4g_kuriage_diff := ROW(
                lc_jichitai_cd,                                                                                         -- 市区町村コード
                COALESCE(rec_main_all.mae_shikuchoson_cd, lc_jichitai_cd),                                              -- 合併前_市区町村コード
                '01',                                                                                                   -- 処分コード
                COALESCE('7'|| LPAD(rec_main_all.busho_cd::character varying, 2, '0') || LPAD(rec_main_all.seq_no_shobun::character varying, 10, '0')  || '', ''),    -- 繰上徴収管理番号
                COALESCE(rec_main_all.shobun_kojin_no, ''),                                                             -- 宛名番号
                COALESCE(rec_main_all.fuka_nendo, 0),                                                                   -- 賦課年度
                COALESCE(rec_main_all.soto_nendo, 0),                                                                   -- 相当年度
                COALESCE(rec_main_all.tsuchisho_no, ''),                                                                -- 通知書番号
                get_r4g_code_conv(1, 3, null, LPAD(rec_main_all.zeimoku_cd::character varying, 2, '0')), 	            -- 業務詳細（科目）コード            
                COALESCE(rec_main_all.tokucho_shitei_no, ''),                                                           -- 特別徴収義務者指定番号
                LPAD(rec_main_all.kibetsu_cd::character varying, 2, '0'),                                               -- 期別
                COALESCE(rec_main_all.shinkoku_rireki_no, 0),                                                           -- 申告履歴番号
                COALESCE(rec_main_all.jigyo_nendo_no, 0),                                                               -- 事業年度番号
                COALESCE(rec_main_all.jido_kojin_no, ''),                                                               -- 児童_宛名番号
                '1',                                                                                                    -- 最新フラグ
                '',                                                                                                     -- 指定都市_行政区等コード
                COALESCE(get_formatted_date(rec_main_all.kessai_ymd), lc_default_date),                                 -- 繰上徴収決議年月日
                CASE WHEN rec_main_all.torikeshi_ymd <> 0 OR rec_main_all.torikeshi_ymd IS NOT NULL THEN get_formatted_date(rec_main_all.torikeshi_ymd) ELSE lc_default_date END, -- 繰上徴収取消年月日
                COALESCE(get_formatted_date(rec_main_all.kuriage_noki_ymd), lc_default_date),                           -- 繰上徴収後_納期限
                COALESCE(TO_CHAR(TO_TIMESTAMP(rec_main_all.kuriage_noki_time), 'HH24:MI:SS'), lc_default_time),         -- 繰上徴収後_納期限_時刻
                COALESCE(rec_main_all.hihokensha_no,''),                                                                -- 被保険者番号
                COALESCE(LPAD(rec_main_all.shinkoku_cd::character varying, 2, '0'), ''),                                -- 申告区分
                COALESCE(get_formatted_date(rec_main_all.jigyo_kaishi_ymd), lc_default_date),                           -- 事業年度開始日
                COALESCE(get_formatted_date(rec_main_all.jigyo_shuryo_ymd), lc_default_date),                           -- 事業年度終了日
                COALESCE(rec_main_all.zeigaku, 0),                                                                      -- 処分金額_本税（料）
                COALESCE(rec_main_all.tokusoku, 0),                                                                     -- 処分金額_延滞金
                COALESCE(rec_main_all.entaikin, 0),                                                                     -- 処分金額_督促手数料
                COALESCE(rec_main_all.zeigaku_kintowari, 0),                                                            -- 処分金額_法人住民税内訳_均等割額
                COALESCE(rec_main_all.zeigaku_hojinwari, 0),                                                            -- 処分金額_法人住民税内訳_法人税割額
                COALESCE(rec_main_all.del_flg::character varying,''),                                                   -- 削除フラグ
                COALESCE(rec_main_all.upd_tantosha_cd,''),                                                              -- 操作者ID
                COALESCE(SUBSTRING(rec_main_all.upd_datetime::character varying, 1, 10), lc_default_date),              -- 操作年月日
                COALESCE(SUBSTRING(rec_main_all.upd_datetime::character varying, 12, 8), '00:00:00')                    -- 操作時刻
                );

                -- 中間テーブルの登録を行う
                BEGIN
                    INSERT INTO o_r4g_kuriage_diff VALUES (rec_o_r4g_kuriage_diff.*);
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
          FROM o_r4g_kuriage_diff;

    --７．履歴テーブルの更新（不要）

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
