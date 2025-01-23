CREATE OR REPLACE PROCEDURE proc_make_o_r4g_shokei ( in_n_renkei_data_cd IN numeric,
                                                    inout_n_result_code INOUT character varying, 
                                                    inout_c_err_text INOUT character varying )
AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 承継納税義務者情報（統合収滞納管理）                                                                        */
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
          ss.shobun_kojin_no
         ,ss.remban
         ,ROW_NUMBER() OVER (PARTITION BY ss.shobun_kojin_no ORDER BY ss.shobun_kojin_no, ss.seq_no_shokei) AS rireki_no
         ,ss.sozokunin_kojin_no
         ,sh.shobun_ymd
         ,ss.sozokubun_bumbo
         ,ss.sozokubun_bunshi
         ,ss.upd_tantosha_cd
         ,ss.upd_datetime
         ,ss.upd_datetime
    FROM 
        f_shokei_sozokunin ss 
    INNER JOIN 
        f_shokei sh
        ON ss.seq_no_shokei = sh.seq_no_shokei
    where 
        ss.upd_datetime > ld_last_exec_datetime;

    rec_main_all                                record;
    rec_o_r4g_shokei_diff                       o_r4g_shokei_diff%ROWTYPE;

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
         lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_shokei_diff';
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
                rec_o_r4g_shokei_diff := ROW(
                lc_jichitai_cd,                                                               			      -- 市区町村コード
                COALESCE(rec_main_all.shobun_kojin_no, ''),                            					      -- 納税義務者_宛名番号
                rec_main_all.rireki_no,                                     					              -- 納税義務者_履歴番号
                COALESCE(rec_main_all.remban, 0),                                     					      -- 承継納付義務者連番
                COALESCE(rec_main_all.sozokunin_kojin_no, ''),                                                -- 承継納付義務者_宛名番号
                '1',					                                                                      -- 最新フラグ
                '',                                           					                              -- 指定都市_行政区等コード
                COALESCE(get_formatted_date(rec_main_all.shobun_ymd), lc_default_date),                       -- 異動年月日
                '1',                             					                                          -- 異動事由
                COALESCE(rec_main_all.sozokubun_bunshi, 0),                                 				  -- 持分_分子
                COALESCE(rec_main_all.sozokubun_bumbo, 0),                                               	  -- 持分_分母
                '1',                                                                      					  -- 削除フラグ
                COALESCE(rec_main_all.upd_tantosha_cd, ''),                                                   -- 操作者ID                               					
                COALESCE(SUBSTRING(rec_main_all.upd_datetime::character varying, 1, 10), lc_default_date),    -- 操作年月日
                COALESCE(SUBSTRING(rec_main_all.upd_datetime::character varying, 12, 8), '00:00:00')          -- 操作時刻
                );

                -- 中間テーブルの登録を行う
                BEGIN
                    INSERT INTO o_r4g_shokei_diff VALUES (rec_o_r4g_shokei_diff.*);
                EXCEPTION
                        WHEN OTHERS THEN
                            inout_c_err_text := SQLERRM;
                            inout_n_result_code := 9;
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
          FROM o_r4g_shokei_diff;
    
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
        RETURN;
END;
$$ LANGUAGE plpgsql;
