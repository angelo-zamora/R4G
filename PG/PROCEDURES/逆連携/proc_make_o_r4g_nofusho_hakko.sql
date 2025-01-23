CREATE OR REPLACE PROCEDURE proc_make_o_r4g_nofusho_hakko ( in_n_renkei_data_cd IN numeric,
                                                    inout_n_result_code INOUT character varying,  
                                                    inout_c_err_text INOUT character varying )
AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 納付書発行情報（統合収滞納管理）                                                                             */
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
            , f_taino.shinkoku_rireki_no
            , f_taino.jigyo_nendo_no
            , f_taino.tokucho_shitei_no
            , f_taino.kibetsu_cd
            , f_taino.jido_kojin_no
            , f_nofusho_rireki.hakko_kaisu
            , f_nofusho_rireki.nofusho_hakko_no
            , f_nofusho_rireki.kojin_no
            , f_taino.hihokensha_no
            , f_nofusho_rireki.zeigaku
            , f_nofusho_rireki.entaikin
            , f_nofusho_rireki.tokusoku
            , f_nofusho_rireki.shiharai_kigen_ymd
            , f_taino.keiji_shubetsu_cd
            , f_taino.sharyo_no1											
            , f_taino.sharyo_no2											
            , f_taino.sharyo_no3											
            , f_taino.sharyo_no4											
            , f_nofusho_rireki.bcd_kigen_ymd											
            , f_nofusho_rireki.mpn_kigen_ymd											
            , f_nofusho_rireki.qr_kigen_ymd											
            , f_nofusho_rireki.shuno_kikan_no											
            , f_nofusho_rireki.keshikomi_tokutei_key1											
            , f_nofusho_rireki.keshikomi_tokutei_key2											
            , f_nofusho_rireki.nofu_shubetu_cd											
            , f_nofusho_rireki.nofu_no											
            , f_nofusho_rireki.mnp_no											
            , f_nofusho_rireki.mnp_nofu_kbn											
            , f_nofusho_rireki.CVS_01
            , f_nofusho_rireki.OCR_ID
            , f_nofusho_rireki.OCR_01
            , f_nofusho_rireki.OCR_02
            , f_nofusho_rireki.seq_no_nofusho											
            , f_nofusho_rireki.del_flg
            , f_nofusho_rireki.upd_tantosha_cd
            , f_nofusho_rireki.upd_datetime
            FROM 
                f_nofusho_rireki
            INNER JOIN 
                f_taino ON f_nofusho_rireki.kibetsu_key = f_taino.kibetsu_key
            WHERE 
                f_nofusho_rireki.syutsuryoku_zumi_flg = 0
                AND f_nofusho_rireki.upd_datetime > ld_last_exec_datetime;

    rec_main                                            record;
    rec_o_r4g_nofusho_hakko_diff                         o_r4g_nofusho_hakko_diff%ROWTYPE;

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
         lc_sql := 'TRUNCATE TABLE dlgrenkei.o_r4g_nofusho_hakko_diff';
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

                rec_o_r4g_nofusho_hakko_diff.shikuchoson_cd := lc_jichitai_cd;                                                           	    -- 市区町村コード
                rec_o_r4g_nofusho_hakko_diff.mae_shikuchoson_cd := COALESCE(rec_main.mae_shikuchoson_cd, lc_jichitai_cd);                       -- 合併前_市区町村コード TODO IF NULL, GET VALUE FROM T_KANRI
                rec_o_r4g_nofusho_hakko_diff.fuka_nendo := COALESCE(rec_main.fuka_nendo, 0);                                     			    -- 賦課年度
                rec_o_r4g_nofusho_hakko_diff.soto_nendo := COALESCE(rec_main.soto_nendo, 0);                                     			    -- 相当年度
                rec_o_r4g_nofusho_hakko_diff.tsuchisho_no := COALESCE(rec_main.tsuchisho_no,'');                                                -- 通知書番号
                rec_o_r4g_nofusho_hakko_diff.zeimoku_cd := get_r4g_code_conv(1, 3, null, rec_main.zeimoku_cd::character varying); -- 業務詳細（科目）コード
                rec_o_r4g_nofusho_hakko_diff.shinkoku_rireki_no := COALESCE(rec_main.shinkoku_rireki_no, 0) ;                                   -- 申告履歴番号
                rec_o_r4g_nofusho_hakko_diff.jigyo_nendo_no := COALESCE(rec_main.jigyo_nendo_no, 0);                                 		    -- 事業年度番号
                rec_o_r4g_nofusho_hakko_diff.tokucho_shitei_no := COALESCE(rec_main.tokucho_shitei_no,'');                                      -- 特別徴収義務者指定番号
                rec_o_r4g_nofusho_hakko_diff.kibetsu_cd := LPAD(rec_main.kibetsu_cd::character varying, 2, '0');                 			    -- 期別
                rec_o_r4g_nofusho_hakko_diff.jido_atena_no := COALESCE(rec_main.jido_kojin_no,'');                                              -- 児童_宛名番号
                rec_o_r4g_nofusho_hakko_diff.hakko_kbn := '03';                                                                                 -- 発行システム区分
                rec_o_r4g_nofusho_hakko_diff.hakko_kaisu := COALESCE(rec_main.hakko_kaisu,'');                                                  -- 発行回数
                rec_o_r4g_nofusho_hakko_diff.hakko_remban := COALESCE(rec_main.nofusho_hakko_no::character varying,'');                         -- 発行連番
                rec_o_r4g_nofusho_hakko_diff.saishin_flg := '1';                                                                 		        -- 最新フラグ
                rec_o_r4g_nofusho_hakko_diff.shiteitoshi_gyoseiku_cd := '';                                                                     -- 指定都市_行政区等コード
                rec_o_r4g_nofusho_hakko_diff.atena_no := COALESCE(rec_main.kojin_no, '');                                                       -- 宛名番号
                rec_o_r4g_nofusho_hakko_diff.hihokensha_no := COALESCE(rec_main.hihokensha_no, '');                                             -- 被保険者番号
                rec_o_r4g_nofusho_hakko_diff.zeigaku := COALESCE(rec_main.zeigaku, 0);                                                          -- 収納額
                rec_o_r4g_nofusho_hakko_diff.entaikin := COALESCE(rec_main.entaikin, 0);                                                        -- 延滞金
                rec_o_r4g_nofusho_hakko_diff.tokusoku := COALESCE(rec_main.tokusoku, 0);                                                        -- 督促手数料
                rec_o_r4g_nofusho_hakko_diff.shitei_kigen_ymd := COALESCE(get_formatted_date(rec_main.shiharai_kigen_ymd), lc_default_date);    -- 指定期限
                rec_o_r4g_nofusho_hakko_diff.keiji_shubetsu_cd := LPAD(rec_main.keiji_shubetsu_cd::text, 2, '0');                               -- 種別コード
                rec_o_r4g_nofusho_hakko_diff.sharyo_no1 := COALESCE(rec_main.sharyo_no1, '');                                                   -- 車両番号（標識番号）_標板文字
                rec_o_r4g_nofusho_hakko_diff.sharyo_no2 := COALESCE(rec_main.sharyo_no2, '');                                                   -- 車両番号（標識番号）_分類番号
                rec_o_r4g_nofusho_hakko_diff.sharyo_no3 := COALESCE(rec_main.sharyo_no3, '');                                                   -- 車両番号（標識番号）_かな文字
                rec_o_r4g_nofusho_hakko_diff.sharyo_no4 := COALESCE(rec_main.sharyo_no4, '');                                                   -- 車両番号（標識番号）_一連指定番号
                rec_o_r4g_nofusho_hakko_diff.bcd_kigen_ymd:= COALESCE(get_formatted_date(rec_main.bcd_kigen_ymd), lc_default_date);             -- コンビニバーコード使用期限
                rec_o_r4g_nofusho_hakko_diff.mpn_kigen_ymd := COALESCE(get_formatted_date(rec_main.mpn_kigen_ymd), lc_default_date);            -- マルチペイメント支払期限
                rec_o_r4g_nofusho_hakko_diff.qr_kigen_ymd := COALESCE(get_formatted_date(rec_main.qr_kigen_ymd), lc_default_date);              -- 二次元コード支払期限
                rec_o_r4g_nofusho_hakko_diff.shuno_kikan_no	 := COALESCE(rec_main.shuno_kikan_no, '');                                          -- 収納機関番号
                rec_o_r4g_nofusho_hakko_diff.keshikomi_tokutei_key1 := COALESCE(rec_main.keshikomi_tokutei_key1, '');                           -- 滞納消込特定キー情報1
                rec_o_r4g_nofusho_hakko_diff.keshikomi_tokutei_key2 := COALESCE(rec_main.keshikomi_tokutei_key2, '');                           -- 滞納消込特定キー情報2
                rec_o_r4g_nofusho_hakko_diff.nofu_shubetu_cd := COALESCE(rec_main.nofu_shubetu_cd::character varying, '');                      -- 納付種別
                rec_o_r4g_nofusho_hakko_diff.nofu_no := COALESCE(rec_main.nofu_no, '');                                                         -- 納付番号
                rec_o_r4g_nofusho_hakko_diff.mnp_no := COALESCE(rec_main.mnp_no, '');                                                           -- MPN確認番号
                rec_o_r4g_nofusho_hakko_diff.mnp_nofu_kbn := COALESCE(rec_main.mnp_nofu_kbn, '');                                               -- MPN納付区分
                rec_o_r4g_nofusho_hakko_diff.bcd := COALESCE(rec_main.CVS_01, '');                                                              -- バーコード情報
                rec_o_r4g_nofusho_hakko_diff.ocr_id := COALESCE(rec_main.OCR_ID, '');                                                           -- OCRID
                rec_o_r4g_nofusho_hakko_diff.ocr_01 := COALESCE(rec_main.OCR_01, '');                                                           -- 上段OCR
                rec_o_r4g_nofusho_hakko_diff.ocr_02 := COALESCE(rec_main.OCR_02, '');                                                           -- 下段OCR
                rec_o_r4g_nofusho_hakko_diff.eltax_nozeisha_id := '';                                                                        	-- eLTAX納税者ID
                rec_o_r4g_nofusho_hakko_diff.el_no := '';                                                                        		        -- eL番号
                rec_o_r4g_nofusho_hakko_diff.nofuzumi_no := COALESCE(rec_main.seq_no_nofusho, 0);                                               -- 納付済通知書を一意に特定する番号
                rec_o_r4g_nofusho_hakko_diff.jiko_encho_flg:=                                                                                   -- 時効延長有無区分
                    CASE
                        WHEN rec_main.nofu_shubetu_cd IN (8, 19) THEN '0'
                        ELSE '1'
                    END;
                rec_o_r4g_nofusho_hakko_diff.del_flg := COALESCE(rec_main.del_flg::character varying,'');                                            -- 削除フラグ
                rec_o_r4g_nofusho_hakko_diff.sosasha_cd := COALESCE(rec_main.upd_tantosha_cd,'');                                               	 -- 操作者ID
                rec_o_r4g_nofusho_hakko_diff.sosa_ymd := COALESCE(SUBSTRING(rec_main.upd_datetime::character varying, 1, 10), lc_default_date);      -- 操作年月日 
                rec_o_r4g_nofusho_hakko_diff.sosa_time := COALESCE(SUBSTRING(rec_main.upd_datetime::character varying, 12, 8), lc_default_time);     -- 操作時刻
                rec_o_r4g_nofusho_hakko_diff.seq_no_nofusho := COALESCE(rec_main.seq_no_nofusho::character varying,'');                                                 -- 納付書SEQ											

                        BEGIN
                            INSERT INTO 
                                o_r4g_nofusho_hakko_diff VALUES (rec_o_r4g_nofusho_hakko_diff.*);
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
        FROM o_r4g_nofusho_hakko_diff;

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