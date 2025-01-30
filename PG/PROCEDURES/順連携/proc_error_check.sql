--------------------------------------------------------
--  DDL for Procedure proc_error_check
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_error_check( 
    in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, 
    in_n_shori_ymd IN numeric, 
    io_c_error_cd INOUT character varying, 
    io_c_error_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : （共通）データ連携エラーチェック                                                                        */
/* 引数　　 : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ                                                                          */
/*            in_n_shori_ymd      … 処理日                                                                           */
/*            io_c_error_cd       … エラーコード                                                                     */
/*            io_c_error_text     … エラー内容                                                                       */
/* 戻り値　 : io_c_error_cd       … エラーコード                                                                     */
/*            io_c_error_text     … エラー内容                                                                       */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :                                                                                          */
/**********************************************************************************************************************/

DECLARE
    rec_err_check RECORD;
    lc_table_name VARCHAR(30);
    lc_sql TEXT;
    lc_type VARCHAR(30);
    ln_length INTEGER DEFAULT 0;
BEGIN

    -- (1) Fetch table name from F_RENKEI_DATA
    BEGIN
        SELECT table_name INTO lc_table_name
        FROM dlgrenkei.f_renkei_data
        WHERE renkei_data_cd = in_n_renkei_data_cd;

        IF lc_table_name IS NULL OR lc_table_name = '' THEN
            io_c_error_cd := '9';
            io_c_error_text := 'エラーチェック対象テーブル未設定';
            RETURN;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            io_c_error_cd := '9';
            io_c_error_text := '中間テーブル名取得エラー';
            RETURN;
    END;

    -- (2) Loop through cursor equivalent
    FOR rec_err_check IN
        SELECT column_name,
               column_comments,
               null_check_flg,
               null_check_error_cd,
               zero_check_flg,
               zero_check_error_cd,
               apostrophe_check_flg,
               apostrophe_check_error_cd,
               date_check_flg,
               date_check_error_cd,
               data_check_table_flg,
               data_check_table_error_cd,
               data_check_table_name
        FROM f_renkei_error_check
        WHERE renkei_data_cd = in_n_renkei_data_cd
          AND (null_check_flg = 1 OR zero_check_flg = 1 OR date_check_flg = 1 
               OR data_check_table_flg = 1 OR apostrophe_check_flg = 1)
    LOOP

        -- NULL Check
        IF rec_err_check.null_check_flg = 1 THEN
            lc_sql := 'UPDATE ' || lc_table_name || ' ' ||
                      'SET error_cd  = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.null_check_error_cd || '), ' ||
                      '    result_cd = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.null_check_error_cd || '), ' ||
                      '    error_text = SUBSTRING(COALESCE(error_text, '''') || ''' || rec_err_check.column_comments || ' NULLエラー, '', 1, 200), ' ||
                      '    shori_ymd = ' || in_n_shori_ymd || ', ' ||
                      '    seq_no_renkei = ' || in_n_renkei_seq || ' ' ||
                      'WHERE ' || rec_err_check.column_name || ' IS NULL';
            BEGIN
                EXECUTE lc_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    NULL;
            END;
        END IF;

        -- ZERO Check
        IF rec_err_check.zero_check_flg = 1 THEN
            lc_sql := 'UPDATE ' || lc_table_name || ' ' ||
                      'SET error_cd  = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.zero_check_error_cd || '), ' ||
                      '    result_cd = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.zero_check_error_cd || '), ' ||
                      '    error_text = SUBSTRING(COALESCE(error_text, '''') || ''' || rec_err_check.column_comments || ' 0エラー, '', 1, 200), ' ||
                      '    shori_ymd = ' || in_n_shori_ymd || ', ' ||
                      '    seq_no_renkei = ' || in_n_renkei_seq || ' ' ||
                      'WHERE ' || rec_err_check.column_name || ' = 0';
            BEGIN
                EXECUTE lc_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    NULL;
            END;
        END IF;

        -- DATE Check
        IF rec_err_check.date_check_flg = 1 THEN
            lc_sql := 'UPDATE ' || lc_table_name || ' ' ||
                      'SET error_cd  = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.date_check_error_cd || '), ' ||
                      '    result_cd = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.date_check_error_cd || '), ' ||
                      '    error_text = SUBSTRING(COALESCE(error_text, '''') || ''' || rec_err_check.column_comments || ' 日付型エラー, '', 1, 200), ' ||
                      '    shori_ymd = ' || in_n_shori_ymd || ', ' ||
                      '    seq_no_renkei = ' || in_n_renkei_seq || ' ' ||
                      'WHERE TO_DATE(' || rec_err_check.column_name || ', ''YYYYMMDD'') IS NULL';
            BEGIN
                EXECUTE lc_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    NULL;
            END;
        END IF;

        -- DATA Check
        IF rec_err_check.data_check_table_flg = 1 THEN
            IF rec_err_check.data_check_table_name = 't_chg_code' THEN
                lc_sql := 'UPDATE ' || lc_table_name || ' ' ||
                          'SET error_cd  = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.data_check_table_error_cd || '), ' ||
                          '    result_cd = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.data_check_table_error_cd || '), ' ||
                          '    error_text = ''コード変換マスタ「T_CHG_CODE」にデータなし'', ' ||
                          '    shori_ymd = ' || in_n_shori_ymd || ', ' ||
                          '    seq_no_renkei = ' || in_n_renkei_seq || ' ' ||
                          'WHERE ' || rec_err_check.column_name || ' NOT IN (SELECT input_cd FROM t_chg_code ' ||
                          'WHERE bunrui_cd = (SELECT bunrui_cd FROM t_chg_bunrui WHERE column_name = ''' || rec_err_check.column_name || '''))';
                BEGIN
                    EXECUTE lc_sql;
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                END;
            ELSE
                IF rec_err_check.data_check_table_name IS NOT NULL AND LENGTH(rec_err_check.data_check_table_name) > 2 THEN
                    lc_sql := 'UPDATE ' || lc_table_name || ' ' ||
                              'SET error_cd  = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.data_check_table_error_cd || '), ' ||
                              '    result_cd = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.data_check_table_error_cd || '), ' ||
                              '    error_text = SUBSTRING(COALESCE(error_text, '''') || ''' || rec_err_check.column_comments || ' マスタ「' || rec_err_check.data_check_table_name || '」になし, '', 1, 200), ' ||
                              '    shori_ymd = ' || in_n_shori_ymd || ', ' ||
                              '    seq_no_renkei = ' || in_n_renkei_seq || ' ' ||
                              'WHERE ' || rec_err_check.column_name || ' NOT IN (SELECT SUBSTRING(' || rec_err_check.data_check_table_name || ', 3, LENGTH(' || rec_err_check.data_check_table_name || ') - 2) ' ||
                              'FROM ' || rec_err_check.data_check_table_name || ')';
                    BEGIN
                        EXECUTE lc_sql;
                    EXCEPTION
                        WHEN OTHERS THEN
                            NULL;
                    END;
                END IF;
            END IF;
        END IF;

        -- APOSTROPHE Check
        IF rec_err_check.apostrophe_check_flg = 1 THEN
            IF rec_err_check.apostrophe_check_error_cd = 9 THEN
                lc_sql := 'UPDATE ' || lc_table_name || ' ' ||
                          'SET error_cd  = 9, ' ||
                          '    result_cd = 9, ' ||
                          '    error_text = SUBSTRING(COALESCE(error_text, '''') || ''' || rec_err_check.column_comments || ' 半角アポストロフィエラー, '', 1, 200), ' ||
                          '    shori_ymd = ' || in_n_shori_ymd || ', ' ||
                          '    seq_no_renkei = ' || in_n_renkei_seq || ' ' ||
                          'WHERE POSITION('''''''' IN ' || rec_err_check.column_name || ') > 0';
            ELSE
                lc_sql := 'UPDATE ' || lc_table_name || ' ' ||
                          'SET error_cd  = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.apostrophe_check_error_cd || '), ' ||
                          '    result_cd = COALESCE(NULLIF(error_cd, 9), ' || rec_err_check.apostrophe_check_error_cd || '), ' ||
                          '    error_text = SUBSTRING(COALESCE(error_text, '''') || ''' || rec_err_check.column_comments || ' 半角アポストロフィエラー, '', 1, 200), ' ||
                          '    shori_ymd = ' || in_n_shori_ymd || ', ' ||
                          '    seq_no_renkei = ' || in_n_renkei_seq || ' ' ||
                          'WHERE POSITION('''''''' IN ' || rec_err_check.column_name || ') > 0';
            END IF;
            BEGIN
                EXECUTE lc_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    NULL;
            END;
        END IF;

    END LOOP;

END;
$$;
