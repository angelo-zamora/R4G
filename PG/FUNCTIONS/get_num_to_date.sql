CREATE OR REPLACE FUNCTION get_num_to_date(in_date NUMERIC)
RETURNS DATE AS $$
/**********************************************************************************************************************/
/* 処理概要 : 日付を数値型から日付型に変換する                                                                        */
/* 引数     : in_date … 数値日付                                                                                     */
/* 戻り値　　: DATE    … 日付                                                                                         */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 変更履歴 : 新規作成                                                                                               */
/**********************************************************************************************************************/
DECLARE
    out_date DATE;
BEGIN
    IF in_date = 0 OR in_date IS NULL THEN
        -- 引数が「0」or「NULL」の場合
        RETURN NULL;
    ELSE
        -- 引数が0orNull以外の場合
        out_date := TO_DATE(in_date::TEXT, 'YYYYMMDD');
    END IF;

    RETURN out_date;

EXCEPTION
    WHEN OTHERS THEN
        -- 日付変換エラーの場合
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
