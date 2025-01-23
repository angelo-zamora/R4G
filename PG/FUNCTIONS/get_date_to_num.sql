CREATE OR REPLACE FUNCTION get_date_to_num(in_date DATE)
RETURNS NUMERIC AS $$
/**********************************************************************************************************************/
/* 処理概要 : 日付を数値型から日付型に変換する                                                                           */
/* 引数     : in_date … 数値日付                                                                                       */
/* 戻り値    : NUMBER … 数値形式の日付                                                                                  */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 変更履歴 : 新規作成                                                               */
/**********************************************************************************************************************/
DECLARE
    out_date NUMERIC;
BEGIN
    IF in_date IS NULL THEN
        -- 引数が「NULL」の場合
        RETURN 0;
    ELSE
        -- 引数が「NULL」以外の場合
        out_date := TO_NUMBER(TO_CHAR(in_date, 'YYYYMMDD'));
    END IF;

    RETURN out_date;

EXCEPTION
    WHEN OTHERS THEN
        -- 日付変換エラーの場合
        RETURN 0;
END;
$$ LANGUAGE plpgsql;
