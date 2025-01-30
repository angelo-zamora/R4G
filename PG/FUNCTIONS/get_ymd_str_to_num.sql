CREATE OR REPLACE FUNCTION get_ymd_str_to_num(in_ymd character varying)
RETURNS NUMERIC AS $$
/**********************************************************************************************************************/
/* 処理概要 : 日付項目にてnull または 0 または "0000-00-00" の場合チェック                                                */
/* 引数     : in_ymd … 日付                                                                                            */
/* 戻り値    : NUMERIC … 日付                                                                                          */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/30  CRESS-INFO.Drexler     新規作成     日付文字列から数値の変換を行う                                  */
/**********************************************************************************************************************/
BEGIN

    in_ymd := TRIM(in_ymd);
    -- null または 0 または "0000-00-00" の場合チェック
    IF in_ymd IS NULL OR in_ymd = '' OR in_ymd IS NULL OR in_ymd = '0' OR in_ymd = '0000-00-00' THEN
        RETURN 0;
    END IF;

    RETURN getdatetonum(to_date(in_ymd, 'YYYY-MM-DD'));

EXCEPTION
    WHEN OTHERS THEN
        RETURN 0;
END;
$$ LANGUAGE plpgsql;
