CREATE OR REPLACE FUNCTION get_ymd_str_to_num(in_ymd character varying)
RETURNS NUMERIC AS $$
/**********************************************************************************************************************/
/* 処理概要 : 日付項目にてnull または 0 または "0000-00-00" の場合チェック                                                */
/* 引数     : in_ymd … 日付                                                                                            */
/* 戻り値    : NUMERIC … 日付                                                                                          */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/22  CRESS-INFO.Drexler   新規作成                                                                */
/**********************************************************************************************************************/
BEGIN
    -- null または 0 または "0000-00-00" の場合チェック
    IF rec_main.birth_ymd IS NULL OR rec_main.birth_ymd = '' OR rec_main.birth_ymd = '0000-00-00' THEN
        RETURN 0;
    END IF;

    RETURN getdatetonum(to_date(rec_main.ido_todoke_ymd, 'YYYY-MM-DD'));

EXCEPTION
    WHEN OTHERS THEN
        RETURN 0;
END;
$$ LANGUAGE plpgsql;
