CREATE OR REPLACE FUNCTION ymd_null_check(in_ymd character varying)
RETURNS BOOLEAN AS $$
/**********************************************************************************************************************/
/* 処理概要 : 日付項目にてnull または 0 または "0000-00-00" の場合チェック                                                */
/* 引数     : in_ymd … 日付                                                                                           */
/* 戻り値    : BOOLEAN … True or False                                                                                */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/22  CRESS-INFO.Drexler   新規作成                                                                */
/**********************************************************************************************************************/
BEGIN
    -- null または 0 または "0000-00-00" の場合チェック
    IF rec_main.birth_ymd IS NULL OR rec_main.birth_ymd = '' OR rec_main.birth_ymd = '0000-00-00' THEN
        -- Trueを返却
        RETURN TRUE;
    END IF;
    -- Falseを返却
    RETURN FALSE;

EXCEPTION
    WHEN OTHERS THEN
        -- 日付エラーの場合
        RETURN True;
END;
$$ LANGUAGE plpgsql;
