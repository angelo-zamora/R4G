CREATE OR REPLACE FUNCTION get_str_to_num(in_str_num character varying)
RETURNS NUMERIC AS $$
/**********************************************************************************************************************/
/* 処理概要 : 文字列を数値型変換する                                                                                     */
/* 引数     : in_str_num … 文字列                                                                                   　  */
/* 戻り値　　: NUMERIC    … 変換後の数値                                                                                 */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/30  CRESS-INFO.Drexler     新規作成     文字列から数値の変換を行う                                  */
/**********************************************************************************************************************/

BEGIN
   
    in_str_num := TRIM(in_str_num);

    -- NULLまたは空文字の場合は0を返却
    IF in_str_num IS NULL OR in_str_num = '' THEN
        RETURN 0;
    END IF;

    RETURN in_str_num::numeric;

EXCEPTION
    WHEN OTHERS THEN
        RETURN 0;
END;
$$ LANGUAGE plpgsql;
