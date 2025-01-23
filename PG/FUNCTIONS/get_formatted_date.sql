CREATE OR REPLACE FUNCTION get_formatted_date ( numeric_date IN numeric)
RETURNS character varying AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 日付フォーマット                                                                                      　  */
 /* 引数　　 : in_c_date                       … SQL日付                                                    　　　　　　  */
 /* 戻り値　 :                                                                                                          */
 /*--------------------------------------------------------------------------------------------------------------------*/
 /* 履歴　　 : 新規作成                                                                                                 */
 /*                                                                                                                    */
 /**********************************************************************************************************************/
DECLARE
    formatted_date character varying;
BEGIN
	BEGIN

    --変換処理
    formatted_date := TO_CHAR(TO_DATE(CAST(numeric_date AS TEXT), 'YYYYMMDD'), 'YYYY-MM-DD');
    
    --返却値
    RETURN formatted_date;
		
    EXCEPTION
        WHEN OTHERS THEN
        RAISE NOTICE 'SQLSTATE : %  SQLERRM : %', SQLSTATE, SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;
