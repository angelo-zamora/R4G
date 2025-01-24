CREATE OR REPLACE FUNCTION get_kotei_chimoku(
    in_n_chimoku_cd numeric,
    in_n_bukken_shurui_cd numeric DEFAULT 1
)
RETURNS character varying AS $$
/**********************************************************************************************************************/
/* 処理概要 : R4G固定項目取得ファンクション                                                                                 */
/* 引数     : in_n_chimoku_cd           … 固定コード ( 利用 OR 廃止 OR 定義 )                                           */
/* 戻り値    : in_n_bukken_shurui_cd    … 物件種類コード ( 1:居住用、2:商業用 )                                          */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/24  CRESS-INFO.Angelo   新規作成                                                                 */
/**********************************************************************************************************************/
DECLARE

    lc_chimoku character varying;

BEGIN
    IF COALESCE( in_n_chimoku_cd, 0 ) = 0 THEN
        RETURN NULL;
    END IF;

    BEGIN
        SELECT chimoku INTO lc_chimoku
        FROM t_chimoku
        WHERE chimoku_cd       = in_n_chimoku_cd
          AND bukken_shurui_cd = in_n_bukken_shurui_cd
          AND del_flg          = 0;

    EXCEPTION 
        WHEN NO_DATA_FOUND THEN
            lc_chimoku := '(固定マスターに未登録の値: ' || in_n_chimoku_cd || 
                CASE in_n_bukken_shurui_cd 
                    WHEN 1 THEN '(居住用)' 
                    WHEN 2 THEN '(商業用)'
                    ELSE '' 
                END || ')';
    END;

    RETURN lc_chimoku;

END;
$$ LANGUAGE plpgsql;