CREATE OR REPLACE FUNCTION get_kaiso(p_kaiso_cd INTEGER)
RETURNS character varying AS $$

/*********************************************************************************************************************/
/* 処理概要 : 階層関数を取得する                                                                                     */
/* 引数　　 : p_kaiso_cd                       … 階層コード                                                         */
/* 戻り値　 :                                                                                                        */
/*-------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : 新規作成                                                                                               */
/*********************************************************************************************************************/

DECLARE
    rec_kaiso character varying;
BEGIN

    SELECT kaiso
    INTO rec_kaiso
    FROM t_kaiso
    WHERE kaiso_cd = p_kaiso_cd
      AND del_flg = 0;

    RETURN rec_kaiso;

EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
        
END;
$$ LANGUAGE plpgsql;
