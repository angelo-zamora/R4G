CREATE OR REPLACE FUNCTION get_r4g_rev_gemmen_kaishi_ymd(
    in_shobun_ymd NUMERIC,
    in_shobun_shubetsu_cd CHAR,
    in_zeimoku_cd NUMERIC
)
RETURNS VARCHAR AS $$
/**********************************************************************************************************************/
/* 処理概要 :   R4Gァンクション                                                                                        */
/*             処分種別コードが差押の場合、引数の処分日 + 1 を返す。                                                      */
/*  引数       in_shobun_ymd 処分日                                                                                   */
/*             in_shobun_shubetsu_cd 処分種別コード                                                                   */
/*  戻り値　　  処分日                                                                                                 */
/**********************************************************************************************************************/
DECLARE
    ln_sashiosae_gemmen_ymd_kbn NUMERIC;
BEGIN
    BEGIN
        SELECT sashiosae_gemmen_ymd_kbn INTO ln_sashiosae_gemmen_ymd_kbn
        FROM t_entaikin_kanri
        WHERE zeimoku_cd = in_zeimoku_cd;
    EXCEPTION
        WHEN OTHERS THEN
            ln_sashiosae_gemmen_ymd_kbn := NULL;
    END;

    RETURN CASE 
        -- 差押の場合
        WHEN in_shobun_shubetsu_cd IN ('1', '2', '3', '4') 
         AND ln_sashiosae_gemmen_ymd_kbn = 2 THEN
            get_date_to_num(get_num_to_date(in_shobun_ymd) + 1)::VARCHAR
        ELSE
            in_shobun_ymd::VARCHAR
    END;
END;
$$ LANGUAGE plpgsql;
