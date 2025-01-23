CREATE OR REPLACE FUNCTION get_r4g_code_conv (
    in_conv_kbn IN NUMERIC, 
    in_bunrui_cd IN NUMERIC, 
    in_khn_cd IN character varying, 
    in_r4g_cd IN character varying
)
RETURNS character varying
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 税目コード変換機能                                                                                        */
/* 引数     : in_conv_kbn        NUMERIC               … 変換前税目コード                                               */
/*            in_bunrui_cd       NUMERIC               … 分類コード                                                    */
/*            in_khn_cd          character varying     … 基本データコード                                               */
/*            in_r4g_cd          character varying      … Ｒ４Ｇコード                                                  */
/* 戻値     : lc_out_conv_cd      character varying       … 変換後のコード                                               */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/

DECLARE
   lc_out_conv_cd   character varying;
   ln_mutual_flg    numeric;

BEGIN
    -- 変換区分 = 1 の場合
   IF in_conv_kbn = 1 THEN
        -- 基本データコード取得 を実行する。
        SELECT khn_cd, mutual_flg INTO lc_out_conv_cd, ln_mutual_flg
        FROM z_r4g_code_conv
        WHERE bunrui_cd = in_bunrui_cd
        AND r4g_cd = in_r4g_cd;

        -- 相互変換フラグ = 0 の場合、かつデータが取得できなかった場合
        IF ln_mutual_flg IS NULL OR ln_mutual_flg = 0  THEN
            lc_out_conv_cd := in_r4g_cd;
        END IF;

    -- 変換区分 <> 1 の場合
   ELSE 
        -- Ｒ４Ｇコード取得 を実行する。
        SELECT r4g_cd INTO lc_out_conv_cd
        FROM z_r4g_code_conv
        WHERE bunrui_cd = in_bunrui_cd
        AND khn_cd = in_khn_cd;

        -- データが取得できなかった場合
        IF lc_out_conv_cd IS NULL THEN 
            lc_out_conv_cd := in_khn_cd;
        END IF;

   END IF;

   RETURN lc_out_conv_cd;

EXCEPTION
   WHEN OTHERS THEN
      RAISE NOTICE 'エラー: %', SQLERRM;  
      RETURN NULL;
END;
$$;
