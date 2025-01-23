CREATE OR REPLACE FUNCTION get_kibetsu_key (
    in_fuka_nendo IN character varying, 
    in_soto_nendo IN character varying, 
    in_gyomu_shosai_cd IN character varying,
    in_kibetsu_cd IN character varying,
    in_tokucho_shitei_no IN character varying,
    in_jido_kojin_no IN character varying,
    in_tsuchisho_no IN character varying,
    in_jigyo_nendo_no IN character varying,
    in_shinkoku_rireki_no IN character varying
)
RETURNS character varying
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 期別明細KEY生成                                                                                           */
/* 引数     : in_fuka_nendo                      character varying      … 賦課年度                                     */
/*            in_soto_nendo                      character varying      … 相当年度                                     */
/*            in_gyomu_shosai_cd                 character varying      … 業務詳細（科目）コード                        */
/*            in_kibetsu_cd                      character varying      … 期別コード                                   */
/*            in_tokucho_shitei_no               character varying      … 特別徴収義務者指定番号                         */
/*            in_jido_kojin_no                   character varying      … 児童宛名番号                                 */
/*            in_tsuchisho_no                    character varying      … 通知書番号                                   */
/*            in_jigyo_nendo_no                  character varying      … 事業年度番号                                 */
/*            in_shinkoku_rireki_no              character varying      … 申告履歴番号                                 */
/* 戻値     : lc_kibetsu_key                     character varying      … 期別明細KEY                                 */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/

DECLARE
   lc_kibetsu_key      character varying := '';
   ln_zeimoku_cd       character varying := '';

   KIHON_DATA TEXT[] := ARRAY[
    'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'A9',
    'AA', 'B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B9',
    'BA', 'BB', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'D1'
  ];

BEGIN
    
    -- 業務詳細（科目）コード が null または 0 の場合
    IF in_gyomu_shosai_cd IS NULL OR in_gyomu_shosai_cd = '0' THEN

       RETURN lc_kibetsu_key;
       
    END IF;

    -- コード変換を実行し、戻り値を<処理:変数>税目コードにセットする。
    ln_zeimoku_cd := get_r4g_code_conv(0, 3, in_gyomu_shosai_cd, NULL);

    IF in_gyomu_shosai_cd = '02' OR array_position(KIHON_DATA, in_gyomu_shosai_cd) IS NOT NULL THEN 

        -- 返却期別キーをフォーマットする
        lc_kibetsu_key := LPAD( in_fuka_nendo, 4, '0' )
                            || LPAD( in_soto_nendo, 4, '0' )
                            || LPAD( ln_zeimoku_cd, 3, '0' )
                            || LPAD( in_kibetsu_cd, 4, '0' )
                            || LPAD( in_jido_kojin_no, 15, '0' )
                            || LPAD( in_tsuchisho_no, 20, '0' )
                            || LPAD( in_jigyo_nendo_no, 8, '0' )
                            || LPAD( in_shinkoku_rireki_no, 8, '0' )
                            || '0';

    -- 業務詳細（科目）コード が上記以外の場合
    ELSE 
        -- 返却期別キーをフォーマットする
        lc_kibetsu_key := LPAD( in_fuka_nendo, 4, '0' )
                            || LPAD( in_soto_nendo, 4, '0' )
                            || LPAD( ln_zeimoku_cd, 3, '0' )
                            || LPAD( in_kibetsu_cd, 4, '0' )
                            || '000000000000000'
                            || LPAD( in_tsuchisho_no, 20, '0' )
                            || LPAD( in_jigyo_nendo_no, 8, '0' )
                            || LPAD( in_shinkoku_rireki_no, 8, '0' )
                            || '0';

    END IF;

    -- 期別キー値を返却する
    RETURN lc_kibetsu_key;

EXCEPTION
   WHEN OTHERS THEN
      RAISE NOTICE 'エラー: %', SQLERRM;  
      RETURN NULL;
END;
$$;
