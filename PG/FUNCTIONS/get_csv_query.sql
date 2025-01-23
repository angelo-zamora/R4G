CREATE OR REPLACE FUNCTION get_csv_query(
    in_c_renkei_cd IN numeric,
    in_target_table IN character varying
)
RETURNS character varying AS $$

/**********************************************************************************************************************/
/* 処理概要 : CSV出力用SQL                                                                                             */
/* 引数　　 : in_c_renkei_cd                        … 連携コード                                                   　　 */
/*     　　 : in_target_table                       … 対象テーブル                                                  　　*/
/* 戻り値　 :                                                                                                          */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : 新規作成                                                                                                 */
/*                                                                                                                    */
/**********************************************************************************************************************/
DECLARE
    lc_sql              character varying := '';
    lc_columns          character varying := '';
    lc_conditions       character varying := '';
    lc_order_clause     character varying := '';

BEGIN 
    
    IF (in_c_renkei_cd = 2 AND in_target_table = 'o_r4g_taino_diff') THEN
        lc_columns := '*';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no';
   
    ELSIF (in_c_renkei_cd = 3 AND in_target_table = 'o_r4g_bunno_diff') THEN
        lc_columns := '*';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, kanri_no, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no, rireki_no, bunno_kaisu, hakko_cnt';
    
    ELSIF (in_c_renkei_cd = 4 AND in_target_table = 'o_r4g_yuyo_diff') THEN
        lc_columns := '*';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, kanri_no, atena_no, shinsei_ymd, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no';

    ELSIF (in_c_renkei_cd = 5 AND in_target_table = 'o_r4g_entai_diff') THEN
        lc_columns := '*';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, kanri_no, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no';
   
    ELSIF (in_c_renkei_cd = 6 AND in_target_table = 'o_r4g_shobun_diff') THEN
        lc_columns := 'shikuchoson_cd, mae_shikuchoson_cd, shobun_cd, shobun_kanri_no, atena_no, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no, saishin_flg, shiteitoshi_gyoseiku_cd, hasso_ymd, shobun_ymd, kaijo_ymd, shinkoku_cd, jigyo_kaishi_ymd, jigyo_shuryo_ymd, zeigaku, entaikin, tokusoku, zeigaku_kintowari, zeigaku_hojinwari, del_flg, sosasha_cd, sosa_ymd, sosa_time';
        lc_conditions := 'WHERE shobun_cd = ''02''';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, shobun_cd, shobun_kanri_no, atena_no, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no';

    ELSIF (in_c_renkei_cd = 7 AND in_target_table = 'o_r4g_shobun_diff') THEN
        lc_columns := '*';
        lc_conditions := 'WHERE shobun_cd = ''03''';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, shobun_cd, shobun_kanri_no, atena_no, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no';

    ELSIF (in_c_renkei_cd = 8 AND in_target_table = 'o_r4g_kuriage_diff') THEN
        lc_columns := '*';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, shobun_cd, kanri_no, atena_no, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no';
   
    ELSIF (in_c_renkei_cd = 9 AND in_target_table = 'o_r4g_shobun_diff') THEN
        lc_columns := '*';
        lc_conditions := 'WHERE shobun_cd = ''04''';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, shobun_cd, shobun_kanri_no, atena_no, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no';

    ELSIF (in_c_renkei_cd = 10 AND in_target_table = 'o_r4g_shikkoteishi_diff') THEN
        lc_columns := '*';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, kanri_no,atena_no, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no';

    ELSIF (in_c_renkei_cd = 11 AND in_target_table = 'o_r4g_kesson_diff') THEN
        lc_columns := '*';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no, atena_no, shobun_ymd';
   
    ELSIF (in_c_renkei_cd = 12 AND in_target_table = 'o_r4g_juto_diff') THEN
        lc_columns := '*';
        lc_order_clause := 'shikuchoson_cd, mae_shikuchoson_cd, kanri_no, atena_no, shobun_cd, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no';

    ELSIF (in_c_renkei_cd = 13 AND in_target_table = 'o_r4g_nofusho_hakko_diff') THEN
        lc_columns := 'shikuchoson_cd, mae_shikuchoson_cd, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, shinkoku_rireki_no, jigyo_nendo_no, tokucho_shitei_no, kibetsu_cd, jido_atena_no, hakko_kbn, hakko_kaisu, hakko_remban, saishin_flg, shiteitoshi_gyoseiku_cd, atena_no, hihokensha_no, zeigaku, entaikin, tokusoku, shitei_kigen_ymd, keiji_shubetsu_cd, sharyo_no1, sharyo_no2, sharyo_no3, sharyo_no4, bcd_kigen_ymd, mpn_kigen_ymd, qr_kigen_ymd, shuno_kikan_no, keshikomi_tokutei_key1, keshikomi_tokutei_key2, nofu_shubetu_cd, nofu_no, mnp_no, mnp_nofu_kbn, bcd, ocr_id, ocr_01, ocr_02, eltax_nozeisha_id, el_no, nofuzumi_no, jiko_encho_flg, del_flg, sosasha_cd, sosa_ymd, sosa_time';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd,fuka_nendo,soto_nendo,tsuchisho_no,zeimoku_cd,shinkoku_rireki_no,jigyo_nendo_no,tokucho_shitei_no,kibetsu_cd,jido_atena_no,hakko_kbn,hakko_kaisu,hakko_remban';

    ELSIF (in_c_renkei_cd = 14 AND in_target_table = 'o_r4g_shokei_diff') THEN
        lc_columns := '*';
        lc_order_clause := 'ORDER BY shikuchoson_cd, atena_no, rireki_no, remban, shokei_atena_no';

    ELSIF (in_c_renkei_cd = 15 AND in_target_table = 'o_r4g_jiko_diff') THEN
		lc_columns := '*';
		lc_order_clause := 'ORDER BY shikuchoson_cd,mae_shikuchoson_cd,fuka_nendo,soto_nendo,tsuchisho_no,zeimoku_cd,tokucho_shitei_no,kibetsu_cd,shinkoku_rireki_no,jigyo_nendo_no,jido_atena_no';
		
    ELSIF (in_c_renkei_cd = 999 AND in_target_table = 'o_r4g_saikoku_hasso_diff') THEN
        lc_columns := '*';
        lc_order_clause := 'ORDER BY shikuchoson_cd, mae_shikuchoson_cd, fuka_nendo, soto_nendo, tsuchisho_no, zeimoku_cd, tokucho_shitei_no, kibetsu_cd, shinkoku_rireki_no, jigyo_nendo_no, jido_atena_no, hasso_ymd, saishin_flg';

    ELSE
        RAISE NOTICE '対象テーブルが存在しません。';
        RETURN NULL;  
    END IF;

    lc_sql := 'SELECT ' || lc_columns || ' FROM ' || in_target_table || ' ' || lc_conditions  || ' ' || lc_order_clause;

    RETURN lc_sql;  

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;  
END;
$$ LANGUAGE plpgsql;
