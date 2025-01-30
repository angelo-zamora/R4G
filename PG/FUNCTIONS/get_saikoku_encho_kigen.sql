CREATE OR REPLACE FUNCTION get_saikoku_encho_kigen(in_kibetsu_key IN character varying,
                                                    in_zeimoku_cd IN character varying)
RETURNS character varying AS $$
/**********************************************************************************************************************/
/* 処理概要 : 催告延長期限日取得                                                                                        */
/* 引数     : in_kibetsu_key  … 期別明細KEY                                                                            */
/* 引数 　　: in_zeimoku_cd   … 税目コード                                                                            */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 変更履歴 : 新規作成                                                                                               */
/**********************************************************************************************************************/
DECLARE

    ln_saikoku_nissu        NUMERIC;
    ln_hakko_ymd		    NUMERIC;

BEGIN
    SELECT saikoku_nissu INTO ln_saikoku_nissu
        FROM t_jiko_kanri
        WHERE zeimoku_cd = in_zeimoku_cd::numeric;

    SELECT
            MAX(f_saikoku_rireki.hakko_ymd) INTO ln_hakko_ymd
        FROM
            f_saikoku_rireki_kibetsu
        LEFT JOIN
            f_saikoku_rireki ON f_saikoku_rireki_kibetsu.seq_no_saikoku = f_saikoku_rireki.seq_no_saikoku
        LEFT JOIN
            t_saikoku_reibun ON f_saikoku_rireki.saikoku_reibun_cd = t_saikoku_reibun.saikoku_reibun_cd
        WHERE
            f_saikoku_rireki_kibetsu.kibetsu_key = in_kibetsu_key
            AND f_saikoku_rireki_kibetsu.del_flg = 0
            AND f_saikoku_rireki.del_flg = 0
            AND t_saikoku_reibun.del_flg = 0
            AND t_saikoku_reibun.jiko_encho_flg = 1;

    IF ln_hakko_ymd IS NULL THEN
        RETURN '0000-00-00';
    ELSE 
        RETURN CAST(get_jiko_add_months_m(getdatetonum(get_num_to_date(ln_hakko_ymd)) + ln_saikoku_nissu, 6, 0) AS character varying);
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        -- 日付変換エラーの場合
        RAISE NOTICE 'SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
