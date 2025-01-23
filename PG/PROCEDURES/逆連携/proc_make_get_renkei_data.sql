CREATE OR REPLACE PROCEDURE proc_make_get_renkei_data (in_n_renkei_data_cd IN NUMERIC, INOUT inout_getdata of_renkei_data)
AS $$
    /**********************************************************************************************************************/
    /* 処理概要 : 逆連携データ設定情報取得処理                                                                         　　   */
    /* 引数　　 : IN_N_RENKEI_DATA_CD                  … 逆連携データコード                               　　               */
    /* 戻り値　 : INOUT_GETDATA                        … 逆連携データ設定情報                                 　　           */
    /*--------------------------------------------------------------------------------------------------------------------*/
    /* 履歴　　 : 新規作成　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　                             */
    /**********************************************************************************************************************/

DECLARE

    LN_COUNT INT DEFAULT 0;

    cur_src CURSOR FOR
    SELECT * 
      FROM of_renkei_data
     WHERE renkei_data_cd = in_n_renkei_data_cd;

    src_data                       of_renkei_data%ROWTYPE;
    
BEGIN
    BEGIN
        OPEN cur_src;
        LOOP

            FETCH cur_src INTO src_data;
            EXIT WHEN NOT FOUND;

            ln_count := ln_count + 1;

            inout_getdata.diff_data_kbn := src_data.diff_data_kbn;
            inout_getdata.last_exec_datetime := src_data.last_exec_datetime;
            inout_getdata.renkei_data := src_data.renkei_data;
            inout_getdata.koza_bunno_kaishi_add_nissu := src_data.koza_bunno_kaishi_add_nissu;
            inout_getdata.koza_bunno_kaishi_eigyo_flg := src_data.koza_bunno_kaishi_eigyo_flg;
            inout_getdata.koza_bunno_kaishi_eigyo_kbn := src_data.koza_bunno_kaishi_eigyo_kbn;
            inout_getdata.koza_bunno_shuryo_add_nissu := src_data.koza_bunno_shuryo_add_nissu;
            inout_getdata.koza_bunno_shuryo_eigyo_flg := src_data.koza_bunno_shuryo_eigyo_flg;
            inout_getdata.koza_bunno_shuryoi_eigyo_kbn := src_data.koza_bunno_shuryoi_eigyo_kbn;

            EXIT;

        END LOOP;

        CLOSE cur_src;

        IF (ln_count = 0) THEN
            RAISE EXCEPTION '%',  '逆連携データコード：' || in_n_renkei_data_cd || ' はOF_RENKEI_DATAテーブルに存在しません。';
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'SQLSTATE : %  SQLERRM : %', SQLSTATE, SQLERRM;
    END;

END;
$$ LANGUAGE plpgsql;
