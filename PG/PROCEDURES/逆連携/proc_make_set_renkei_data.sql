CREATE OR REPLACE PROCEDURE proc_make_set_renkei_data ( in_n_renkei_data_cd IN NUMERIC, in_d_last_exec_datetime IN TIMESTAMP(0) )
AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 逆連携データ設定情報更新処理(最終実行日時の更新)                                                              */
 /* 引数　　 : in_n_renkei_data_cd                … 逆連携データコード                                                    */
 /*          : in_d_last_exec_datetime            … 前回処理日時                                                         */
 /* 戻り値　 :                                                                                                           */
 /*---------------------------------------------------------------------------------------------------------------------*/
 /* 履歴　　 : 新規作成　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　　*/
 /**********************************************************************************************************************/

DECLARE
v_sqlrowcount INT;

BEGIN
    BEGIN
        UPDATE of_renkei_data
           SET last_exec_datetime   = in_d_last_exec_datetime
         WHERE renkei_data_cd = in_n_renkei_data_cd;

        GET DIAGNOSTICS v_sqlrowcount = ROW_COUNT;
        IF v_sqlrowcount = 0 THEN
            RAISE EXCEPTION '%',  '逆連携データコード：' || in_n_renkei_data_cd || ' はOF_RENKEI_DATAテーブルに存在しません。';
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
        RAISE NOTICE 'SQLSTATE : %  SQLERRM : %', SQLSTATE, SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;
