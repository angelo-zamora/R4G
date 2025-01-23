CREATE OR REPLACE PROCEDURE proc_make_add_renkei_log ( in_n_renkei_data_cd IN NUMERIC
                                                            ,in_c_renkei_data IN character varying
                                                            ,in_d_kaishi_datetime IN TIMESTAMP(0)
                                                            ,in_d_shuryo_datetime IN TIMESTAMP(0)
                                                            ,in_n_data_count IN NUMERIC)
AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : 逆連携ログ追加処理                                                                                        */
 /* 引数　　 : in_n_renkei_data_cd                … 逆連携データコード                                                   */
 /*          : in_c_renkei_data                   … 処理名                                                             */
 /*          : in_d_kaishi_datetime               … 処理開始日時                                                        */
 /*          : in_d_shuryo_datetime               … 処理終了日時                                                        */
 /*          : in_n_data_count                    … 処理件数                                                            */
 /* 戻り値　 :                                                                                                          */
 /*--------------------------------------------------------------------------------------------------------------------*/
 /* 履歴　　 : 新規作成                                                                                                 */
 /*                                                                                                                    */
 /**********************************************************************************************************************/

DECLARE
v_sqlrowcount INT;

BEGIN
    BEGIN
        INSERT INTO of_renkei_log( sequence_no_out_log
                                            ,renkei_data_cd
                                            ,renkei_data
                                            ,kaishi_datetime
                                            ,shuryo_datetime
                                            ,data_count
                                            ,out_data_count)
        VALUES( (SELECT nextval('SEQ_O_OUT_LOG'))
                ,in_n_renkei_data_cd
                ,in_c_renkei_data    
                ,in_d_kaishi_datetime
                ,in_d_shuryo_datetime
                ,in_n_data_count
                ,NULL);

        GET DIAGNOSTICS v_sqlrowcount = ROW_COUNT;
        IF v_sqlrowcount = 0 THEN
            RAISE EXCEPTION 'OF_RENKEI_LOGテーブルにデータを追加できません。';
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
        RAISE NOTICE 'SQLSTATE : %  SQLERRM : %', SQLSTATE, SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;
