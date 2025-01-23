--------------------------------------------------------
--  DDL for Procedure proc_kojin_denwa_chofuku_upd
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE proc_kojin_denwa_chofuku_upd  (
    in_n_renkei_data_cd IN numeric, 
    in_n_renkei_seq IN numeric, 
    in_n_shori_ymd IN numeric, 
    io_c_err_code INOUT character varying, 
    io_c_err_text INOUT character varying 
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 個人宛名情報（電話番号）_重複データ更新                                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :                                                                                                         */
/**********************************************************************************************************************/
DECLARE

   ln_upd_count                        numeric;
   ln_err_count                        numeric;
   lc_err_text                         character varying(100);
   ln_result_cd                        numeric DEFAULT 0;
   lc_err_cd                           character varying;
   
BEGIN
    BEGIN
        UPDATE f_denwa
        SET (del_flg, yusen_flg) = (
            SELECT 
                CASE  A.NUM  WHEN 1 THEN  0  ELSE 
                CASE RENKEI_FLG  WHEN 0 THEN  0  ELSE 1  END  END
                ,CASE  A.NUM  WHEN 1 THEN  A.MAX_YUSEN_FLG  ELSE 0  END
            FROM (
                SELECT 
                    busho_cd,
                    kojin_no,
                    remban,
                    renkei_flg,
                    ROW_NUMBER() OVER(PARTITION BY busho_cd, kojin_no, denwa_no ORDER BY CASE WHEN renkei_flg = 0 THEN 0 ELSE 1 END, yusen_flg DESC, remban DESC) AS num,
                    MAX(yusen_flg) OVER(PARTITION BY busho_cd, kojin_no, denwa_no) AS max_yusen_flg
                FROM 
                    f_denwa
                WHERE 
                    del_flg = 0
            ) A
            WHERE 
                f_denwa.busho_cd = A.busho_cd
                AND f_denwa.kojin_no = A.kojin_no
                AND f_denwa.remban = A.remban
        )
        WHERE del_flg = 0;

			ln_upd_count := ln_upd_count + 1;
			lc_err_text := '';
			lc_err_cd := '0';
			ln_result_cd := 2;

		EXCEPTION
		WHEN OTHERS THEN
		ln_err_count := ln_err_count + 1;
		lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
		lc_err_cd := '9';
		ln_result_cd := 9;

        RAISE NOTICE '結果コード: % | エラーコード: % | エラー内容: % ', ln_result_cd, lc_err_cd, lc_err_text;

        END;

EXCEPTION
WHEN OTHERS THEN
    io_c_err_code := SQLSTATE;
    io_c_err_text := SQLERRM;
    RETURN;
END;
$$;
