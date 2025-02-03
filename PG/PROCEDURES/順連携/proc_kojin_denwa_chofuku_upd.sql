--------------------------------------------------------
--  DDL for Procedure proc_kojin_denwa_chofuku_upd
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE dlgrenkei.proc_kojin_denwa_chofuku_upd  (
    in_n_renkei_data_cd IN numeric, 
    in_n_renkei_seq IN numeric, 
    in_n_shori_ymd IN numeric, 
    io_c_err_code INOUT character varying, 
    io_c_err_text INOUT character varying 
)
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 送付先・連絡先情報（統合収滞納）                                                                           */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/02/03  CRESS-INFO.Drexler     新規作成     036o014「送付先・連絡先情報（統合収滞納）」の取込を行う      */
/**********************************************************************************************************************/
DECLARE

   ln_upd_count                        numeric;
   ln_err_count                        numeric;
   lc_err_text                         character varying(100);
   ln_result_cd                        numeric DEFAULT 0;
   lc_err_cd                           character varying;
   
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
        
EXCEPTION
WHEN OTHERS THEN
    io_c_err_code := SQLSTATE;
    io_c_err_text := SQLERRM;
    RETURN;
END;
$$;
