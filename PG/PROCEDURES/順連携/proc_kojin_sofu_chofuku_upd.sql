--------------------------------------------------------
--  DDL for Procedure proc_kojin_sofu_chofuku_upd
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE proc_kojin_sofu_chofuku_upd( 
    in_n_renkei_data_cd IN numeric, 
    in_n_renkei_seq IN numeric, 
    in_n_shori_ymd IN numeric, 
    io_c_err_code INOUT character varying, 
    io_c_err_text INOUT character varying 
)

LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 個人宛名情報（送付先）_重複データ更新                                                                       */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : 新規作成                                                                                                  */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                    */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                  */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                */
/*      OUT : io_c_err_code     … 例外エラー発生時のエラーコード                                                         */
/*            io_c_err_text      … 例外エラー発生時のエラー内容                                                          */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/
DECLARE

   ln_upd_count                        numeric;
   ln_err_count                        numeric;
   lc_err_text                         character varying(100);
   ln_result_cd                        numeric DEFAULT 0;
   lc_err_cd                           character varying;

BEGIN
    BEGIN
        UPDATE f_sofu
        SET (del_flg, yusen_flg) = (
            SELECT 
                CASE WHEN A.num = 1 THEN 0 ELSE renkei_flg END,
                CASE WHEN A.num = 1 THEN A.max_yusen_flg ELSE 0 END
            FROM (
                SELECT 
                    busho_cd,
                    kojin_no,
                    remban,
                    renkei_flg,
                    ROW_NUMBER() OVER (
                        PARTITION BY busho_cd, kojin_no, 
                                     sofu_yubin_no, 
                                     sofu_jusho, 
                                     sofu_jusho_katagaki, 
                                     sofu_shimei,
                                     shikuchoson_cd, 
                                     machiaza_cd, 
                                     todofuken, 
                                     shikugunchoson, 
                                     machiaza,
                                     banchigohyoki, 
                                     kokumei_cd, 
                                     kokumeito, 
                                     kokugai_jusho, 
                                     shimei_kana
                        ORDER BY renkei_flg, yusen_flg DESC, remban DESC
                    ) AS num,
                    MAX(yusen_flg) OVER (
                        PARTITION BY busho_cd, kojin_no, 
                                     sofu_yubin_no, 
                                     sofu_jusho, 
                                     sofu_jusho_katagaki, 
                                     sofu_shimei,
                                     shikuchoson_cd, 
                                     machiaza_cd, 
                                     todofuken, 
                                     shikugunchoson, 
                                     machiaza,
                                     banchigohyoki, 
                                     kokumei_cd, 
                                     kokumeito, 
                                     kokugai_jusho, 
                                     shimei_kana
                    ) AS max_yusen_flg
                FROM f_sofu
                WHERE del_flg = 0
            ) A
            WHERE f_sofu.busho_cd = A.busho_cd
            AND f_sofu.kojin_no = A.kojin_no
            AND f_sofu.gyomu_cd = A.gyomu_cd
            AND f_sofu.remban = A.remban
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
    END;

EXCEPTION
    WHEN OTHERS THEN
        io_c_err_code := SQLSTATE;
        io_c_err_text := SQLERRM;
        RETURN;
END;
$$;
