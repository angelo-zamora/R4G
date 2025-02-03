--------------------------------------------------------
--  DDL for Procedure proc_kojin_sofu_chofuku_upd
--------------------------------------------------------
CREATE OR REPLACE PROCEDURE dlgrenkei.proc_kojin_sofu_chofuku_upd( 
    in_n_renkei_data_cd IN numeric, 
    in_n_renkei_seq IN numeric, 
    in_n_shori_ymd IN numeric, 
    io_c_err_code INOUT character varying, 
    io_c_err_text INOUT character varying 
)

LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 送付先・連絡先情報（統合収滞納）                                                                        */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : 新規作成                                                                                                */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : 2025/02/03 CRESS-INFO.Angelo     新規作成     036o014「送付先・連絡先情報（統合収滞納）」の取込を行う。 */
/**********************************************************************************************************************/
DECLARE

   ln_upd_count                        numeric DEFAULT 0;
   ln_err_count                        numeric DEFAULT 0;
   lc_err_text                         character varying(100);
   ln_result_cd                        numeric DEFAULT 0;
   lc_err_cd                           character varying;

   ln_result_cd_upd                    numeric DEFAULT 2; -- 更新
   ln_result_cd_err                    numeric DEFAULT 9; -- エラー

   lc_err_cd_normal                    character varying = '0'; -- 通常
   lc_err_cd_err                       character varying = '9'; -- エラー

BEGIN
    BEGIN
        UPDATE f_sofu
        SET (del_flg, yusen_flg) = (
            SELECT 
                CASE WHEN a.num = 1 THEN 0 ELSE renkei_flg END,
                CASE WHEN a.num = 1 THEN a.max_yusen_flg ELSE 0 END
            FROM (
                SELECT 
                    busho_cd,
                    kojin_no,
                    remban,
                    renkei_flg,
                    gyomu_cd,
                    ROW_NUMBER() OVER (
                        PARTITION BY busho_cd, kojin_no, 
                                     sofu_yubin_no, 
                                     sofu_jusho, 
                                     sofu_jusho_katagaki,
                                     gyomu_cd,
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
                                     sofu_shimei_kana
                        ORDER BY renkei_flg, yusen_flg DESC, remban DESC
                    ) AS num,
                    MAX(yusen_flg) OVER (
                        PARTITION BY busho_cd, kojin_no, 
                                     sofu_yubin_no, 
                                     sofu_jusho, 
                                     sofu_jusho_katagaki, 
                                     sofu_shimei,
                                     gyomu_cd,
                                     shikuchoson_cd, 
                                     machiaza_cd, 
                                     todofuken, 
                                     shikugunchoson, 
                                     machiaza,
                                     banchigohyoki, 
                                     kokumei_cd, 
                                     kokumeito, 
                                     kokugai_jusho, 
                                     sofu_shimei_kana
                    ) AS max_yusen_flg
                FROM f_sofu
                WHERE del_flg = 0
            ) a
            WHERE f_sofu.busho_cd = a.busho_cd
            AND f_sofu.kojin_no = a.kojin_no
            AND f_sofu.gyomu_cd = a.gyomu_cd
            AND f_sofu.remban = a.remban
        )
        WHERE del_flg = 0;

        ln_upd_count := ln_upd_count + 1;
        lc_err_text := '';
        lc_err_cd := lc_err_cd_normal;
        ln_result_cd := ln_result_cd_upd;

    EXCEPTION
        WHEN OTHERS THEN
            ln_err_count := ln_err_count + 1;
            lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
            lc_err_cd := lc_err_cd_err;
            ln_result_cd := ln_result_cd_err;
    END;

EXCEPTION
    WHEN OTHERS THEN
        io_c_err_code := SQLSTATE;
        io_c_err_text := SQLERRM;
        RETURN;
END;
$$;
