--------------------------------------------------------
--  DDL for Procedure proc_r4g_kojin_doitsunin_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_kojin_doitsunin_upd ( in_n_para12 IN numeric, io_c_err_code INOUT character varying, 
    io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 個人宛名情報連携（同一人情報更新処理）                                                                      */
/* 引数　　 : in_n_para12          … proc_kojin.パラメータ(12)                                                         */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/01/23  CRESS-INFO.Angelo     新規作成     001o006「住民情報（個人番号あり）」の取込を行う               */
/**********************************************************************************************************************/

DECLARE
   rec_f_kojin                    dlgmain.f_kojin%ROWTYPE;

   ln_shori_count                 smallint DEFAULT 0;

   cur_main CURSOR FOR
   SELECT LPAD( i_r4g_atena.atena_no::text, 15, '0' ) as atena_no
   FROM dlgrenkei.i_r4g_atena
   WHERE result_cd = 2;

   cur_main_all CURSOR FOR
   SELECT kojin_no
   FROM f_kojin;
	
	rec_main                       dlgrenkei.i_r4g_atena%ROWTYPE;

BEGIN

   IF in_n_para12 = 0 THEN
   -- 異動分のみ処理
      OPEN cur_main;
      LOOP
          FETCH cur_main INTO rec_main.atena_no;
          EXIT WHEN NOT FOUND;

          ln_shori_count := ln_shori_count + 1;

          CALL doitsunin_upd(rec_main.atena_no);

      END LOOP;

      CLOSE cur_main;

   ELSE
   -- 全件処理
      OPEN cur_main_all;
      LOOP
          FETCH cur_main_all INTO rec_main.kojin_no;
          EXIT WHEN NOT FOUND;

          ln_shori_count := ln_shori_count + 1;

          CALL doitsunin_upd(rec_main.kojin_no);

      END LOOP;

      CLOSE cur_main_all;
   END IF;

EXCEPTION
WHEN OTHERS THEN
    io_c_err_code := SQLSTATE;
    io_c_err_text := SQLERRM;
    RETURN;
END;
$$;