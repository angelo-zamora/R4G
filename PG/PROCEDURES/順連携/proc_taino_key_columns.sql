CREATE OR REPLACE PROCEDURE proc_taino_key_columns(IN rec_main i_r4g_shuno, INOUT rec_f_taino f_taino )
LANGUAGE plpgsql
AS $$

DECLARE

   lc_kibetsu                                 character varying;

BEGIN

   rec_f_taino.fuka_nendo                    := rec_main.fuka_nendo::numeric;
   rec_f_taino.nendo_kbn                     := 0;
   rec_f_taino.kankatsu_cd                   := 0;
   rec_f_taino.zeimoku_cd                    := get_r4g_cd(rec_main.zeimoku_cd, '3');
   rec_f_taino.kibetsu_cd                    := CASE WHEN rec_main.kibetsu_cd IS NOT NULL THEN rec_main.kibetsu_cd::numeric ELSE 0 END;

   SELECT kibetsu INTO lc_kibetsu
   FROM t_kibetsu
   WHERE fuka_nendo = rec_f_taino.fuka_nendo
       AND nendo_kbn = rec_f_taino.nendo_kbn
       AND kankatsu_cd = rec_f_taino.kankatsu_cd
       AND zeimoku_cd = rec_f_taino.zeimoku_cd
       AND kibetsu_cd = rec_f_taino.kibetsu_cd;

   rec_f_taino.soto_nendo                    := rec_main.soto_nendo::numeric;
   rec_f_taino.kibetsu                       := lc_kibetsu;
   rec_f_taino.tokucho_shitei_no             := rec_main.tokucho_shitei_no;
   rec_f_taino.kojin_no                      := rec_main.atena_no;
   rec_f_taino.tsuchisho_no                  := rec_main.tsuchisho_no;
   rec_f_taino.jigyo_nendo_no                := rec_main.jigyo_nendo_no::numeric;
   rec_f_taino.shinkoku_rireki_no            := rec_main.shinkoku_rireki_no::numeric;
   rec_f_taino.kibetsu_key                   := get_kibetsu_key(
	                                            rec_f_taino.fuka_nendo
	                                            , rec_f_taino.soto_nendo
	                                            , rec_main.zeimoku_cd
	                                            , rec_f_taino.zeimoku_cd
	                                            , rec_f_taino.kibetsu_cd
	                                            , rec_f_taino.tokucho_shitei_no
	                                            , rec_f_taino.kojin_no
	                                            , rec_f_taino.tsuchisho_no
	                                            , rec_f_taino.jigyo_nendo_no
	                                            , rec_f_taino.shinkoku_rireki_no);
   
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
$$;
