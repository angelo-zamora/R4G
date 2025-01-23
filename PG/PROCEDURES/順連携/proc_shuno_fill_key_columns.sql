CREATE OR REPLACE PROCEDURE proc_shuno_fill_key_columns(IN rec_main i_r4g_shuno_rireki, INOUT rec_f_shuno f_shuno)
LANGUAGE plpgsql
AS $$

BEGIN

   rec_f_shuno.fuka_nendo                    := rec_main.fuka_nendo::numeric;
   rec_f_shuno.soto_nendo                    := rec_main.soto_nendo::numeric;
   rec_f_shuno.zeimoku_cd                    := get_r4g_code_conv(1, '3', null, rec_main.zeimoku_cd::character varying);
   rec_f_shuno.kibetsu_cd                    := rec_main.kibetsu_cd::numeric;
   rec_f_shuno.tsuchisho_no                  := rec_main.tsuchisho_no;
   rec_f_shuno.kibetsu_key                   := get_kibetsu_key(rec_f_shuno.fuka_nendo, rec_f_shuno.soto_nendo, rec_main.zeimoku_cd,
                                                rec_f_shuno.zeimoku_cd, rec_f_shuno.kibetsu_cd, rec_main.tokucho_shitei_no, 
                                                rec_main.jido_atena_no, rec_f_shuno.tsuchisho_no, CASE WHEN rec_main.jigyo_nendo_no IS NOT NULL OR rec_main.jigyo_nendo_no <> '' THEN rec_main.jigyo_nendo_no::numeric ELSE 0 END,
                                                rec_main.shinkoku_rireki_no::numeric);
   rec_f_shuno.shuno_keshikomi_key           := rec_main.shuno_rireki_no;
   
END;
$$;