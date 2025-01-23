CREATE OR REPLACE PROCEDURE noki_torai_upd(IN rec_main f_taino)
LANGUAGE plpgsql
AS $$

DECLARE
   cur_lock CURSOR FOR
   SELECT kibetsu_key
   FROM f_taino
   WHERE kibetsu_key = rec_main.kibetsu_key
   FOR UPDATE;

   rec_lock                       f_taino%ROWTYPE;

   ln_noki_torai_ymd              numeric;
   ln_shobun_kano_ymd             numeric;
   
   rec_get_noki_torai_ymd         rec_noki_torai[];

BEGIN

      rec_get_noki_torai_ymd := get_noki_torai_ymd( rec_main.kibetsu_key, rec_main.noki_ymd, rec_main.tokusoku_ymd
                   , rec_main.tokusoku_koji_ymd, rec_main.tokusoku_noki_ymd, rec_main.noki_kuriage_ymd );
	   ln_noki_torai_ymd := rec_get_noki_torai_ymd[1].noki_torai_ymd;
	   ln_shobun_kano_ymd := rec_get_noki_torai_ymd[1].shobun_kano_ymd;

   OPEN cur_lock;
      FETCH cur_lock INTO rec_lock;
   CLOSE cur_lock;

   UPDATE f_taino
   SET noki_torai_handan_ymd = ln_noki_torai_ymd
      , shobun_kano_ymd = ln_shobun_kano_ymd
   WHERE kibetsu_key = rec_main.kibetsu_key;
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
$$;