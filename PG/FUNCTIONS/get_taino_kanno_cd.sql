   CREATE OR REPLACE FUNCTION dlgmain.get_taino_kanno_cd(IN rec_f_taino f_taino)
   RETURNS character varying 
   LANGUAGE plpgsql
   AS $get_taino_kanno_cd$
   BEGIN
      RETURN get_kanno_cd(
         rec_f_taino.zeigaku
       , rec_f_taino.tokusoku
       , CASE rec_f_taino.entaikin_kakutei_cd WHEN 1 THEN rec_f_taino.entaikin ELSE 0 END
       , rec_f_taino.zeigaku_shuno
       , rec_f_taino.tokusoku_shuno
       , CASE rec_f_taino.entaikin_kakutei_cd WHEN 1 THEN rec_f_taino.entaikin_shuno ELSE 0 END
       );
		 
   EXCEPTION
      WHEN OTHERS THEN 
		RAISE NOTICE '% === %', SQLSTATE, SQLERRM;
		RETURN NULL;
   END;
   $get_taino_kanno_cd$;
