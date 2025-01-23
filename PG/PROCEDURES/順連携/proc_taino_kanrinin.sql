CREATE OR REPLACE PROCEDURE proc_taino_kanrinin(INOUT rec_f_taino f_taino )
LANGUAGE plpgsql
AS $$

DECLARE
   cur_kanrinin CURSOR FOR
   SELECT dairinin_kojin_no
   FROM f_dairinin
   WHERE nozeigimusha_kojin_no   = rec_f_taino.kojin_no
     AND zeimoku_cd = rec_f_taino.zeimoku_cd
     AND dairinin_yukokikan_kaishi_ymd <= to_number(to_char(CURRENT_DATE,'yyyymmdd'), '99999999')
     AND dairinin_yukokikan_shuryo_ymd >= to_number(to_char(CURRENT_DATE,'yyyymmdd'), '99999999')
     AND del_flg = 0;
     
   lc_dairinin_no character varying;
     
BEGIN

   OPEN cur_kanrinin;
      FETCH cur_kanrinin INTO lc_dairinin_no;
      IF NOT FOUND THEN
         rec_f_taino.kanrinin_cd                    := 0;
         rec_f_taino.kanrinin_kojin_no              := LPAD( '0', 15, '0' );
      ELSE
         rec_f_taino.kanrinin_cd                    := 1;
         rec_f_taino.kanrinin_kojin_no              := lc_dairinin_no;
      END IF;
   CLOSE cur_kanrinin;

   EXCEPTION
      WHEN OTHERS THEN NULL;
      
END;
$$;