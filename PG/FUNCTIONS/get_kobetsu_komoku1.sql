--------------------------------------------------------
--  DDL for Function get_kobetsu_komoku1
--------------------------------------------------------

CREATE OR REPLACE FUNCTION get_kobetsu_komoku1 (rec_main i_r4g_shuno)
RETURNS character varying
LANGUAGE plpgsql
AS $$

DECLARE

   lc_kobetsu_komoku1                 character varying;

BEGIN

   IF rec_main.zeimoku_cd = '02' THEN
      BEGIN
         lc_kobetsu_komoku1 := rec_main.tokucho_shitei_no;
      END;
   ELSIF rec_main.zeimoku_cd = '08' THEN
      BEGIN
         lc_kobetsu_komoku1 := CONCAT(rec_main.sharyo_no1,rec_main.sharyo_no2,rec_main.sharyo_no3,rec_main.sharyo_no4);
      END;
   ELSIF rec_main.zeimoku_cd IN ('21','22','27','28') THEN
      BEGIN
         lc_kobetsu_komoku1 := rec_main.hihokensha_no;
      END;
   ELSIF rec_main.zeimoku_cd IN ('23','24','25','26') THEN
      BEGIN
         lc_kobetsu_komoku1 := rec_main.kokuhokigo_no;
      END;
   ELSIF rec_main.zeimoku_cd IN ('A1','A2','A3','A4','A5','A6','A7','A8','A9','AA',
   'B1','B2','B3','B4','B5','B6','B7','B8','B9','BA','BB','C1','C2','C3','C4','C5','C6','C7','D1','D2') THEN
      BEGIN
         lc_kobetsu_komoku1 := rec_main.jido_atena_no;
      END;
   END IF;

   RETURN lc_kobetsu_komoku1;

EXCEPTION
   WHEN OTHERS THEN
      RETURN NULL;
END;
$$;