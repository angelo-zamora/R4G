CREATE OR REPLACE PROCEDURE dlgrenkei.jiko_upd(IN rec_main f_taino)
LANGUAGE plpgsql
AS $jiko_upd$
/**********************************************************************************************************************/
/* 機能概要 : 時効予定日更新のサブ処理                                                                                   */
/* 入力 IN  : rec_main  メインカーソル                                                                                  */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     : 2025/02/03  CRESS-INFO.Drexler     新規作成     036o014「送付先・連絡先情報（統合収滞納）」の取込を行う      */
/**********************************************************************************************************************/
DECLARE
   cur_lock CURSOR FOR
   SELECT kibetsu_key
   FROM f_taino
   WHERE kibetsu_key = rec_main.kibetsu_key
   FOR UPDATE;

   rec_lock                       f_taino%ROWTYPE;
	
   ln_jiko_ymd                    NUMERIC DEFAULT 0;
   ln_shometsu_ymd                NUMERIC DEFAULT 0;
   ln_jiko_kisan_ymd              NUMERIC DEFAULT 0;
   
   rec_get_jiko_handan            rec_jiko[];

BEGIN

      rec_get_jiko_handan := get_jiko_handan( rec_main.kibetsu_key, getdatetonum( CURRENT_TIMESTAMP(0)::date ) );
      ln_jiko_ymd := rec_get_jiko_handan[1].jiko_ymd;
	   ln_shometsu_ymd := rec_get_jiko_handan[1].shometsu_ymd;
	   ln_jiko_kisan_ymd := rec_get_jiko_handan[1].jiko_kisan_ymd;

   OPEN cur_lock;
      FETCH cur_lock INTO rec_lock.kibetsu_key;
   CLOSE cur_lock;
   
   UPDATE f_taino
   SET jiko_yotei_ymd = ln_jiko_ymd
      , shometsu_yotei_ymd = ln_shometsu_ymd
      , yobi_komoku3 = ln_jiko_kisan_ymd
      , upd_tantosha_cd = 'jiko_upd'
      , upd_datetime = CURRENT_TIMESTAMP(0)
   WHERE kibetsu_key = rec_main.kibetsu_key;

EXCEPTION
   WHEN OTHERS THEN NULL;
END;
$jiko_upd$;