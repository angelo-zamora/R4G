CREATE OR REPLACE PROCEDURE sp_mino_upd(IN in_ln_para02 numeric, INOUT rec_main f_taino)
LANGUAGE plpgsql
AS $sp_mino_upd$

DECLARE
   rec_f_taino                    dlgmain.f_taino%ROWTYPE;

	cur_shuno CURSOR FOR
   SELECT kibetsu_key
        , SUM( zeigaku_shuno ) zeigaku_shuno
        , SUM( tokusoku_shuno ) tokusoku_shuno
        , SUM( entaikin_shuno ) entaikin_shuno
        , MAX( f_shuno_jiko_kisan.shuno_ymd ) last_shuno_ymd
        , SUM( CASE f_shuno.shuno_ymd 
						WHEN f_shuno_jiko_kisan.shuno_ymd THEN zeigaku_shuno + tokusoku_shuno + entaikin_shuno 
						ELSE 0
					END
		   ) last_shuno_kingaku
   FROM f_shuno
      LEFT JOIN ( SELECT MAX(CASE t_nofu_shurui.jiko_kisan_flg WHEN 1 THEN shuno_ymd ELSE 0 END) shuno_ymd
          FROM f_shuno
			 LEFT JOIN t_nofu_shurui ON f_shuno.nofu_shurui_cd = t_nofu_shurui.nofu_shurui_cd
          WHERE f_shuno.kibetsu_key = rec_main.kibetsu_key ) f_shuno_jiko_kisan
		ON f_shuno.shuno_ymd = f_shuno_jiko_kisan.shuno_ymd
   WHERE f_shuno.kibetsu_key = rec_main.kibetsu_key
   GROUP BY f_shuno.kibetsu_key;

   rec_shuno                      f_shuno%ROWTYPE;

	cur_lock CURSOR FOR
   SELECT kibetsu_key
   FROM f_taino
   WHERE kibetsu_key = rec_main.kibetsu_key
   FOR UPDATE;

   rec_lock                       f_taino%ROWTYPE;

BEGIN

   rec_shuno.zeigaku_shuno := 0;
   rec_shuno.tokusoku_shuno := 0;
   rec_shuno.entaikin_shuno := 0;
   rec_shuno.last_shuno_ymd := 0;
   rec_shuno.last_shuno_kingaku := 0;

-- 2015/04/11 S
   -- rec_shuno 処理中
   rec_shuno := NULL;
-- 2015/04/11 E

   OPEN cur_shuno;
      FETCH cur_shuno INTO rec_shuno;
   CLOSE cur_shuno;

   IF in_ln_para02 = 0 THEN
      rec_f_taino.zeigaku_shuno        := rec_main.zeigaku_shuno;
      rec_f_taino.tokusoku_shuno       := rec_main.tokusoku_shuno;
      rec_f_taino.entaikin_shuno       := rec_main.entaikin_shuno;
      rec_f_taino.saishu_shuno_ymd     := rec_shuno.last_shuno_ymd;
      rec_f_taino.saishu_shuno_kingaku := rec_shuno.last_shuno_kingaku;
      rec_f_taino.kanno_cd             := get_kanno_cd( rec_main.zeigaku, rec_main.tokusoku, rec_main.entaikin, rec_main.zeigaku_shuno, rec_main.tokusoku_shuno, rec_main.entaikin_shuno );
--         rec_f_taino.kanno_ymd            := CASE rec_f_taino.kanno_cd WHEN 4 THEN rec_f_taino.saishu_shuno_ymd ELSE 0 END;
      rec_f_taino.zeigaku_mino         := rec_main.zeigaku - rec_main.zeigaku_shuno;
      rec_f_taino.tokusoku_mino        := rec_main.tokusoku - rec_main.tokusoku_shuno;
      rec_f_taino.entaikin_mino        := rec_main.entaikin - rec_main.entaikin_shuno;
      rec_f_taino.upd_datetime         := CURRENT_TIMESTAMP(0);
      rec_f_taino.upd_tantosha_cd      := 'MINOUPD';
      rec_f_taino.upd_tammatsu         := 'SERVER';
   ELSE
      rec_f_taino.zeigaku_shuno        := COALESCE( rec_shuno.zeigaku_shuno, 0 );
      rec_f_taino.tokusoku_shuno       := COALESCE( rec_shuno.tokusoku_shuno, 0 );
      rec_f_taino.entaikin_shuno       := COALESCE( rec_shuno.entaikin_shuno, 0 );
      rec_f_taino.saishu_shuno_ymd     := COALESCE( rec_shuno.last_shuno_ymd, 0 );
      rec_f_taino.saishu_shuno_kingaku := COALESCE( rec_shuno.last_shuno_kingaku, 0 );
      rec_f_taino.kanno_cd             := get_kanno_cd( rec_main.zeigaku, rec_main.tokusoku, rec_main.entaikin, COALESCE( rec_shuno.zeigaku_shuno, 0 ), COALESCE( rec_shuno.tokusoku_shuno, 0 ), COALESCE( rec_shuno.entaikin_shuno, 0 ) );
--         rec_f_taino.kanno_ymd            := CASE rec_f_taino.kanno_cd WHEN 4 THEN rec_f_taino.saishu_shuno_ymd ELSE 0 END;
      rec_f_taino.zeigaku_mino         := rec_main.zeigaku - COALESCE( rec_shuno.zeigaku_shuno, 0 );
      rec_f_taino.tokusoku_mino        := rec_main.tokusoku - COALESCE( rec_shuno.tokusoku_shuno, 0 );
      rec_f_taino.entaikin_mino        := rec_main.entaikin - COALESCE( rec_shuno.entaikin_shuno, 0 );
      rec_f_taino.upd_datetime         := CURRENT_TIMESTAMP(0);
      rec_f_taino.upd_tantosha_cd      := 'MINOUPD';
      rec_f_taino.upd_tammatsu         := 'SERVER';
   END IF;

   OPEN cur_lock;
      FETCH cur_lock INTO rec_lock;
   CLOSE cur_lock;

   BEGIN
      UPDATE f_taino
      SET zeigaku_shuno        = COALESCE( rec_f_taino.zeigaku_shuno, 0 )
         , tokusoku_shuno       = COALESCE( rec_f_taino.tokusoku_shuno, 0 )
         , entaikin_shuno       = COALESCE( rec_f_taino.entaikin_shuno, 0 )
         , saishu_shuno_ymd     = COALESCE( rec_f_taino.saishu_shuno_ymd, 0 )
         , saishu_shuno_kingaku = COALESCE( rec_f_taino.saishu_shuno_kingaku, 0 )
         , kanno_cd             = COALESCE( rec_f_taino.kanno_cd, 0 )
         , kanno_ymd            = COALESCE( CASE rec_f_taino.kanno_cd WHEN 4 THEN rec_f_taino.saishu_shuno_ymd ELSE 0 END, 0 )
         , zeigaku_mino         = COALESCE( rec_f_taino.zeigaku_mino, 0 )
         , tokusoku_mino        = COALESCE( rec_f_taino.tokusoku_mino, 0 )
         , entaikin_mino        = COALESCE( rec_f_taino.entaikin_mino, 0 )
         , upd_datetime         = rec_f_taino.upd_datetime
         , upd_tantosha_cd      = rec_f_taino.upd_tantosha_cd
         , upd_tammatsu         = rec_f_taino.upd_tammatsu
      WHERE kibetsu_key = rec_main.kibetsu_key;
   EXCEPTION
      WHEN OTHERS THEN NULL;
		RAISE NOTICE '% : %', SQLSTATE, SQLERRM;
		ROLLBACK;
   END;
EXCEPTION
   WHEN OTHERS THEN NULL;
	RAISE NOTICE '% : %', SQLSTATE, SQLERRM;
END;
$sp_mino_upd$;