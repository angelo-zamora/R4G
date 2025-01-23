CREATE OR REPLACE PROCEDURE doitsunin_upd(in_kojin_no IN character varying) 
LANGUAGE plpgsql
AS $doitsunin_upd$

DECLARE
	cur_lock CURSOR FOR
	SELECT kojin_no
	FROM f_kojin
	WHERE kojin_no = in_kojin_no
	FOR UPDATE;

	rec_lock                       f_kojin%ROWTYPE;

	cur_doitsunin_kojin CURSOR FOR
	SELECT *
	FROM f_kojin a
			,(SELECT b.doitsunin_kojin_no
			FROM f_kojin b
			WHERE b.kojin_no = in_kojin_no
			) c
	WHERE a.kojin_no = c.doitsunin_kojin_no;

	rec_doitsunin_kojin            f_kojin%ROWTYPE;

BEGIN
	OPEN cur_lock;
		FETCH cur_lock INTO rec_lock.kojin_no;
	CLOSE cur_lock;
	BEGIN

			rec_doitsunin_kojin := NULL;

			OPEN cur_doitsunin_kojin;
			FETCH cur_doitsunin_kojin INTO rec_doitsunin_kojin;
			CLOSE cur_doitsunin_kojin;

			UPDATE f_kojin
				SET doitsunin_shimei           = rec_doitsunin_kojin.shimei
				, doitsunin_shimei_kana      = rec_doitsunin_kojin.shimei_kana
				, doitsunin_jusho            = rec_doitsunin_kojin.jusho
				, doitsunin_jusho_katagaki   = rec_doitsunin_kojin.jusho_katagaki
				, doitsunin_chiku_cd         = rec_doitsunin_kojin.chiku_cd
				, upd_datetime               = CURRENT_TIMESTAMP(0)
				, upd_tantosha_cd            = 'RENKEI'
				, upd_tammatsu               = 'SERVER'
			WHERE doitsunin_kojin_no         = rec_doitsunin_kojin.kojin_no;

	EXCEPTION
	  WHEN OTHERS THEN NULL;
      RAISE NOTICE '% : % ', SQLSTATE, SQLERRM;
	END;
END;
$doitsunin_upd$;