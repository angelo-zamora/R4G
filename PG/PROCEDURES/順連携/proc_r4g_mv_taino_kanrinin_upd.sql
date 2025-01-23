--------------------------------------------------------
--  DDL for Procedure  proc_r4g_mv_taino_kanrinin_upd
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_r4g_mv_taino_kanrinin_upd ( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, io_c_err_code INOUT character varying, io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : mv_滞納_管理人のデータ更新                                                                                 */
/* 引数 IN  : in_n_renkei_data_cd  … 連携データコード                                                                   */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                                   */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                                 */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                       */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                         */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :                                                                                                     */
/**********************************************************************************************************************/

DECLARE

    rec_log                        f_renkei_log%ROWTYPE;

    ln_shori_count                 numeric DEFAULT 0;
    ln_ins_count                   numeric DEFAULT 0;
    ln_upd_count                   numeric DEFAULT 0;
    ln_del_count                   numeric DEFAULT 0;
    ln_err_count                   numeric DEFAULT 0;
    lc_err_cd                      character varying;
    lc_err_text                    character varying(100);
    ln_result_cd                   numeric DEFAULT 0;
    ln_kanri_count                 numeric DEFAULT 0;
    lc_kanrinin_kojin_no           character varying;

    lc_sql                         character varying(1000);

    cur_main CURSOR FOR
    SELECT *
    FROM f_taino
    WHERE kanrinin_cd > 0 
    AND del_flg = 0;

    rec_main                                   f_taino%ROWTYPE;
    rec_mv_taino_kanrinin                      mv_taino_kanrinin%ROWTYPE;

BEGIN
    lc_sql := '';

    rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

    -- mv_taino_kanrininに関係するインデックスを削除する
    BEGIN
    lc_sql := 'DROP INDEX IF EXISTS mv_taino_kanrinin_idx01';
      EXECUTE lc_sql;
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;

    BEGIN
    lc_sql := 'DROP INDEX IF EXISTS mv_taino_kanrinin_idx02';
      EXECUTE lc_sql;
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;

    BEGIN
    lc_sql := 'DROP INDEX IF EXISTS mv_taino_kanrinin_idx03';
      EXECUTE lc_sql;
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;

    -- インデックス再作成
   BEGIN
      LC_SQL := 'CREATE INDEX DLGMAIN.f_taino_idx991 ';
      LC_SQL := LC_SQL || ' ON DLGMAIN.MV_TAINO_KANRININ ';
      LC_SQL := LC_SQL || ' (KANRININ_KOJIN_NO, DEL_FLG) ';
      LC_SQL := LC_SQL || ' PCTFREE 10 ';
      LC_SQL := LC_SQL || ' INITRANS 2 ';
      LC_SQL := LC_SQL || ' MAXTRANS 255 ';
      LC_SQL := LC_SQL || ' TABLESPACE INDX ';
      LC_SQL := LC_SQL || ' STORAGE(INITIAL 64K NEXT 1M MINEXTENTS 1 MAXEXTENTS 2147483645 BUFFER_POOL DEFAULT) ';
      LC_SQL := LC_SQL || ' LOGGING ';
      LC_SQL := LC_SQL || '';
      EXECUTE IMMEDIATE LC_SQL;
   EXCEPTION
      WHEN OTHERS THEN NULL;
   END;

    -- dlgmain：mv_滞納_管理人（mv_taino_kanrinin）をTRUNCATEする。
    BEGIN
        SELECT COUNT(*) INTO ln_del_count FROM mv_taino_kanrinin;
        lc_sql := 'TRUNCATE TABLE dlgmain.mv_taino_kanrinin';
        EXECUTE lc_sql;
    EXCEPTION
        WHEN OTHERS THEN
        io_c_err_code := SQLSTATE;
        io_c_err_text := SQLERRM;
		RETURN;
      END;

   -- ５．連携データの作成・更新
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

        ln_shori_count := ln_shori_count + 1;

        --f_dairininに該当データが存在する場合
        SELECT COUNT(*), dairinin_kojin_no INTO ln_kanri_count, lc_kanrinin_kojin_no FROM f_dairinin WHERE dairinin_kojin_no = rec_main.kanrinin_cd;

         rec_mv_taino_kanrinin := ROW(
		 rec_main.kibetsu_key
         ,rec_main.fuka_nendo
         ,rec_main.soto_nendo
         ,rec_main.zeimoku_cd
         ,rec_main.kibetsu_cd
         ,rec_main.kibetsu
         ,rec_main.kojin_no
         ,rec_main.tsuchisho_no
         ,rec_main.jigyo_kaishi_ymd
         ,rec_main.jigyo_shuryo_ymd
         ,rec_main.shinkoku_cd
         ,rec_main.shusei_kaisu
         ,rec_main.nendo_kbn
         ,rec_main.kankatsu_cd
         ,rec_main.kasankin_cd
         ,rec_main.tokucho_shitei_no
         ,rec_main.shinkoku_rireki_no
         ,rec_main.jigyo_nendo_no
         ,rec_main.jido_kojin_no
         ,rec_main.noki_ymd
         ,rec_main.noki_kuriage_ymd
         ,rec_main.shitei_noki_ymd
         ,rec_main.tokusoku_ymd
         ,rec_main.tokusoku_henrei_ymd
         ,rec_main.tokusoku_koji_ymd
         ,rec_main.tokusoku_noki_ymd
         ,rec_main.saikoku_ymd
         ,rec_main.saikoku_noki_ymd
         ,rec_main.hotei_noki_to_ymd
         ,rec_main.hotei_noki_ymd
         ,rec_main.kisan_ymd
         ,rec_main.kazei_kosei_ymd
         ,rec_main.kosei_jiyu_cd
         ,rec_main.shinkoku_ymd
         ,rec_main.shusei_shinkoku_ymd
         ,rec_main.kakutei_shinkoku_ymd
         ,rec_main.kosei_kettei_tsuchi_ymd
         ,rec_main.encho_tsuki
         ,rec_main.shinkoku_kigen_ymd
         ,rec_main.encho_kigen_ymd
         ,rec_main.kosei_seikyu_ymd
         ,rec_main.kokuzei_shinkoku_kiso_kbn
         ,rec_main.kokuzei_shinkoku_ymd
         ,rec_main.kosei_shinkoku_ymd
         ,rec_main.jiko_yotei_ymd
         ,rec_main.shometsu_yotei_ymd
         ,rec_main.zeigaku
         ,rec_main.tokusoku
         ,rec_main.entaikin
         ,rec_main.entaikin_kakutei_cd
         ,rec_main.entaikin_kyosei_kbn
         ,rec_main.entaikin_kyosei_ymd
         ,rec_main.zeigaku_kintowari
         ,rec_main.zeigaku_hojinwari
         ,rec_main.zeigaku_iryo_ippan
         ,rec_main.zeigaku_iryo_taisyoku
         ,rec_main.zeigaku_kaigo_ippan
         ,rec_main.zeigaku_kaigo_taisyoku
         ,rec_main.zeigaku_shien_ippan
         ,rec_main.zeigaku_shien_taisyoku
         ,rec_main.zeigaku_shuno
         ,rec_main.tokusoku_shuno
         ,rec_main.entaikin_shuno
         ,rec_main.zeigaku_kintowari_shuno
         ,rec_main.zeigaku_hojinwari_shuno
         ,rec_main.saishu_nikkei_ymd
         ,rec_main.saishu_shuno_ymd
         ,rec_main.saishu_shuno_kingaku
         ,rec_main.kanno_cd
         ,rec_main.kanno_ymd
         ,rec_main.zeigaku_mino
         ,rec_main.tokusoku_mino
         ,rec_main.entaikin_mino
         ,rec_main.shotokuwari
         ,rec_main.fukakachiwari
         ,rec_main.shihonwari
         ,rec_main.shunyuwari
         ,rec_main.tosho_kazeigaku
         ,rec_main.jukasankin_taisho_zeigaku
         ,CASE WHEN ln_kanri_count = 0 THEN 0  ELSE rec_main.kanrinin_cd END
         ,CASE WHEN lc_kanrinin_kojin_no IS NULL OR lc_kanrinin_kojin_no = '' THEN LPAD('0', 15, '0')  ELSE lc_kanrinin_kojin_no END
         ,rec_main.shobun_kano_ymd
         ,rec_main.noki_torai_handan_ymd
         ,rec_main.kaikei_nendo
         ,rec_main.kobetsu_komoku1
         ,rec_main.kobetsu_komoku2		
         ,rec_main.kobetsu_komoku3		
         ,rec_main.yobi_komoku1			
         ,rec_main.yobi_komoku2			
         ,rec_main.yobi_komoku3			
         ,''
         ,rec_main.hihokensha_no			
         ,rec_main.kokuhokigo_no			
         ,rec_main.kyoyu_shisan_no		
         ,rec_main.shizei_jimusho_cd		
         ,rec_main.tsuchi_ymd				
         ,rec_main.koseiin_tokusoku_flg	
         ,rec_main.zeigaku_kotei_tochikaoku
         ,rec_main.zeigaku_kotei_shokyaku	
         ,rec_main.zeigaku_shinrin		
         ,rec_main.shotokuwari_kojo		
         ,rec_main.shotokuwari_kanpu		
         ,rec_main.kojo_fusoku			
         ,rec_main.kojo_fusoku_nofu		
         ,rec_main.noki_tokurei_flg		
         ,rec_main.noki_tokurei_ym		
         ,rec_main.kazei_kbn				
         ,rec_main.keiji_kanri_no			
         ,rec_main.shadai_no				
         ,rec_main.keiji_shubetsu_cd		
         ,rec_main.sharyo_no1				
         ,rec_main.sharyo_no2				
         ,rec_main.sharyo_no3				
         ,rec_main.sharyo_no4				
         ,rec_main.shomeisho_yuko_kigen	
         ,rec_main.jukazei_flg			
         ,rec_main.kesson_ymd				
         ,rec_main.kesson_jiyu_cd			
         ,rec_main.zeigaku_kesson			
         ,rec_main.entaikin_kesson		
         ,rec_main.tokusoku_kesson		
         ,rec_main.kodomo_jigyosho_no		
         ,rec_main.ins_datetime			
         ,rec_main.upd_datetime			
         ,rec_main.upd_tantosha_cd		
         ,rec_main.upd_tammatsu			
         ,rec_main.del_flg
         );

          BEGIN
                INSERT INTO mv_taino_kanrinin VALUES (rec_mv_taino_kanrinin.*);
                ln_ins_count := ln_ins_count + 1;
                EXCEPTION
                        WHEN OTHERS THEN
                            io_c_err_code := SQLSTATE;
                            io_c_err_text := SQLERRM;
                            RETURN;
                END;
      END LOOP;
      
   CLOSE cur_main;

    -- f_taino_idx991をDROPする
    BEGIN
    lc_sql := 'DROP INDEX IF EXISTS f_taino_idx991';
      EXECUTE lc_sql;
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;

    -- mv_taino_kanrininに関係するインデックスを再作成する
    BEGIN
        LC_SQL := 'CREATE INDEX DLGMAIN.mv_taino_kanrinin_idx01 ';
        LC_SQL := LC_SQL || ' ON DLGMAIN.MV_TAINO_KANRININ ';
        LC_SQL := LC_SQL || ' (KANRININ_KOJIN_NO, DEL_FLG) ';
        LC_SQL := LC_SQL || ' PCTFREE 10 ';
        LC_SQL := LC_SQL || ' INITRANS 2 ';
        LC_SQL := LC_SQL || ' MAXTRANS 255 ';
        LC_SQL := LC_SQL || ' TABLESPACE INDX ';
        LC_SQL := LC_SQL || ' STORAGE(INITIAL 64K NEXT 1M MINEXTENTS 1 MAXEXTENTS 2147483645 BUFFER_POOL DEFAULT) ';
        LC_SQL := LC_SQL || ' LOGGING ';
        LC_SQL := LC_SQL || '';
        EXECUTE IMMEDIATE LC_SQL;
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;

    BEGIN
        LC_SQL := 'CREATE INDEX DLGMAIN.mv_taino_kanrinin_idx02 ';
        LC_SQL := LC_SQL || ' ON DLGMAIN.MV_TAINO_KANRININ ';
        LC_SQL := LC_SQL || ' (KANRININ_KOJIN_NO, DEL_FLG) ';
        LC_SQL := LC_SQL || ' PCTFREE 10 ';
        LC_SQL := LC_SQL || ' INITRANS 2 ';
        LC_SQL := LC_SQL || ' MAXTRANS 255 ';
        LC_SQL := LC_SQL || ' TABLESPACE INDX ';
        LC_SQL := LC_SQL || ' STORAGE(INITIAL 64K NEXT 1M MINEXTENTS 1 MAXEXTENTS 2147483645 BUFFER_POOL DEFAULT) ';
        LC_SQL := LC_SQL || ' LOGGING ';
        LC_SQL := LC_SQL || '';
        EXECUTE IMMEDIATE LC_SQL;
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;

    BEGIN
        LC_SQL := 'CREATE INDEX DLGMAIN.mv_taino_kanrinin_idx03 ';
        LC_SQL := LC_SQL || ' ON DLGMAIN.MV_TAINO_KANRININ ';
        LC_SQL := LC_SQL || ' (KANRININ_KOJIN_NO, DEL_FLG) ';
        LC_SQL := LC_SQL || ' PCTFREE 10 ';
        LC_SQL := LC_SQL || ' INITRANS 2 ';
        LC_SQL := LC_SQL || ' MAXTRANS 255 ';
        LC_SQL := LC_SQL || ' TABLESPACE INDX ';
        LC_SQL := LC_SQL || ' STORAGE(INITIAL 64K NEXT 1M MINEXTENTS 1 MAXEXTENTS 2147483645 BUFFER_POOL DEFAULT) ';
        LC_SQL := LC_SQL || ' LOGGING ';
        LC_SQL := LC_SQL || '';
        EXECUTE IMMEDIATE LC_SQL;
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;

    rec_log.seq_no_renkei := in_n_renkei_seq;
    rec_log.proc_shuryo_datetime := CURRENT_TIMESTAMP;
    rec_log.proc_shori_count := ln_shori_count;
    rec_log.proc_ins_count := ln_ins_count;
    rec_log.proc_upd_count := ln_upd_count;
    rec_log.proc_del_count := ln_del_count;
    rec_log.proc_err_count := ln_err_count;

    -- データ連携ログ更新
    CALL proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;
   
   EXCEPTION WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;
