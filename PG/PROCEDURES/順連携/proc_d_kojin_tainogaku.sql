--------------------------------------------------------
--  DDL for Procedure proc_d_kojin_tainogaku
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE proc_d_kojin_tainogaku( in_n_renkei_data_cd IN numeric, in_n_renkei_seq IN numeric, in_n_shori_ymd IN numeric, out_n_result_code INOUT numeric, out_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* �����T�v : �l�ؔ[�z�X�V                                                                                          */
/* ���� IN  : in_n_renkei_data_cd �c �A�g�f�[�^�R�[�h                                                                 */
/*            in_n_renkei_seq     �c �A�gSEQ�i�����P�ʂŕ��Ԃ����SEQ�j                                               */
/*            in_n_shori_ymd      �c ������ �i�����P�ʂŐݒ肳��鏈�����j                                            */
/*      OUT : out_n_result_co     �c ��O�G���[�������̃G���[�R�[�h                                                   */
/*            out_c_err_text      �c ��O�G���[�������̃G���[���e                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* �����@�@ : �V�K�쐬                                                                                                */
/**********************************************************************************************************************/

DECLARE

   ln_busho_array                type_busho[];
   ln_zeimoku_array              type_zeimoku[];
   ln_busho_zeimoku_array        numeric[][];

   ln_kagono_kbn_array           type_kagono_kbn_array[];
   ln_noki_mitorai_kbn_array     type_ln_noki_mitorai_kbn_array[];
   ln_jiko_kbn_array             type_jiko_kbn_array[];
   ln_shikkoteishi_kbn_array     type_shikkoteishi_kbn_array[];
   ln_kesson_kbn_array           type_kesson_kbn_array[];
   ln_kanrinin_kbn_array         type_kanrinin_kbn_array[];
   ln_kanno_cd_array             type_kanno_cd_array[];
   ln_kanno_flg_array            type_kanno_flg_array[];

   ln_taino_zeigaku              bigint DEFAULT 0;
   ln_taino_tokusoku             bigint DEFAULT 0;
   ln_taino_entaikin             bigint DEFAULT 0;
   ln_taino_kasankin             bigint DEFAULT 0;
   ln_taino_gokei                bigint DEFAULT 0;

   ln_i                          numeric DEFAULT 0;
   ln_j                          numeric DEFAULT 0;

   ln_para01                     numeric DEFAULT 0;
   ln_para02                     numeric DEFAULT 0;
   ln_para03                     numeric DEFAULT 0;
   ln_para04                     numeric DEFAULT 0;
   ln_para05                     numeric DEFAULT 0;

   ln_shori_count                numeric DEFAULT 0;
   ln_ins_count                  numeric DEFAULT 0;
   ln_upd_count                  numeric DEFAULT 0;
   ln_del_count                  numeric DEFAULT 0;
   ln_err_count                  numeric DEFAULT 0;
   ld_kaishi_datetime            timestamp;
   ld_shuryo_datetime            timestamp;

   lc_sql                        character varying(2000);

   cur_kojin CURSOR FOR
   SELECT kojin_no
   FROM f_kojin
   WHERE f_kojin.del_flg = 0;

   rec_kojin                     f_kojin%ROWTYPE;

   cur_menu CURSOR ( in_n_busho_cd numeric ) FOR
   SELECT *
   FROM t_menu_sub
   WHERE busho_cd = in_n_busho_cd
     AND menu_main_cd = 1
     AND menu_sub_cd  = 1
     AND del_flg = 0;

   rec_menu                       t_menu_sub%ROWTYPE;

   cur_busho CURSOR FOR
   SELECT busho_cd,
          del_flg
   FROM t_busho
   WHERE del_flg = 0
   ORDER BY busho_cd;
   
   rec_busho                 type_busho;

   cur_zeimoku CURSOR FOR
   SELECT zeimoku_cd
   FROM t_zeimoku
   WHERE del_flg = 0
   GROUP BY zeimoku_cd
   ORDER BY zeimoku_cd;
   
   rec_zeimoku                type_zeimoku;

   cur_busho_zeimoku CURSOR FOR
   SELECT t_busho.busho_cd, t_zeimoku.zeimoku_cd
   FROM t_busho
   LEFT JOIN t_zeimoku ON t_busho.busho_cd = t_zeimoku.busho_cd
   WHERE t_busho.del_flg = 0
   	AND t_zeimoku.del_flg = 0
   ORDER BY t_busho.busho_cd, t_zeimoku.zeimoku_cd;

   rec_busho_zeimoku             type_busho_zeimoku_cd;

   cur_parameter CURSOR FOR
   SELECT *
   FROM f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  f_renkei_parameter%ROWTYPE;

   cur_taino CURSOR FOR
   SELECT kibetsu_key
        , zeimoku_cd
        , kojin_no
        , doitsunin_kojin_no
        , kasankin_cd
        , zeigaku_mino
        , kasankin_mino
        , tokusoku_mino
        , entaikin_mino
        , noki_torai_handan_ymd
        , shobun_kano_ymd
        , jiko_yotei_ymd
        , shometsu_yotei_ymd
        , SIGN( SUM( shikkoteishi_flg ) ) shikkoteishi_flg
        , SIGN( SUM( kesson_flg ) ) kesson_flg
        , kanrinin_flg
        , kanno_cd
   FROM ( SELECT f_taino.kibetsu_key
               , f_taino.zeimoku_cd
               , f_taino.kojin_no
               , f_taino.doitsunin_kojin_no
               , f_taino.kasankin_cd
               , CASE f_taino.kasankin_cd WHEN 0 THEN f_taino.zeigaku_mino ELSE 0 END zeigaku_mino
               , CASE f_taino.kasankin_cd WHEN 0 THEN 0 ELSE f_taino.zeigaku_mino END kasankin_mino
               , f_taino.tokusoku_mino
               , f_taino.entaikin_mino
               , f_taino.noki_torai_handan_ymd
               , f_taino.shobun_kano_ymd
               , f_taino.jiko_yotei_ymd
               , f_taino.shometsu_yotei_ymd
               , CASE f_shikkoteishi_kibetsu.kibetsu_key WHEN NULL THEN 0 ELSE 1 END shikkoteishi_flg
               , CASE f_kesson_kibetsu.kibetsu_key WHEN NULL THEN 0 ELSE 1 END kesson_flg
               , 0 kanrinin_flg
               , f_taino.kanno_cd
          FROM f_taino
          LEFT JOIN f_shikkoteishi_kibetsu ON f_taino.kibetsu_key = f_shikkoteishi_kibetsu.kibetsu_key
          LEFT JOIN f_kesson_kibetsu ON f_taino.kibetsu_key = f_kesson_kibetsu.kibetsu_key
			 WHERE f_shikkoteishi_kibetsu.shobun_jotai_cd BETWEEN 10 AND 19
				 AND f_shikkoteishi_kibetsu.del_flg = 0
				 AND f_kesson_kibetsu.shobun_jotai_cd BETWEEN 10 AND 19
				 AND f_kesson_kibetsu.del_flg = 0
			    AND f_taino.kojin_no = rec_kojin.kojin_no
             AND f_taino.del_flg = 0
             AND f_shikkoteishi_kibetsu.del_flg = 0
             AND f_kesson_kibetsu.del_flg = 0

          UNION
          SELECT mv_taino_kanrinin.kibetsu_key
               , mv_taino_kanrinin.zeimoku_cd
               , mv_taino_kanrinin.kojin_no
               , mv_taino_kanrinin.doitsunin_kojin_no
               , mv_taino_kanrinin.kasankin_cd
               , CASE mv_taino_kanrinin.kasankin_cd WHEN 0 THEN mv_taino_kanrinin.zeigaku_mino ELSE 0 END zeigaku_mino
               , CASE mv_taino_kanrinin.kasankin_cd WHEN 0 THEN 0 ELSE mv_taino_kanrinin.zeigaku_mino END kasankin_mino
               , mv_taino_kanrinin.tokusoku_mino
               , mv_taino_kanrinin.entaikin_mino
               , mv_taino_kanrinin.noki_torai_handan_ymd
               , mv_taino_kanrinin.shobun_kano_ymd
               , mv_taino_kanrinin.jiko_yotei_ymd
               , mv_taino_kanrinin.shometsu_yotei_ymd
               , CASE f_shikkoteishi_kibetsu.kibetsu_key WHEN NULL THEN 0 ELSE 1 END shikkoteishi_flg
               , CASE f_kesson_kibetsu.kibetsu_key WHEN NULL THEN 0 ELSE 1 END kesson_flg
               , 1 kanrinin_flg
               , mv_taino_kanrinin.kanno_cd
          FROM mv_taino_kanrinin
          LEFT JOIN f_shikkoteishi_kibetsu ON mv_taino_kanrinin.kibetsu_key = f_shikkoteishi_kibetsu.kibetsu_key
			 LEFT JOIN f_kesson_kibetsu ON mv_taino_kanrinin.kibetsu_key = f_kesson_kibetsu.kibetsu_key
			 WHERE f_shikkoteishi_kibetsu.shobun_jotai_cd BETWEEN 10 AND 19 
			 	AND f_shikkoteishi_kibetsu.del_flg = 0
				AND f_kesson_kibetsu.shobun_jotai_cd BETWEEN 10 AND 19
				AND f_kesson_kibetsu.del_flg = 0
            AND mv_taino_kanrinin.kanrinin_kojin_no = rec_kojin.kojin_no
            AND f_shikkoteishi_kibetsu.del_flg = 0
            AND f_kesson_kibetsu.del_flg = 0
            ) AS dunno
   GROUP BY kibetsu_key, zeimoku_cd, kojin_no, doitsunin_kojin_no, kasankin_cd, zeigaku_mino, kasankin_mino, tokusoku_mino
          , entaikin_mino, noki_torai_handan_ymd, shobun_kano_ymd, jiko_yotei_ymd, shometsu_yotei_ymd, kanrinin_flg, kanno_cd;


   rec_taino                      type_taino[];

BEGIN

   ld_kaishi_datetime := CURRENT_TIMESTAMP(0);

   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 3 THEN ln_para03 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 4 THEN ln_para04 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 5 THEN ln_para05 := rec_parameter.parameter_value; END IF;
	  
	  END LOOP;
   CLOSE cur_parameter;

   BEGIN
      lc_sql := 'TRUNCATE TABLE dlgmain.f_kojin_tainogaku';

      EXECUTE lc_sql;
   EXCEPTION
         WHEN OTHERS THEN NULL;
		 
   END;

   OPEN cur_busho;
      LOOP
         FETCH cur_busho INTO rec_busho;
		 EXIT WHEN NOT FOUND;
		 
		 ln_busho_array := ARRAY_APPEND(ln_busho_array, rec_busho);
		 
	  END LOOP;
   CLOSE cur_busho;
   
   OPEN cur_zeimoku;
      LOOP
         FETCH cur_zeimoku INTO rec_zeimoku.zeimoku_cd;
		 EXIT WHEN NOT FOUND;
		 
		 ln_zeimoku_array := ARRAY_APPEND(ln_zeimoku_array, rec_zeimoku);
		 
	  END LOOP;
   CLOSE cur_zeimoku;
   
   OPEN cur_busho_zeimoku;
      LOOP
         FETCH cur_busho_zeimoku INTO rec_busho_zeimoku;
         EXIT WHEN NOT FOUND;
		 
         ln_busho_zeimoku_array[rec_busho_zeimoku.busho_cd][rec_busho_zeimoku.zeimoku_cd] := 1;
		 
      END LOOP;
   CLOSE cur_busho_zeimoku;
   
   IF ln_para01 = 1 AND ln_busho_array IS NOT NULL THEN
      FOR ln_i IN ARRAY_LOWER(ln_busho_array, 1)..ARRAY_UPPER(ln_busho_array, 1) LOOP
         OPEN cur_menu(ln_busho_array[ln_i].busho_cd);
            IF NOT FOUND THEN
               ln_kagono_kbn_array[ln_i]       := 0;
               ln_noki_mitorai_kbn_array[ln_i] := 0;
               ln_jiko_kbn_array[ln_i]         := 0;
               ln_shikkoteishi_kbn_array[ln_i] := 0;
               ln_kesson_kbn_array[ln_i]       := 0;
               ln_kanrinin_kbn_array[ln_i]     := 0;
               ln_kanno_cd_array[ln_i]         := 0;
               ln_kanno_flg_array[ln_i]        := 0;
            ELSE
               FETCH cur_menu INTO rec_menu;
               ln_kagono_kbn_array[ln_i]       := rec_menu.kagono_kbn;
               ln_noki_mitorai_kbn_array[ln_i] := rec_menu.noki_mitorai_kbn;
               ln_jiko_kbn_array[ln_i]         := rec_menu.jiko_kbn;
               ln_shikkoteishi_kbn_array[ln_i] := rec_menu.shikkoteishi_kbn;
               ln_kesson_kbn_array[ln_i]       := rec_menu.kesson_kbn;
               ln_kanrinin_kbn_array[ln_i]     := rec_menu.kanrinin_kbn;
               ln_kanno_cd_array[ln_i]         := rec_menu.kanno_cd;
               ln_kanno_flg_array[ln_i]        := rec_menu.kanno_flg;
            END IF;
         CLOSE cur_menu;
      END LOOP;
   END IF;

	IF ln_busho_array IS NOT NULL THEN
		FOR ln_i IN ARRAY_LOWER(ln_busho_array, 1)..ARRAY_UPPER(ln_busho_array, 1) LOOP
			BEGIN
				INSERT INTO f_kojin_tainogaku( busho_cd, kojin_no, zeigaku_taino, tokusoku_taino, entaikin_taino, kasankin_taino, gokei_kingaku_taino, kazeisha_kbn, upd_tantosha_cd, upd_tammatsu, del_flg )
				SELECT ln_busho_array[ln_i].busho_cd, f_kojin.kojin_no, 0, 0, 0, 0, 0, 0, 'BATCH', 'SERVER', 0
				FROM f_kojin;

			EXCEPTION
				WHEN OTHERS THEN NULL;
			END;
		END LOOP;
   END IF;

   OPEN cur_kojin;
      LOOP
         FETCH cur_kojin INTO rec_kojin;
         EXIT WHEN NOT FOUND;

         ln_shori_count := ln_shori_count + 1;

         OPEN cur_taino;
             FETCH cur_taino INTO rec_taino;
         CLOSE cur_taino;

         IF array_length(rec_taino, 1) <> 0 AND ln_busho_array IS NOT NULL THEN
            FOR ln_i IN ARRAY_LOWER(ln_busho_array, 1)..ARRAY_UPPER(ln_busho_array, 1) LOOP
               ln_taino_zeigaku  := 0;
               ln_taino_tokusoku := 0;
               ln_taino_entaikin := 0;
               ln_taino_kasankin := 0;
               ln_taino_gokei    := 0;

               FOR ln_j IN ARRAY_LOWER(rec_taino, 1)..ARRAY_UPPER(rec_taino, 1) LOOP
                  IF ln_busho_zeimoku_array.EXISTS(ln_busho_array[ln_i].busho_cd) THEN
						   IF ln_busho_zeimoku_array[ln_busho_array[ln_i].busho_cd][rec_taino[ln_j].zeimoku_cd] IS NOT NULL THEN
                     -- IF ln_busho_zeimoku_array[ln_busho_array[ln_i].busho_cd].EXISTS(rec_taino[ln_j].zeimoku_cd) THEN
                        IF ln_para01 = 0 THEN
                           ln_taino_zeigaku := ln_taino_zeigaku + rec_taino[ln_j].zeigaku_mino;
                           ln_taino_kasankin := ln_taino_kasankin + rec_taino[ln_j].kasankin_mino;
                           ln_taino_tokusoku := ln_taino_tokusoku + rec_taino[ln_j].tokusoku_mino;
                           ln_taino_entaikin := ln_taino_entaikin + rec_taino[ln_j].entaikin_mino;
                           ln_taino_gokei   := ln_taino_zeigaku + ln_taino_kasankin + ln_taino_tokusoku + ln_taino_entaikin;
                        ELSE
                           CONTINUE WHEN ln_noki_mitorai_kbn_array[ln_i] IN( 1, 3 ) AND rec_taino[ln_j].noki_torai_handan_ymd > getdatetonum( CURRENT_TIMESTAMP(0) );
                           CONTINUE WHEN ln_noki_mitorai_kbn_array[ln_i] IN( 2, 4 ) AND rec_taino[ln_j].shobun_kano_ymd > getdatetonum( CURRENT_TIMESTAMP(0) );
                           CONTINUE WHEN ln_jiko_kbn_array[ln_i] IN( 0, 2 ) AND ( ( rec_taino[ln_j].jiko_yotei_ymd <> 0 AND rec_taino[ln_j].jiko_yotei_ymd <= getdatetonum( CURRENT_TIMESTAMP(0) ) ) OR ( rec_taino[ln_j].shometsu_yotei_ymd <> 0 AND rec_taino[ln_j].shometsu_yotei_ymd <= getdatetonum( CURRENT_TIMESTAMP(0) ) ) );
                           CONTINUE WHEN ln_shikkoteishi_kbn_array[ln_i] IN( 0, 2 ) AND rec_taino[ln_j].shikkoteishi_flg = 1;
                           CONTINUE WHEN ln_kesson_kbn_array[ln_i] IN( 0, 2 ) AND rec_taino[ln_j].kesson_flg = 1;
                           CONTINUE WHEN ln_kanrinin_kbn_array[ln_i] = 0 AND rec_taino[ln_j].kanrinin_flg = 1;

                           IF ln_kagono_kbn_array[ln_i] = 0 AND rec_taino[ln_j].zeigaku_mino <= 0 THEN 
                              NULL;
                           ELSE
                              ln_taino_zeigaku := ln_taino_zeigaku + rec_taino[ln_j].zeigaku_mino;
                           END IF;

                           IF ln_kagono_kbn_array[ln_i] = 0 AND rec_taino[ln_j].kasankin_mino <= 0 THEN 
                              NULL;
                           ELSE
                              ln_taino_kasankin := ln_taino_kasankin + rec_taino[ln_j].kasankin_mino;
                           END IF;

                           IF ln_kagono_kbn_array[ln_i] = 0 AND rec_taino[ln_j].tokusoku_mino <= 0 THEN 
                              NULL;
                           ELSIF ln_kanno_flg_array[ln_i] = 0 AND ln_kanno_cd_array[ln_i] = 1 AND rec_taino[ln_j].kanno_cd IN( 1, 2, 3, 4 ) THEN
                              NULL;
                           ELSIF ln_kanno_flg_array[ln_i] = 0 AND ln_kanno_cd_array[ln_i] = 2 AND rec_taino[ln_j].kanno_cd IN( 2, 3, 4 ) THEN
                              NULL;
                           ELSIF ln_kanno_flg_array[ln_i] = 0 AND ln_kanno_cd_array[ln_i] = 3 AND rec_taino[ln_j].kanno_cd IN( 2, 3, 4 ) THEN
                              NULL;
                           ELSE
                              ln_taino_tokusoku := ln_taino_tokusoku + rec_taino[ln_j].tokusoku_mino;
                           END IF;

                           IF ln_kagono_kbn_array[ln_i] = 0 AND rec_taino[ln_j].entaikin_mino <= 0 THEN 
                              NULL;
                           ELSIF ln_kanno_flg_array[ln_i] = 0 AND ln_kanno_cd_array[ln_i] = 1 AND rec_taino[ln_j].kanno_cd IN( 1, 2, 3, 4 ) THEN
                              NULL;
                           ELSIF ln_kanno_flg_array[ln_i] = 0 AND ln_kanno_cd_array[ln_i] = 2 AND rec_taino[ln_j].kanno_cd IN( 2, 3, 4 ) THEN
                              NULL;
                           ELSIF ln_kanno_flg_array[ln_i] = 0 AND ln_kanno_cd_array[ln_i] = 3 AND rec_taino[ln_j].kanno_cd IN( 2, 3, 4 ) THEN
                              NULL;
                           ELSE
                              ln_taino_entaikin := ln_taino_entaikin + rec_taino[ln_j].entaikin_mino;
                           END IF;

                           ln_taino_gokei   := ln_taino_zeigaku + ln_taino_kasankin + ln_taino_tokusoku + ln_taino_entaikin;
                        END IF;

                        
                     END IF;
                  END IF;
                 NULL;
               END LOOP;


               BEGIN
                  UPDATE f_kojin_tainogaku
                  SET zeigaku_taino  = ln_taino_zeigaku
                    , tokusoku_taino = ln_taino_tokusoku
                    , entaikin_taino = ln_taino_entaikin
                    , kasankin_taino = ln_taino_kasankin
                    , gokei_kingaku_taino = ln_taino_gokei
                    , kazeisha_kbn   = CASE ln_taino_gokei WHEN 0 THEN 1 ELSE 2 END
                  WHERE busho_cd = ln_busho_array[ln_i].busho_cd
                   AND kojin_no = rec_kojin.kojin_no;
               EXCEPTION
                  WHEN OTHERS THEN NULL;
               END;
            END LOOP;
         END IF;

      END LOOP;
   CLOSE cur_kojin;

   ld_shuryo_datetime := CURRENT_TIMESTAMP(0);

   BEGIN
      INSERT INTO f_batch_log( renkei_data_cd, seq_no_renkei, shori_ymd, kaishi_datetime, shuryo_datetime, shori_count, ins_count, upd_count, del_count, err_count )
      VALUES( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, ld_kaishi_datetime, ld_shuryo_datetime, ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count );
   EXCEPTION
      WHEN OTHERS THEN NULL;

   END;

   RAISE NOTICE '�J�n�F% �I���F% ���������F% �ǉ������F% �X�V�����F% �G���[�����F%', ld_kaishi_datetime, ld_shuryo_datetime, ln_shori_count, ln_ins_count, ln_upd_count, ln_err_count;

END;
$$;