--------------------------------------------------------
--  DDL for Procedure  proc_r4g_taino
--------------------------------------------------------

CREATE OR REPLACE PROCEDURE dlgrenkei.proc_r4g_taino (
   in_n_renkei_data_cd IN numeric,
   in_n_renkei_seq IN numeric,
   in_n_shori_ymd IN numeric,
   io_c_err_code INOUT character varying,
   io_c_err_text INOUT character varying )
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : f_滞納（f_taino）の追加／更新／削除を実施する                                                           */
/* 引数 IN  : in_n_renkei_data_cd … 連携データコード                                                                 */
/*            in_n_renkei_seq     … 連携SEQ（処理単位で符番されるSEQ）                                               */
/*            in_n_shori_ymd      … 処理日 （処理単位で設定される処理日）                                            */
/*      OUT : io_c_err_code       … 例外エラー発生時のエラーコード                                                   */
/*            io_c_err_text       … 例外エラー発生時のエラー内容                                                     */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 :  CRESS-INFO.Angelo     新規作成     036o005「調定収納情報（統合収滞納）」の取込を行う                   */
/**********************************************************************************************************************/

DECLARE
   rec_f_taino                    f_taino%ROWTYPE;
   ln_shori_count                 numeric DEFAULT 0;
   ln_ins_count                   numeric DEFAULT 0;
   ln_upd_count                   numeric DEFAULT 0;
   ln_del_count                   numeric DEFAULT 0;
   ln_del_diag_count              numeric DEFAULT 0;
   ln_err_count                   numeric DEFAULT 0;
   lc_err_text                    character varying(100);
   ln_result_cd                   numeric DEFAULT 0;
   lc_err_cd                      character varying;
   lc_sql                         character varying(1000);

   ln_para01                      numeric DEFAULT 0;
   ln_para02                      numeric DEFAULT 0;
   ln_para15                      numeric DEFAULT 0;
   ln_para16                      numeric DEFAULT 0;

   ln_result_cd_add               numeric DEFAULT 1; -- 追加
   ln_result_cd_del               numeric DEFAULT 3; -- 削除
   ln_result_cd_upd               numeric DEFAULT 2; -- 更新
   ln_result_cd_err               numeric DEFAULT 9; -- エラー

   lc_err_cd_normal               character varying = '0'; -- 通常
   lc_err_cd_err                  character varying = '9'; -- エラー

   rec_log                        dlgrenkei.f_renkei_log%ROWTYPE;
   ln_hotei_noki_to_ymd           numeric DEFAULT 0;
   ln_hotei_noki_ymd              numeric DEFAULT 0;
   lc_kibetsu                     character varying;

   cur_main CURSOR FOR
   SELECT *
   FROM dlgrenkei.i_r4g_shuno
   WHERE result_cd < 8;

   rec_main                       dlgrenkei.i_r4g_shuno%ROWTYPE;

   cur_parameter CURSOR FOR
   SELECT *
   FROM dlgrenkei.f_renkei_parameter
   WHERE renkei_data_cd = in_n_renkei_data_cd;

   rec_parameter                  dlgrenkei.f_renkei_parameter%ROWTYPE;

   cur_data_kanri_kibetsu CURSOR FOR
   SELECT *
   FROM f_data_kanri_kibetsu;

   rec_data_kanri_kibetsu         f_data_kanri_kibetsu%ROWTYPE;
   
   cur_lock CURSOR FOR
   SELECT *
   FROM f_taino
   WHERE kibetsu_key = rec_f_taino.kibetsu_key;

   rec_lock                       f_taino%ROWTYPE;

   cur_doitsunin CURSOR FOR
   SELECT doitsunin_kojin_no
   FROM f_kojin
   WHERE kojin_no = rec_f_taino.kojin_no;

   rec_doitsunin                  f_kojin%ROWTYPE;

BEGIN

   rec_log.proc_kaishi_datetime := CURRENT_TIMESTAMP;

   -- １．パラメータ情報の取得
   OPEN cur_parameter;
      LOOP
         FETCH cur_parameter INTO rec_parameter;
         EXIT WHEN NOT FOUND;

         IF rec_parameter.parameter_no = 1 THEN ln_para01 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 2 THEN ln_para02 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 15 THEN ln_para15 := rec_parameter.parameter_value; END IF;
         IF rec_parameter.parameter_no = 16 THEN ln_para16 := rec_parameter.parameter_value; END IF;
      END LOOP;
   CLOSE cur_parameter;

   OPEN cur_doitsunin;
      FETCH cur_doitsunin INTO rec_doitsunin;
   CLOSE cur_doitsunin;

   IF rec_doitsunin.doitsunin_kojin_no IS NULL THEN
      rec_doitsunin.doitsunin_kojin_no := rec_f_taino.kojin_no;
   END IF;

   -- ２．連携先データの削除
   IF ln_para01 = 1 THEN
      BEGIN
         SELECT COUNT(*) INTO ln_del_count FROM f_taino;
         lc_sql := 'TRUNCATE TABLE dlgmain.f_taino';
         EXECUTE lc_sql;

      EXCEPTION WHEN OTHERS THEN
         io_c_err_code := SQLSTATE;
         io_c_err_text := SQLERRM;
         RETURN;
      END;
   END IF;


   -- ３．中間テーブルデータのエラーチェック
   CALL proc_error_check( in_n_renkei_data_cd, in_n_renkei_seq, in_n_shori_ymd, io_c_err_code, io_c_err_text );

   IF io_c_err_code <> '0' THEN
      RETURN;
   END IF;
   
   IF ln_para02 = 1 THEN
      CALL proc_taino_drop_index();
   END IF;

   -- ５．連携データの作成・更新
   ln_shori_count := 0;
   OPEN cur_main;
      LOOP
         FETCH cur_main INTO rec_main;
         EXIT WHEN NOT FOUND;

         IF rec_main.zeimoku_cd <> '04' AND rec_f_taino.zeigaku > 0 AND (rec_f_taino.noki_ymd IS NULL OR rec_f_taino.noki_ymd = 0) THEN
            ln_err_count := ln_err_count + 1;
            lc_err_text := '納期エラー';
            lc_err_cd := lc_err_cd_err;
            ln_result_cd := ln_result_cd_err;
         ELSE
            ln_shori_count                 := ln_shori_count + 1;
            lc_err_cd                      := lc_err_cd_normal;
            lc_err_text                    := '';
            ln_result_cd                   := 0;
            rec_f_taino                    := NULL;
            rec_lock                       := NULL;

            -- 期別明細KEY
            rec_f_taino.kibetsu_key := get_kibetsu_key(
               rec_main.fuka_nendo, rec_main.soto_nendo,
               rec_main.zeimoku_cd, rec_main.kibetsu_cd,
               rec_main.tokucho_shitei_no,rec_main.jido_atena_no,
               rec_main.tsuchisho_no, rec_main.jigyo_nendo_no,
               rec_main.shinkoku_rireki_no
            );
            -- 賦課年度
            rec_f_taino.fuka_nendo := rec_main.fuka_nendo::numeric;
            -- 相当年度
            rec_f_taino.soto_nendo := rec_main.soto_nendo::numeric;
            -- 税目コード
            rec_f_taino.zeimoku_cd := get_r4g_code_conv(0, 3, rec_main.zeimoku_cd, NULL);
            -- 期別コード
            rec_f_taino.kibetsu_cd := rec_main.kibetsu_cd;

            SELECT kibetsu INTO lc_kibetsu
            FROM t_kibetsu
            WHERE fuka_nendo = rec_f_taino.fuka_nendo
                  AND nendo_kbn = rec_f_taino.nendo_kbn
                  AND kankatsu_cd = rec_f_taino.kankatsu_cd
                  AND zeimoku_cd = rec_f_taino.zeimoku_cd
                  AND kibetsu_cd = rec_f_taino.kibetsu_cd;

            -- 期別
            rec_f_taino.kibetsu := lc_kibetsu;
            -- 個人番号
            rec_f_taino.kojin_no := rec_main.atena_no;
            -- 通知書番号
            rec_main.tsuchisho_no := rec_main.tsuchisho_no;
            -- 事業開始
            rec_main.jigyo_kaishi_ymd := CASE WHEN rec_main.jigyo_kaishi_ymd IS NOT NULL THEN getdatetonum(to_date(rec_main.jigyo_kaishi_ymd, 'YYYY-MM-DD')) ELSE 0 END;
            -- 事業終了
            rec_main.jigyo_shuryo_ymd := CASE WHEN rec_main.jigyo_shuryo_ymd IS NOT NULL THEN getdatetonum(to_date(rec_main.jigyo_shuryo_ymd, 'YYYY-MM-DD')) ELSE 0 END;
            -- 申告区分コード
            rec_main.shinkoku_cd := CASE WHEN rec_main.shinkoku_cd IS NOT NULL THEN rec_main.shinkoku_cd::numeric ELSE 0 END;
            -- 修正回数
            rec_f_taino.shusei_kaisu := 0;
            -- 年度区分
            rec_f_taino.nendo_kbn := 0;
            -- 管轄コード
            rec_f_taino.kankatsu_cd := 0;
            -- 加算金コード
            rec_f_taino.kasankin_cd := 0;
            -- 特別徴収義務者指定番号
            rec_f_taino.tokucho_shitei_no := rec_main.tokucho_shitei_no;
            -- 申告履歴番号
            rec_f_taino.shinkoku_rireki_no := rec_main.shinkoku_rireki_no::numeric;
            -- 事業年度番号
            rec_f_taino.jigyo_nendo_no := rec_main.jigyo_nendo_no::numeric;
            -- 児童宛名番号
            rec_f_taino.jido_kojin_no := rec_main.jido_atena_no;
            -- 納期限
            rec_f_taino.noki_ymd := getdatetonum(to_date(rec_main.noki_ymd, 'YYYY-MM-DD'));
            -- 繰上納期限
            rec_f_taino.noki_kuriage_ymd := 0;
            -- 指定納期限
            rec_f_taino.shitei_noki_ymd := getdatetonum(to_date(rec_main.shitei_noki_ymd, 'YYYY-MM-DD'));
            -- 督促発布日
            rec_f_taino.tokusoku_ymd := 0;
            -- 督促返戻日
            rec_f_taino.tokusoku_henrei_ymd := 0;
            -- 督促公示日
            rec_f_taino.tokusoku_koji_ymd := 0;
            -- 督促納期
            rec_f_taino.tokusoku_noki_ymd := 0;
            -- 催告日
            rec_f_taino.saikoku_ymd := 0;
            -- 催告納期
            rec_f_taino.saikoku_noki_ymd := 0;
            -- 法定納期限等
            rec_f_taino.hotei_noki_to_ymd := CASE
                                                WHEN ln_para15 IN (0, 2) THEN 
                                                      getdatetonum(to_date(rec_main.hotei_noki_to_ymd, 'YYYY-MM-DD'))
                                                WHEN ln_para15 = 1 THEN 0
                                                ELSE NULL END;
            -- 法定納期限
            rec_f_taino.hotei_noki_ymd := CASE
                                          WHEN ln_para16 IN (0, 2) THEN 
                                                getdatetonum(to_date(rec_main.hotei_noki_ymd, 'YYYY-MM-DD'))
                                          WHEN ln_para16 = 1 THEN 0
                                          ELSE NULL END;
            -- 起算日
            rec_f_taino.kisan_ymd := 0;
            -- 課税更正日
            rec_f_taino.kazei_kosei_ymd := getdatetonum(to_date(rec_main.kazei_kosei_ymd, 'YYYY-MM-DD'));
            -- 更正事由コード
            rec_f_taino.kosei_jiyu_cd := rec_main.kosei_jiyu_cd::numeric;
            -- 申告日
            rec_f_taino.shinkoku_ymd := getdatetonum(to_date(rec_main.shinkoku_ymd, 'YYYY-MM-DD'));
            -- 修正申告日
            rec_f_taino.shusei_shinkoku_ymd := getdatetonum(to_date(rec_main.shusei_shinkoku_ymd, 'YYYY-MM-DD'));
            --確定申告日
            rec_f_taino.kakutei_shinkoku_ymd := getdatetonum(to_date(rec_main.kakutei_shinkoku_ymd, 'YYYY-MM-DD'));
            -- 更正決定通知年月日
            rec_f_taino.kosei_kettei_tsuchi_ymd := getdatetonum(to_date(rec_main.kosei_kettei_tsuchi_ymd, 'YYYY-MM-DD'));
            -- 延長月数
            rec_f_taino.encho_tsuki := rec_main.shinkoku_kigen_encho::numeric;
            -- 申告期限
            rec_f_taino.shinkoku_kigen_ymd := getdatetonum(to_date(rec_main.shinkoku_kigen, 'YYYY-MM-DD'));
            -- 延長期限
            rec_f_taino.encho_kigen_ymd := getdatetonum(to_date(rec_main.encho_shinkoku_kigen, 'YYYY-MM-DD'));
            -- 更正請求日
            rec_f_taino.kosei_seikyu_ymd := getdatetonum(to_date(rec_main.kosei_seikyu_ymd, 'YYYY-MM-DD'));
            -- 国税の申告基礎区分
            rec_f_taino.kokuzei_shinkoku_kiso_kbn := rec_main.kokuzei_shinkoku_kbn::numeric;
            -- 国税申告（更正）年月日
            rec_f_taino.kokuzei_shinkoku_ymd := getdatetonum(to_date(rec_main.kokuzei_shinkoku_ymd, 'YYYY-MM-DD'));
            -- 更正申告日
            rec_f_taino.kosei_shinkoku_ymd := CASE WHEN getdatetonum(to_date(rec_main.kosei_seikyu_ymd, 'YYYY-MM-DD')) <= getdatetonum(to_date(rec_main.kokuzei_shinkoku_ymd, 'YYYY-MM-DD')) THEN
                                                         getdatetonum(to_date(rec_main.kosei_seikyu_ymd, 'YYYY-MM-DD'))
                                                    ELSE getdatetonum(to_date(rec_main.kokuzei_shinkoku_ymd, 'YYYY-MM-DD')) END;
            -- 時効予定日
            rec_f_taino.jiko_yotei_ymd := 0;
            -- 納税消滅予定年月日
            rec_f_taino.shometsu_yotei_ymd := 0;
            -- 税額
            rec_f_taino.zeigaku := rec_main.zeigaku::numeric;
            -- 督促手数料
            rec_f_taino.tokusoku := rec_main.tokusoku::numeric;
            -- 延滞金
            rec_f_taino.entaikin := rec_main.entaikin::numeric;
            -- 延滞金確定コード
            rec_f_taino.entaikin_kakutei_cd := CASE WHEN rec_f_taino.entaikin = 0 THEN 0 ELSE 1 END;
            -- 延滞金強制入力区分
            rec_f_taino.entaikin_kyosei_kbn := rec_main.entaikin_kyosei_kbn::numeric;
            -- 延滞金強制入力年月日
            rec_f_taino.entaikin_kyosei_ymd := getdatetonum(to_date(rec_main.entaikin_kyosei_ymd, 'YYYY-MM-DD'));
            -- 調定額_法人住民税内訳_均等割額
            rec_f_taino.zeigaku_kintowari := rec_main.zeigaku_kintowari::numeric;
            -- 調定額_法人住民税内訳_法人税割額
            rec_f_taino.zeigaku_hojinwari := rec_main.zeigaku_hojinwari::numeric;
            -- 調定額_国民健康保険内訳_医療一般分
            rec_f_taino.zeigaku_iryo_ippan := rec_main.zeigaku_iryo_ippan::numeric;
            -- 調定額_国民健康保険内訳_医療退職分
            rec_f_taino.zeigaku_iryo_taisyoku := rec_main.zeigaku_iryo_taisyoku::numeric;
            -- 調定額_国民健康保険内訳_介護一般分
            rec_f_taino.zeigaku_kaigo_ippan := rec_main.zeigaku_kaigo_ippan::numeric;
            -- 調定額_国民健康保険内訳_介護退職分
            rec_f_taino.zeigaku_kaigo_taisyoku := rec_main.zeigaku_kaigo_taisyoku::numeric;
            -- 調定額_国民健康保険内訳_支援一般分
            rec_f_taino.zeigaku_shien_ippan := rec_main.zeigaku_shien_ippan::numeric;
            -- 調定額_国民健康保険内訳_支援退職分
            rec_f_taino.zeigaku_shien_taisyoku := rec_main.zeigaku_shien_taisyoku::numeric;
            -- 収納税額
            rec_f_taino.zeigaku_shuno := rec_main.zeigaku_shuno::numeric + rec_main.zeigaku_karikeshi::numeric;
            -- 収納督促手数料
            rec_f_taino.tokusoku_shuno := rec_main.tokusoku_shuno::numeric + rec_main.tokusoku_karikeshi::numeric;
            -- 収納延滞金
            rec_f_taino.entaikin_shuno := rec_main.entaikin_shuno::numeric + rec_main.entaikin_karikeshi::numeric;
            -- 収納額_法人住民税内訳_均等割額
            rec_f_taino.zeigaku_kintowari_shuno := rec_main.zeigaku_kintowari_shuno::numeric;
            -- 収納額_法人住民税内訳_法人税割額
            rec_f_taino.zeigaku_hojinwari_shuno := rec_main.zeigaku_hojinwari_shuno::numeric;
            -- 最終日計日
            rec_f_taino.saishu_nikkei_ymd := getdatetonum(to_date(rec_main.shunyu_ymd, 'YYYY-MM-DD'));
            -- 最終収納日
            rec_f_taino.saishu_shuno_ymd := getdatetonum(to_date(rec_main.ryoshu_ymd, 'YYYY-MM-DD'));
            -- 最終収納額
            rec_f_taino.saishu_shuno_kingaku := 0;
            -- 完納コード
            rec_f_taino.kanno_cd := get_kanno_cd(
                  rec_main.zeigaku, rec_main.tokusoku,
                  rec_main.entaikin, rec_main.zeigaku_shuno,
                  rec_main.tokusoku_shuno, rec_main.entaikin_shuno);
            -- 完納年月日
            rec_f_taino.kanno_ymd := 0;
            -- 未納税額
            rec_f_taino.zeigaku_mino := rec_f_taino.zeigaku - rec_f_taino.zeigaku_shuno;
            -- 未納督促手数料
            rec_f_taino.tokusoku_mino := rec_f_taino.tokusoku - rec_f_taino.tokusoku_shuno;
            -- 未納延滞金
            rec_f_taino.entaikin_mino := CASE WHEN rec_f_taino.entaikin_kakutei_cd = 0 THEN 0 ELSE rec_f_taino.entaikin - rec_f_taino.entaikin_shuno END;
            -- 所得割
            rec_f_taino.shotokuwari := 0;
            -- 付加価値割
            rec_f_taino.fukakachiwari := 0;
            -- 資本割
            rec_f_taino.shihonwari := 0;
            -- 収入割
            rec_f_taino.shunyuwari := 0;
            -- 当初課税額
            rec_f_taino.tosho_kazeigaku := 0;
            -- 重加算金対象税額
            rec_f_taino.jukasankin_taisho_zeigaku := 0;
            -- 管理人区分
            rec_f_taino.kanrinin_cd := 0;
            -- 管理人個人番号
            rec_f_taino.kanrinin_kojin_no := 0;
            -- 処分可能日
            rec_f_taino.shobun_kano_ymd := 0;
            -- 納期到来日
            rec_f_taino.noki_torai_handan_ymd := 0;
            -- 会計年度
            rec_f_taino.kaikei_nendo := 0;
            -- 個別項目１
            rec_f_taino.kobetsu_komoku1 := get_kobetsu_komoku1(rec_main);
            -- 個別項目２
            rec_f_taino.kobetsu_komoku2 := NULL;
            -- 個別項目３
            rec_f_taino.kobetsu_komoku3 := NULL;
            -- 予備項目１
            rec_f_taino.yobi_komoku1 := NULL;
            -- 予備項目２
            rec_f_taino.yobi_komoku2 := NULL;
            -- 予備項目３
            rec_f_taino.yobi_komoku3 := NULL;
            -- 同一人番号
            rec_f_taino.doitsunin_kojin_no := rec_main.atena_no;
            -- 被保険者番号
            rec_f_taino.hihokensha_no := rec_f_taino.hihokensha_no;
            -- 国保記号番号
            rec_f_taino.kokuhokigo_no := rec_f_taino.kokuhokigo_no;
            -- 共有資産番号
            rec_f_taino.kyoyu_shisan_no := rec_f_taino.kyoyu_shisan_no;
            -- 市税事務所コード
            rec_f_taino.shizei_jimusho_cd := rec_f_taino.shizei_jimusho_cd;
            -- 通知年月日
            rec_f_taino.tsuchi_ymd := 0;
            -- 構成員督促送付可否フラグ
            rec_f_taino.koseiin_tokusoku_flg := rec_main.koseiin_tokusoku_flg::numeric;
            -- 土地・家屋_固定資産税額
            rec_f_taino.zeigaku_kotei_tochikaoku := rec_f_taino.zeigaku_kotei_tochikaoku::numeric;
            -- 償却資産_固定資産税額
            rec_f_taino.zeigaku_kotei_shokyaku := rec_f_taino.zeigaku_kotei_shokyaku::numeric;
            -- 森林環境税額
            rec_f_taino.zeigaku_shinrin := rec_f_taino.zeigaku_shinrin::numeric;
            -- 配当割・株式等譲渡所得割控除額
            rec_f_taino.shotokuwari_kojo := rec_main.haitowari_shotokuwari_kojo::numeric;
            -- 配当割・株式等譲渡所得割還付額
            rec_f_taino.shotokuwari_kanpu := rec_main.haitowari_shotokuwari_kanpu::numeric;
            -- 控除不足額
            rec_f_taino.kojo_fusoku := rec_main.kojo_fusoku::numeric;
            -- 充当又は委託納付額
            rec_f_taino.kojo_fusoku_nofu := rec_main.juto_itaku_nofu::numeric;
            -- 納期特例フラグ
            rec_f_taino.noki_tokurei_flg := rec_main.noki_tokurei_kbn::numeric;
            -- 納期特例適用後納期
            rec_f_taino.noki_tokurei_ym := rec_main.noki_tokurei_ym::numeric;
            -- 課税区分
            rec_f_taino.kazei_kbn := rec_main.kazei_kbn::numeric;
            -- 軽自管理番号
            rec_f_taino.keiji_kanri_no := rec_main.keiji_kanri_no;
            -- 車台番号
            rec_f_taino.shadai_no := rec_main.shadai_no;
            -- 種別コード
            rec_f_taino.keiji_shubetsu_cd := rec_main.keiji_shubetsu_cd::numeric;
            -- 車両番号（標識番号）_標板文字
            rec_f_taino.sharyo_no1 := rec_main.sharyo_no1;
            -- 車両番号（標識番号）_分類番号
            rec_f_taino.sharyo_no2 := rec_main.sharyo_no2;
            -- 車両番号（標識番号）_かな文字
            rec_f_taino.sharyo_no3 := rec_main.sharyo_no3;
            -- 車両番号（標識番号）_一連指定番号
            rec_f_taino.sharyo_no4 := rec_main.sharyo_no4;
            -- 証明書有効期限
            rec_f_taino.shomeisho_yuko_kigen := getdatetonum(to_date(rec_main.shomeisho_yuko_kigen, 'YYYY-MM-DD'));
            -- 重加算税の有無
            rec_f_taino.jukazei_flg := rec_main.jukasanzei_flg::numeric;
            -- 不納欠損日
            rec_f_taino.kesson_ymd := getdatetonum(to_date(rec_main.kesson_ymd, 'YYYY-MM-DD'));
            -- 不納欠損事由
            rec_f_taino.kesson_jiyu_cd := rec_main.kesson_jiyu_cd::numeric;
            -- 不納欠損金額_本税（料）
            rec_f_taino.zeigaku_kesson := rec_main.zeigaku_kesson::numeric;
            -- 不納欠損金額_延滞金
            rec_f_taino.entaikin_kesson := rec_main.entaikin_kesson::numeric;
            -- 不納欠損金額_督促手数料
            rec_f_taino.tokusoku_kesson := rec_main.tokusoku_kesson::numeric;
            -- 子ども・子育て事業所番号
            rec_f_taino.kodomo_jigyosho_no := rec_main.kodomo_jigyosho_no;
            -- データ作成日時
            rec_f_taino.ins_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            -- データ更新日時
            rec_f_taino.upd_datetime := concat(rec_main.sosa_ymd, ' ', rec_main.sosa_time)::timestamp;
            -- 更新担当者コード
            rec_f_taino.upd_tantosha_cd := rec_main.sosasha_cd;
            -- 更新端末名称
            rec_f_taino.upd_tammatsu := 'SERVER';
            -- 削除フラグ
            rec_f_taino.del_flg := rec_main.del_flg::numeric;

            OPEN cur_lock;
                  FETCH cur_lock INTO rec_lock;
            CLOSE cur_lock;

            IF rec_f_taino.del_flg = 1 THEN
               BEGIN
               DELETE FROM f_taino
                     WHERE kibetsu_key = rec_f_taino.kibetsu_key;

                     ln_del_count := ln_del_count + 1;
                     lc_err_text := '';
                     lc_err_cd := lc_err_cd_normal;
                     ln_result_cd := ln_result_cd_del;

               EXCEPTION WHEN OTHERS THEN
                  ln_err_count := ln_err_count + 1;
                  lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                  lc_err_cd := lc_err_cd_err;
                  ln_result_cd := ln_result_cd_err;
               END;
            ELSE
               IF rec_lock IS NULL THEN
                  BEGIN
                     INSERT INTO f_taino(
                           kibetsu_key
                           , fuka_nendo
                           , soto_nendo
                           , zeimoku_cd
                           , kibetsu_cd
                           , kibetsu
                           , kojin_no
                           , tsuchisho_no
                           , jigyo_kaishi_ymd
                           , jigyo_shuryo_ymd
                           , shinkoku_cd
                           , shusei_kaisu
                           , nendo_kbn
                           , kankatsu_cd
                           , kasankin_cd
                           , tokucho_shitei_no
                           , shinkoku_rireki_no
                           , jigyo_nendo_no
                           , jido_kojin_no
                           , noki_ymd
                           , noki_kuriage_ymd
                           , shitei_noki_ymd
                           , tokusoku_ymd
                           , tokusoku_henrei_ymd
                           , tokusoku_koji_ymd
                           , tokusoku_noki_ymd
                           , saikoku_ymd
                           , saikoku_noki_ymd
                           , hotei_noki_to_ymd
                           , hotei_noki_ymd
                           , kisan_ymd
                           , kazei_kosei_ymd
                           , kosei_jiyu_cd
                           , shinkoku_ymd
                           , shusei_shinkoku_ymd
                           , kakutei_shinkoku_ymd
                           , kosei_kettei_tsuchi_ymd
                           , encho_tsuki
                           , shinkoku_kigen_ymd
                           , encho_kigen_ymd
                           , kosei_seikyu_ymd
                           , kokuzei_shinkoku_kiso_kbn
                           , kokuzei_shinkoku_ymd
                           , kosei_shinkoku_ymd
                           , jiko_yotei_ymd
                           , shometsu_yotei_ymd
                           , zeigaku
                           , tokusoku
                           , entaikin
                           , entaikin_kakutei_cd
                           , entaikin_kyosei_kbn
                           , entaikin_kyosei_ymd
                           , zeigaku_kintowari
                           , zeigaku_hojinwari
                           , zeigaku_iryo_ippan
                           , zeigaku_iryo_taisyoku
                           , zeigaku_kaigo_ippan
                           , zeigaku_kaigo_taisyoku
                           , zeigaku_shien_ippan
                           , zeigaku_shien_taisyoku
                           , zeigaku_shuno
                           , tokusoku_shuno
                           , entaikin_shuno
                           , zeigaku_kintowari_shuno
                           , zeigaku_hojinwari_shuno
                           , saishu_nikkei_ymd
                           , saishu_shuno_ymd
                           , saishu_shuno_kingaku
                           , kanno_cd
                           , kanno_ymd
                           , zeigaku_mino
                           , tokusoku_mino
                           , entaikin_mino
                           , shotokuwari
                           , fukakachiwari
                           , shihonwari
                           , shunyuwari
                           , tosho_kazeigaku
                           , jukasankin_taisho_zeigaku
                           , kanrinin_cd
                           , kanrinin_kojin_no
                           , shobun_kano_ymd
                           , noki_torai_handan_ymd
                           , kaikei_nendo
                           , kobetsu_komoku1
                           , kobetsu_komoku2
                           , kobetsu_komoku3
                           , yobi_komoku1
                           , yobi_komoku2
                           , yobi_komoku3
                           , doitsunin_kojin_no
                           , hihokensha_no
                           , kokuhokigo_no
                           , kyoyu_shisan_no
                           , shizei_jimusho_cd
                           , tsuchi_ymd
                           , koseiin_tokusoku_flg
                           , zeigaku_kotei_tochikaoku
                           , zeigaku_kotei_shokyaku
                           , zeigaku_shinrin
                           , shotokuwari_kojo
                           , shotokuwari_kanpu
                           , kojo_fusoku
                           , kojo_fusoku_nofu
                           , noki_tokurei_flg
                           , noki_tokurei_ym
                           , kazei_kbn
                           , keiji_kanri_no
                           , shadai_no
                           , keiji_shubetsu_cd
                           , sharyo_no1
                           , sharyo_no2
                           , sharyo_no3
                           , sharyo_no4
                           , shomeisho_yuko_kigen
                           , jukazei_flg
                           , kesson_ymd
                           , kesson_jiyu_cd
                           , zeigaku_kesson
                           , entaikin_kesson
                           , tokusoku_kesson
                           , kodomo_jigyosho_no
                           , ins_datetime
                           , upd_datetime
                           , upd_tantosha_cd
                           , upd_tammatsu
                           , del_flg
                           )
                        VALUES (
                           rec_f_taino.kibetsu_key
                           , rec_f_taino.fuka_nendo
                           , rec_f_taino.soto_nendo
                           , rec_f_taino.zeimoku_cd
                           , rec_f_taino.kibetsu_cd
                           , rec_f_taino.kibetsu
                           , rec_f_taino.kojin_no
                           , rec_f_taino.tsuchisho_no
                           , rec_f_taino.jigyo_kaishi_ymd
                           , rec_f_taino.jigyo_shuryo_ymd
                           , rec_f_taino.shinkoku_cd
                           , rec_f_taino.shusei_kaisu
                           , rec_f_taino.nendo_kbn
                           , rec_f_taino.kankatsu_cd
                           , rec_f_taino.kasankin_cd
                           , rec_f_taino.tokucho_shitei_no
                           , rec_f_taino.shinkoku_rireki_no
                           , rec_f_taino.jigyo_nendo_no
                           , rec_f_taino.jido_kojin_no
                           , rec_f_taino.noki_ymd
                           , rec_f_taino.noki_kuriage_ymd
                           , rec_f_taino.shitei_noki_ymd
                           , rec_f_taino.tokusoku_ymd
                           , rec_f_taino.tokusoku_henrei_ymd
                           , rec_f_taino.tokusoku_koji_ymd
                           , rec_f_taino.tokusoku_noki_ymd
                           , rec_f_taino.saikoku_ymd
                           , rec_f_taino.saikoku_noki_ymd
                           , rec_f_taino.hotei_noki_to_ymd
                           , rec_f_taino.hotei_noki_ymd
                           , rec_f_taino.kisan_ymd
                           , rec_f_taino.kazei_kosei_ymd
                           , rec_f_taino.kosei_jiyu_cd
                           , rec_f_taino.shinkoku_ymd
                           , rec_f_taino.shusei_shinkoku_ymd
                           , rec_f_taino.kakutei_shinkoku_ymd
                           , rec_f_taino.kosei_kettei_tsuchi_ymd
                           , rec_f_taino.encho_tsuki
                           , rec_f_taino.shinkoku_kigen_ymd
                           , rec_f_taino.encho_kigen_ymd
                           , rec_f_taino.kosei_seikyu_ymd
                           , rec_f_taino.kokuzei_shinkoku_kiso_kbn
                           , rec_f_taino.kokuzei_shinkoku_ymd
                           , rec_f_taino.kosei_shinkoku_ymd
                           , rec_f_taino.jiko_yotei_ymd
                           , rec_f_taino.shometsu_yotei_ymd
                           , rec_f_taino.zeigaku
                           , rec_f_taino.tokusoku
                           , rec_f_taino.entaikin
                           , rec_f_taino.entaikin_kakutei_cd
                           , rec_f_taino.entaikin_kyosei_kbn
                           , rec_f_taino.entaikin_kyosei_ymd
                           , rec_f_taino.zeigaku_kintowari
                           , rec_f_taino.zeigaku_hojinwari
                           , rec_f_taino.zeigaku_iryo_ippan
                           , rec_f_taino.zeigaku_iryo_taisyoku
                           , rec_f_taino.zeigaku_kaigo_ippan
                           , rec_f_taino.zeigaku_kaigo_taisyoku
                           , rec_f_taino.zeigaku_shien_ippan
                           , rec_f_taino.zeigaku_shien_taisyoku
                           , rec_f_taino.zeigaku_shuno
                           , rec_f_taino.tokusoku_shuno
                           , rec_f_taino.entaikin_shuno
                           , rec_f_taino.zeigaku_kintowari_shuno
                           , rec_f_taino.zeigaku_hojinwari_shuno
                           , rec_f_taino.saishu_nikkei_ymd
                           , rec_f_taino.saishu_shuno_ymd
                           , rec_f_taino.saishu_shuno_kingaku
                           , rec_f_taino.kanno_cd
                           , rec_f_taino.kanno_ymd
                           , rec_f_taino.zeigaku_mino
                           , rec_f_taino.tokusoku_mino
                           , rec_f_taino.entaikin_mino
                           , rec_f_taino.shotokuwari
                           , rec_f_taino.fukakachiwari
                           , rec_f_taino.shihonwari
                           , rec_f_taino.shunyuwari
                           , rec_f_taino.tosho_kazeigaku
                           , rec_f_taino.jukasankin_taisho_zeigaku
                           , rec_f_taino.kanrinin_cd
                           , rec_f_taino.kanrinin_kojin_no
                           , rec_f_taino.shobun_kano_ymd
                           , rec_f_taino.noki_torai_handan_ymd
                           , rec_f_taino.kaikei_nendo
                           , rec_f_taino.kobetsu_komoku1
                           , rec_f_taino.kobetsu_komoku2
                           , rec_f_taino.kobetsu_komoku3
                           , rec_f_taino.yobi_komoku1
                           , rec_f_taino.yobi_komoku2
                           , rec_f_taino.yobi_komoku3
                           , rec_f_taino.doitsunin_kojin_no
                           , rec_f_taino.hihokensha_no
                           , rec_f_taino.kokuhokigo_no
                           , rec_f_taino.kyoyu_shisan_no
                           , rec_f_taino.shizei_jimusho_cd
                           , rec_f_taino.tsuchi_ymd
                           , rec_f_taino.koseiin_tokusoku_flg
                           , rec_f_taino.zeigaku_kotei_tochikaoku
                           , rec_f_taino.zeigaku_kotei_shokyaku
                           , rec_f_taino.zeigaku_shinrin
                           , rec_f_taino.shotokuwari_kojo
                           , rec_f_taino.shotokuwari_kanpu
                           , rec_f_taino.kojo_fusoku
                           , rec_f_taino.kojo_fusoku_nofu
                           , rec_f_taino.noki_tokurei_flg
                           , rec_f_taino.noki_tokurei_ym
                           , rec_f_taino.kazei_kbn
                           , rec_f_taino.keiji_kanri_no
                           , rec_f_taino.shadai_no
                           , rec_f_taino.keiji_shubetsu_cd
                           , rec_f_taino.sharyo_no1
                           , rec_f_taino.sharyo_no2
                           , rec_f_taino.sharyo_no3
                           , rec_f_taino.sharyo_no4
                           , rec_f_taino.shomeisho_yuko_kigen
                           , rec_f_taino.jukazei_flg
                           , rec_f_taino.kesson_ymd
                           , rec_f_taino.kesson_jiyu_cd
                           , rec_f_taino.zeigaku_kesson
                           , rec_f_taino.entaikin_kesson
                           , rec_f_taino.tokusoku_kesson
                           , rec_f_taino.kodomo_jigyosho_no
                           , rec_f_taino.ins_datetime
                           , rec_f_taino.upd_datetime
                           , rec_f_taino.upd_tantosha_cd
                           , rec_f_taino.upd_tammatsu
                           , rec_f_taino.del_flg
                        );

                        ln_ins_count := ln_ins_count + 1;
                        lc_err_text := '';
                        lc_err_cd := lc_err_cd_normal;
                        ln_result_cd := ln_result_cd_add;

                  EXCEPTION WHEN OTHERS THEN
                     ln_err_count := ln_err_count + 1;
                     lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                     lc_err_cd := lc_err_cd_err;
                     ln_result_cd := ln_result_cd_err;
                  END;
               ELSE
                  BEGIN
                     UPDATE f_taino
                        SET kibetsu = rec_f_taino.kibetsu
                           , jigyo_shuryo_ymd = rec_f_taino.jigyo_shuryo_ymd
                           , noki_ymd = rec_f_taino.noki_ymd
                           , shitei_noki_ymd = rec_f_taino.shitei_noki_ymd
                           , hotei_noki_to_ymd = CASE 
                                                   WHEN ln_para15 = 0 THEN rec_main.hotei_noki_to_ymd
                                                   WHEN ln_para15 = 1 THEN f_taino.hotei_noki_to_ymd
                                                   WHEN ln_para15 = 2 THEN 
                                                      CASE 
                                                            WHEN f_taino.hotei_noki_to_ymd = 0 THEN rec_f_taino.hotei_noki_to_ymd
                                                            ELSE f_taino.hotei_noki_to_ymd
                                                      END
                                                END
                           , hotei_noki_ymd = CASE 
                                                WHEN ln_para16 = 0 THEN rec_main.hotei_noki_ymd
                                                WHEN ln_para16 = 1 THEN f_taino.hotei_noki_ymd
                                                WHEN ln_para16 = 2 THEN 
                                                   CASE 
                                                         WHEN f_taino.hotei_noki_ymd = 0 THEN rec_f_taino.hotei_noki_ymd
                                                         ELSE f_taino.hotei_noki_ymd
                                                   END
                                             END
                           , kazei_kosei_ymd = rec_f_taino.kazei_kosei_ymd
                           , kosei_jiyu_cd = rec_f_taino.kosei_jiyu_cd
                           , shinkoku_ymd = rec_f_taino.shinkoku_ymd
                           , shusei_shinkoku_ymd = rec_f_taino.shusei_shinkoku_ymd
                           , kakutei_shinkoku_ymd = rec_f_taino.kakutei_shinkoku_ymd
                           , encho_tsuki = rec_f_taino.encho_tsuki
                           , encho_kigen_ymd = rec_f_taino.encho_kigen_ymd
                           , kosei_shinkoku_ymd = rec_f_taino.kosei_shinkoku_ymd
                           , zeigaku = rec_f_taino.zeigaku
                           , tokusoku = rec_f_taino.tokusoku
                           , entaikin = rec_f_taino.entaikin
                           , entaikin_kakutei_cd = rec_f_taino.entaikin_kakutei_cd
                           , entaikin_kyosei_kbn = rec_f_taino.entaikin_kyosei_kbn
                           , entaikin_kyosei_ymd = rec_f_taino.entaikin_kyosei_ymd
                           , zeigaku_kintowari = rec_f_taino.zeigaku_kintowari
                           , zeigaku_hojinwari = rec_f_taino.zeigaku_hojinwari
                           , zeigaku_iryo_ippan = rec_f_taino.zeigaku_iryo_ippan
                           , zeigaku_iryo_taisyoku = rec_f_taino.zeigaku_iryo_taisyoku
                           , zeigaku_kaigo_ippan = rec_f_taino.zeigaku_kaigo_ippan
                           , zeigaku_kaigo_taisyoku = rec_f_taino.zeigaku_kaigo_taisyoku
                           , zeigaku_shien_ippan = rec_f_taino.zeigaku_shien_ippan
                           , zeigaku_shien_taisyoku = rec_f_taino.zeigaku_shien_taisyoku
                           , zeigaku_shuno = rec_f_taino.zeigaku_shuno
                           , tokusoku_shuno = rec_f_taino.tokusoku_shuno
                           , entaikin_shuno = rec_f_taino.entaikin_shuno
                           , zeigaku_kintowari_shuno = rec_f_taino.zeigaku_kintowari_shuno
                           , zeigaku_hojinwari_shuno = rec_f_taino.zeigaku_hojinwari_shuno
                           , saishu_nikkei_ymd = rec_f_taino.saishu_nikkei_ymd
                           , saishu_shuno_ymd = rec_f_taino.saishu_shuno_ymd
                           , kanno_cd = rec_f_taino.kanno_cd
                           , zeigaku_mino = rec_f_taino.zeigaku_mino
                           , tokusoku_mino = rec_f_taino.tokusoku_mino
                           , entaikin_mino = rec_f_taino.entaikin_mino
                           , kobetsu_komoku1 = rec_f_taino.kobetsu_komoku1
                           , doitsunin_kojin_no = rec_doitsunin.doitsunin_kojin_no
                           , hihokensha_no = rec_f_taino.hihokensha_no
                           , kokuhokigo_no = rec_f_taino.kokuhokigo_no
                           , kyoyu_shisan_no = rec_f_taino.kyoyu_shisan_no
                           , shizei_jimusho_cd = rec_f_taino.shizei_jimusho_cd
                           , koseiin_tokusoku_flg = rec_f_taino.koseiin_tokusoku_flg
                           , zeigaku_kotei_tochikaoku = rec_f_taino.zeigaku_kotei_tochikaoku
                           , zeigaku_kotei_shokyaku = rec_f_taino.zeigaku_kotei_shokyaku
                           , zeigaku_shinrin = rec_f_taino.zeigaku_shinrin
                           , shotokuwari_kojo = rec_f_taino.shotokuwari_kojo
                           , shotokuwari_kanpu = rec_f_taino.shotokuwari_kanpu
                           , kojo_fusoku = rec_f_taino.kojo_fusoku
                           , kojo_fusoku_nofu = rec_f_taino.kojo_fusoku_nofu
                           , noki_tokurei_flg = rec_f_taino.noki_tokurei_flg
                           , noki_tokurei_ym = rec_f_taino.noki_tokurei_ym
                           , kazei_kbn = rec_f_taino.kazei_kbn
                           , keiji_kanri_no = rec_f_taino.keiji_kanri_no
                           , shadai_no = rec_f_taino.shadai_no
                           , keiji_shubetsu_cd = rec_f_taino.keiji_shubetsu_cd
                           , sharyo_no1 = rec_f_taino.sharyo_no1
                           , sharyo_no2 = rec_f_taino.sharyo_no2
                           , sharyo_no3 = rec_f_taino.sharyo_no3
                           , sharyo_no4 = rec_f_taino.sharyo_no4
                           , shomeisho_yuko_kigen = rec_f_taino.shomeisho_yuko_kigen
                           , jukazei_flg = rec_f_taino.jukazei_flg
                           , kesson_ymd = rec_f_taino.kesson_ymd
                           , kesson_jiyu_cd = rec_f_taino.kesson_jiyu_cd
                           , zeigaku_kesson = rec_f_taino.zeigaku_kesson
                           , entaikin_kesson = rec_f_taino.entaikin_kesson
                           , tokusoku_kesson = rec_f_taino.tokusoku_kesson
                           , kodomo_jigyosho_no = rec_f_taino.kodomo_jigyosho_no
                           , upd_datetime = rec_f_taino.upd_datetime
                           , upd_tantosha_cd = rec_f_taino.upd_tantosha_cd
                           , upd_tammatsu = rec_f_taino.upd_tammatsu
                           , del_flg = rec_f_taino.del_flg
                        WHERE kibetsu_key = rec_f_taino.kibetsu_key;

                        ln_upd_count := ln_upd_count + 1;
                        lc_err_text := '';
                        lc_err_cd := lc_err_cd_normal;
                        ln_result_cd := ln_result_cd_upd;

                  EXCEPTION WHEN OTHERS THEN
                     ln_err_count := ln_err_count + 1;
                     lc_err_text := SUBSTRING( SQLERRM, 1, 100 );
                     lc_err_cd := lc_err_cd_err;
                     ln_result_cd := ln_result_cd_err;
                  END;
               END IF;
            END IF;
         END IF;

         -- 中間テーブルの「削除フラグ」が「1」のデータは「3：削除」を指定
         IF rec_main.del_flg::numeric = 1 THEN
            ln_result_cd := ln_result_cd_del;
         END IF;
         
         -- 中間テーブル更新
         UPDATE i_r4g_shuno 
         SET result_cd      = ln_result_cd
            , error_cd      = lc_err_cd
            , error_text    = lc_err_text
            , seq_no_renkei = in_n_renkei_seq
            , shori_ymd     = in_n_shori_ymd
         WHERE shikuchoson_cd = rec_main.shikuchoson_cd
            AND fuka_nendo = rec_main.fuka_nendo
            AND soto_nendo = rec_main.soto_nendo
            AND tsuchisho_no = rec_main.tsuchisho_no
            AND zeimoku_cd = rec_main.zeimoku_cd
            AND tokucho_shitei_no = rec_main.tokucho_shitei_no
            AND kibetsu_cd = rec_main.kibetsu_cd
            AND shinkoku_rireki_no = rec_main.shinkoku_rireki_no
            AND jigyo_nendo_no = rec_main.jigyo_nendo_no
            AND jido_atena_no = rec_main.jido_atena_no;
      END LOOP;
   CLOSE cur_main;

   IF ln_para02 = 1 THEN
      CALL proc_taino_create_index();
   END IF;

   rec_log.seq_no_renkei := in_n_renkei_seq;
   rec_log.proc_shuryo_datetime := CURRENT_TIMESTAMP;
   rec_log.proc_shori_count := ln_shori_count;
   rec_log.proc_ins_count := ln_ins_count;
   rec_log.proc_upd_count := ln_upd_count;
   rec_log.proc_del_count := ln_del_count;
   rec_log.proc_err_count := ln_err_count;

   -- データ連携ログ更新
   CALL dlgrenkei.proc_upd_log(rec_log, io_c_err_code, io_c_err_text);

   RAISE NOTICE 'レコード数: % | 登録数: % | 更新数: % | 削除数: % | エラー数: % ', ln_shori_count, ln_ins_count, ln_upd_count, ln_del_count, ln_err_count;

EXCEPTION
   WHEN OTHERS THEN
      io_c_err_code := SQLSTATE;
      io_c_err_text := SQLERRM;
      RETURN;
END;
$$;
