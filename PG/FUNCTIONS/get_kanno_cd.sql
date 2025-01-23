--------------------------------------------------------
--  DDL for Function get_kanno_cd
--------------------------------------------------------

CREATE OR REPLACE FUNCTION get_kanno_cd (
   in_n_zeigaku IN numeric
 , in_n_tokusoku IN numeric
 , in_n_entaikin IN numeric
 , in_n_zeigaku_shuno IN numeric
 , in_n_tokusoku_shuno IN numeric
 , in_n_entaikin_shuno IN numeric
)
RETURNS numeric
LANGUAGE plpgsql
AS $$
/**********************************************************************************************************************/
/* 処理概要 : 同一人・共有者 登録情報取得設定                                                                         */
/* 引数　　 : in_n_zeigaku        … （調定）税額                                                                     */
/*            in_n_tokusoku       … （調定）督促手数料                                                               */
/*            in_n_entaikin       … （調定）延滞金                                                                   */
/*            in_n_zeigaku_shuno  … （収納）税額                                                                     */
/*            in_n_tokusoku_shuno … （収納）督促手数料                                                               */
/*            in_n_entaikin_shuno … （収納）延滞金                                                                   */
/* 戻り値　 : 完納フラグ                                                                                              */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴　　 : Ver.1.01.001    2014/04/17   (AIC) T.Yamauchi             新規作成                                      */
/**********************************************************************************************************************/

BEGIN
   IF in_n_zeigaku - in_n_zeigaku_shuno <> 0 THEN
      RETURN 0;
   ELSIF in_n_zeigaku - in_n_zeigaku_shuno = 0 AND in_n_tokusoku - in_n_tokusoku_shuno <> 0 AND in_n_entaikin - in_n_entaikin_shuno <> 0 THEN
      RETURN 1;
   ELSIF in_n_zeigaku - in_n_zeigaku_shuno = 0 AND in_n_tokusoku - in_n_tokusoku_shuno = 0 AND in_n_entaikin - in_n_entaikin_shuno <> 0 THEN
      RETURN 2;
   ELSIF in_n_zeigaku - in_n_zeigaku_shuno = 0 AND in_n_tokusoku - in_n_tokusoku_shuno <> 0 AND in_n_entaikin - in_n_entaikin_shuno = 0 THEN
      RETURN 3;
   ELSIF in_n_zeigaku - in_n_zeigaku_shuno = 0 AND in_n_tokusoku - in_n_tokusoku_shuno = 0 AND in_n_entaikin - in_n_entaikin_shuno = 0 THEN
      RETURN 4;
   ELSE
      RETURN 0;
   END IF;

END;
$$;
