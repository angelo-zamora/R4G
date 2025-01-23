--------------------------------------------------------
--  DDL for Function get_trimmed_space
--------------------------------------------------------

CREATE OR REPLACE FUNCTION get_trimmed_space ( in_c_space IN character varying  )
RETURNS character varying
LANGUAGE plpgsql
AS $$

/**********************************************************************************************************************/
/* 処理概要 : 前後の半角・全角スペース除去                                                                            */
/* 引数     : in_c_space     character varying  … スペース除去前項目                                                 */
/* 戻値     : trimmed_space     character varying … スペース除去後項目                                               */
/*--------------------------------------------------------------------------------------------------------------------*/
/* 履歴     :                                                                                                         */
/**********************************************************************************************************************/

DECLARE
   trimmed_space   character varying;

BEGIN

   IF in_c_space IS NULL THEN
      RETURN '';
   END IF;
   
   trimmed_space := RTRIM(LTRIM(in_c_space, '[ 　]'),'[ 　]');

   RETURN trimmed_space;

EXCEPTION
   WHEN OTHERS THEN
      RETURN NULL;
END;
$$;
