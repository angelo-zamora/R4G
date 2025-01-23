CREATE OR REPLACE FUNCTION get_csv_output(in_c_renkei_cd IN numeric)
RETURNS VOID AS $$

 /**********************************************************************************************************************/
 /* 処理概要 : CSV出力処理                                                                                               */
 /* 引数　　 : in_c_renkei_cd                      … 連携コード                                                  　　　　*/
 /* 戻り値　 :                                                                                                          */
 /*--------------------------------------------------------------------------------------------------------------------*/
 /* 履歴　　 : 新規作成                                                                                                 */
 /*                                                                                                                    */
 /**********************************************************************************************************************/
DECLARE

    lc_copy_sql                                 character varying;
    lc_filepath	                                character varying;
    lc_filename 	                            character varying;
    lc_target_table 	                        character varying;
    lc_timestamp_format                         character varying;

    ln_counter                                  INT := 1; 

    file TEXT; 
    filename_format 	                        TEXT;
    file_counter                                TEXT;

BEGIN 
     -- CSVディレクトリとファイル名を取得
     SELECT directory_name, file_name, table_name
     INTO lc_filepath, lc_filename, lc_target_table
     FROM of_renkei_data
     WHERE renkei_data_cd = in_c_renkei_cd; 

     -- 特定のプロシージャSQLを取得
     lc_copy_sql := get_csv_query(in_c_renkei_cd, lc_target_table);

     --　タイムスタンプの形式セット
     lc_timestamp_format := TO_CHAR(current_timestamp, 'YYYYMMDDHH24MISS');

     -- 取得したCSV情報はNULLや空白を確認する
    IF (lc_filepath IS NOT NULL AND lc_filepath <> '') AND 
       (lc_filename IS NOT NULL AND lc_filename <> '') AND
       (lc_target_table IS NOT NULL AND lc_target_table <> '') AND
       (lc_copy_sql IS NOT NULL AND lc_copy_sql <> '') THEN

        filename_format := lc_filename || '_' || lc_timestamp_format || '_' || ln_counter || '.csv';

        -- 重複ファイルの確認
        FOR file IN SELECT * FROM pg_catalog.pg_ls_dir(lc_filepath) LOOP
            IF file = filename_format THEN
                file_counter := split_part(filename_format, '_', 3);  
                ln_counter := (split_part(file_counter, '.', 1)::integer + 1);
                EXIT;  
            END IF;
        END LOOP;

        -- CSVファイルを出力する
        lc_filepath := lc_filepath || '/' || lc_filename || '_' || lc_timestamp_format || '_' || ln_counter || '.csv';
        EXECUTE 'COPY (' || lc_copy_sql || ') TO ''' || lc_filepath || ''' DELIMITER '','' CSV HEADER ENCODING ''UTF8'';';

    ELSE
      RAISE NOTICE 'CSVファイル出力用データが取得できません。';

    END IF;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'エラーが発生しました。 SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;