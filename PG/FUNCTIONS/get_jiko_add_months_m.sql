CREATE OR REPLACE FUNCTION get_jiko_add_months_m(
    in_n_kijun_ymd numeric,
    in_n_add_month numeric,
    in_n_add_day numeric DEFAULT 0
)
RETURNS integer
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    -- 定数
    d_num_div               CONSTANT integer := 12;        -- 除数

    -- 変数
    ln_work_ymd             integer;                       -- ワーク日付（数値型）
    ln_w                    integer;                       -- 計算用ワーク変数
    ln_y                    integer;                       -- 年算出用ワーク変数
    ln_m                    integer;                       -- 月算出用ワーク変数
    ln_work_add_day         integer;                       -- ワーク加算日数
    lc_kijun_ymd            text := NULL;                  -- 基準日（文字列型）

BEGIN
    -- 引数チェック
    IF get_num_to_date(in_n_kijun_ymd) IS NULL OR in_n_add_month = 0 OR in_n_add_month IS NULL THEN
        RETURN in_n_kijun_ymd;
    END IF;

    -- メイン処理
    ln_work_add_day := 0;

    -- 基準日を文字列型(8桁)で変数にセットしておく
    lc_kijun_ymd := TO_CHAR(in_n_kijun_ymd, 'FM00000000');

    -- 基準日の月に加算月を加算する
    ln_w := TO_NUMBER(SUBSTR(lc_kijun_ymd, 5, 2), 'FM99') + in_n_add_month;

    -- 年を算出
    ln_y := TRUNC(ln_w / d_num_div);

    -- 余りを月として保持する
    ln_m := ln_w - (ln_y * 12);

    IF ln_m = 0 THEN
        ln_y := ln_y - 1;  -- 1年差引く
        ln_m := 12;        -- 月を12月にする
    END IF;

    -- 基準日の年にln_yを加算した値、ln_m、基準日の日を連結
    ln_work_ymd := TO_NUMBER(SUBSTR(lc_kijun_ymd, 1, 4)) + ln_y * 10000 + ln_m * 100 + TO_NUMBER(SUBSTR(lc_kijun_ymd, 7, 2));

    -- ln_work_ymdが日付変換できるまで-1日する
    WHILE (get_num_to_date(ln_work_ymd) IS NULL) LOOP
        ln_work_ymd := ln_work_ymd - 1;
        ln_work_add_day := 1;
    END LOOP;

    ln_work_ymd := getdatetonum(get_num_to_date(ln_work_ymd) + ln_work_add_day);

    -- 引数：in_n_add_day がある場合、加算する
    IF in_n_add_day <> 0 THEN
        ln_work_ymd := getdatetonum(get_num_to_date(ln_work_ymd) + in_n_add_day);
    END IF;

    -- データ返却
    RETURN ln_work_ymd;

END;
$$;
