


-- Reviewed test write SQL. Target table is an approved temporary validation table.
WITH 
filtered_base AS (
  SELECT *
  FROM dwd_dc_ep.dwd_gs_company_base_info_all base
  WHERE  base.company_type_code = '01'
),
dedup_mid AS (
  SELECT *
  FROM (
    SELECT
      mid.*,
      row_number() OVER (
        PARTITION BY mid.v2_company_id
        ORDER BY mid.update_time DESC, mid.insert_time DESC, mid.dwi_company_id DESC
      ) AS rn
    FROM dwd_dc_ep.dwd_gs_company_base_info_mid_dwi_all mid
    WHERE mid.dwi_company_id IS NOT NULL
      AND mid.dwi_company_id <> ''
      AND mid.v2_company_id IS NOT NULL
      AND mid.v2_company_id <> ''
  ) t
  WHERE rn = 1
),
transformed AS (
  SELECT
    CAST(NULL AS STRING) AS key_no,
    CAST(mid.dwi_company_id AS STRING) AS company_id,
    CAST(base.company_name AS STRING) AS company_name,
    CAST(base.oper_key_no AS STRING) AS oper_key_no,
    CAST(base.oper_name AS STRING) AS oper_name,
    concat(CAST(base.regist_capi AS STRING), CAST(base.regist_capi_curr_name AS STRING)) AS regist_capi,
    CAST(base.regist_capi AS STRING) AS regist_capi_value,
    CAST(base.regist_capi_curr_name AS STRING) AS regist_capi_unit,
    CAST(base.rec_capi AS STRING) AS rec_cap,
    CAST(base.company_status_name AS STRING) AS status,
    CAST(base.start_date AS STRING) AS start_date,
    CAST(base.end_date AS STRING) AS end_date,
    CAST(base.credit_code AS STRING) AS credit_code,
    CAST(base.no AS STRING) AS no,
    CAST(base.econ_kind AS STRING) AS econ_kind,
    CAST(base.econ_kind_code AS STRING) AS econ_kind_code,
    CAST(base.check_date AS STRING) AS check_date,
    CAST(base.belong_org AS STRING) AS belong_org,
    CAST(base.province_code AS STRING) AS province_code,
    CAST(base.province_name AS STRING) AS province,
    CAST(base.term_start AS STRING) AS term_start,
    CAST(base.term_end AS STRING) AS term_end,
    CAST(base.address AS STRING) AS address,
    CAST(base.scope AS STRING) AS scope,
    CAST(base.phone_number AS STRING) AS phone_number,
    CAST(base.update_time AS STRING) AS updated_date,
    CAST(current_timestamp() AS STRING) AS dates,
    CASE
      WHEN base.is_del = '1' THEN '-1'
      WHEN to_date(base.insert_time) = to_date(base.update_time) THEN '1'
      ELSE '0'
    END AS isadd,
    row_number() OVER (
      PARTITION BY mid.dwi_company_id
      ORDER BY base.update_time DESC, base.insert_time DESC, base.company_id DESC
    ) AS target_rn
  FROM filtered_base base
  JOIN dedup_mid mid
    ON base.company_id = mid.v2_company_id

)
INSERT OVERWRITE TABLE tmp_dc_ep.t_eci_company_4_dwi
SELECT
  key_no,
  company_id,
  company_name,
  oper_key_no,
  oper_name,
  regist_capi,
  regist_capi_value,
  regist_capi_unit,
  rec_cap,
  status,
  start_date,
  end_date,
  credit_code,
  no,
  econ_kind,
  econ_kind_code,
  check_date,
  belong_org,
  province_code,
  province,
  term_start,
  term_end,
  address,
  scope,
  phone_number,
  updated_date,
  dates,
  isadd
FROM transformed
WHERE target_rn = 1;