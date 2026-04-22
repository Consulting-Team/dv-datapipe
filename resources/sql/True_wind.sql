WITH true_wind_component AS (
    SELECT
        -vdr_relative_wind_speed * SIN(RADIANS(vdr_relative_wind_angle)) AS v_tx,
        -vdr_relative_wind_speed * COS(RADIANS(vdr_relative_wind_angle)) + vdr_aivdo_sog AS v_ty
    FROM tmp.h2521_iceberg_test
),
calc_true_wind_speed AS (
    SELECT
        SQRT(POWER(v_tx, 2) + POWER(v_ty, 2)) AS v_t
    FROM true_wind_component
)
SELECT
    vt AS true_wind_speed,
    CASE
        WHEN v_t < 1.0  THEN 0
        WHEN v_t < 4.0  THEN 1
        WHEN v_t < 7.0  THEN 2
        WHEN v_t < 11.0 THEN 3
        WHEN v_t < 17.0 THEN 4
        WHEN v_t < 22.0 THEN 5
        WHEN v_t < 28.0 THEN 6
        WHEN v_t < 34.0 THEN 7
        WHEN v_t < 41.0 THEN 8
        ELSE 9
    END AS beaufort_number
FROM calc_true_wind_speed;