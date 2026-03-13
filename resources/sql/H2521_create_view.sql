DROP VIEW IF EXISTS hlng.h2521_dv_view;

CREATE VIEW
    hlng.h2521_dv_view AS
SELECT
    unix_timestamp (ias_no1.d, 'yyyy-MM-dd HH:mm') * 1000 ds_timestamp,
    ias_no1.as0006_pv,
    ias_no1.as0007_pv,
    ias_no1.as0009_pv,
    ias_no1.as0013_pv,
    ias_no1.as0014_pv,
    ias_no1.as0016_pv,
    ias_no1.ct0401_pv,
    ias_no1.ct0402_pv,
    ias_no1.ct0403_pv,
    ias_no1.ct0404_pv,
    ias_no1.ct1301_pv,
    ias_no1.ct1302_pv,
    ias_no1.ct1303_pv,
    ias_no1.ct1304_pv,
    ias_no1.ct2109_pv,
    ias_no1.gc0016_pv,
    ias_no1.gf014_daca_pv,
    ias_no1.gf022_daca_pv,
    ias_no1.gf037_daca_pv,
    ias_no1.gf045_daca_pv,
    ias_no1.gf052_01_daca_pv,
    ias_no1.gf053_01_daca_pv,
    ias_no1.gf054_01_daca_pv,
    ias_no1.gh001_daca_pv,
    ias_no1.gh002_daca_pv,
    ias_no1.gh020pc_prs_daca_pv,
    ias_no1.gx13321_pv,
    ias_no1.gx23321_pv,
    ias_no1.gx33321_pv,
    ias_no1.lm025_01_daca_pv,
    ias_no1.lm032_daca_pv,
    ias_no1.lm035_01_daca_pv,
    ias_no1.lm041_daca_pv,
    ias_no1.ob001_02_daca_pv,
    ias_no1.oe012_01_daca_pv,
    ias_no1.om013_01_daca_pv,
    ias_no1.om019_daca_pv,
    ias_no1.om020_daca_pv,
    ias_no1.om034_01_daca_pv,
    ias_no1.om040_daca_pv,
    ias_no1.om041_daca_pv,
    ias_no1.pm102_01_daca_pv,
    ias_no1.pm102_18_daca_pv,
    ias_no1.pm202_01_daca_pv,
    ias_no1.pm204_01_daca_pv,
    ias_no1.pm302_01_daca_pv,
    ias_no1.pm526_daca_pv,
    ias_no1.pm619_daca_pv,
    ias_no1.pr_fit001_01_daca_pv,
    ias_no1.pr_fit002_01_daca_pv,
    ias_no1.pr_pdit001_daca_pv,
    ias_no1.pr_pdit002_daca_pv,
    ias_no1.pr_pit001_daca_pv,
    ias_no1.pr_pit003_daca_pv,
    ias_no1.pr_pit005_daca_pv,
    ias_no1.pr_pit006_daca_pv,
    ias_no1.pr033_01_daca_pv,
    ias_no1.rg009_01_daca_pv,
    ias_no1.rg010_01_daca_pv,
    ias_no1.rg011_01_daca_pv,
    ias_no1.rg012_01_daca_pv,
    ias_no1.sg0002_pv,
    ias_no1.sg0006_pv,
    ias_no1.sg0008_pv,
    ias_no1.sg0012_pv,
    ias_no1.sg0016_pv,
    ias_no1.sg0018_pv,
    ias_no1.trimcal_rg018_pv,
    ias_no1.trimcal_rg019_pv,
    ias_no1.tx038_daca_pv,
    ias_no1.tx065_daca_pv,
    ias_no1.tx138_daca_pv,
    ias_no1.tx165_daca_pv,
    ias_no2.gf017_digacqa_pvfl,
    ias_no2.gf040_digacqa_pvfl,
    ias_no2.gx109_digacqa_pvfl,
    ias_no2.gx208_digacqa_pvfl,
    ias_no2.gx209_digacqa_pvfl,
    ias_no2.gx308_digacqa_pvfl,
    ias_no2.gx309_digacqa_pvfl,
    ias_no2.om_om002_02_digacqa_pvfl,
    ias_no2.om_om104_02_digacqa_pvfl,
    ias_no2.om_om204_02_digacqa_pvfl,
    ias_no2.om_om208_02_digacqa_pvfl,
    ias_no2.sg016_digacqa_pvfl,
    ias_no2.sg017_digacqa_pvfl,
    ias_no3.ob1003_00_pv,
    ias_no3.ob1003_01_pv,
    ias_no3.ob2003_00_pv,
    ias_no3.ob2003_01_pv,
    ias_no3.pm511sg_devctla_pv,
    ias_no3.pm611sg_devctla_pv,
    ias_no4.as0001_1_pv,
    ias_no4.as0001_3_pv,
    agrsa.port_rudder_sensor,
    agrsa.starboard_rudder_sensor,
    aivdo1.course_over_ground,
    aivdo1.latitude,
    aivdo1.longitude,
    aivdo1.speed_over_ground,
    hehdt.heading,
    sddpt.depth,
    vdvbw.longitudinal_water_speed,
    vdvbw.stern_traverse_ground_speed,
    vdvbw.stern_traverse_water_speed,
    vdvbw.traverse_water_speed,
    yxmwv.wind_angle,
    yxmwv.wind_speed,
    ias_no1.ds_date
FROM
    (
        SELECT
            from_unixtime (
                CAST(
                    floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                ) * 60,
                'yyyy-MM-dd HH:mm'
            ) d,
            row_number() OVER (
                PARTITION BY
                    ds_date,
                    from_unixtime (
                        CAST(
                            floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                        ) * 60,
                        'yyyy-MM-dd HH:mm'
                    )
                ORDER BY
                    ds_timestamp ASC
            ) rank,
            as0006_pv,
            as0007_pv,
            as0009_pv,
            as0013_pv,
            as0014_pv,
            as0016_pv,
            ct0401_pv,
            ct0402_pv,
            ct0403_pv,
            ct0404_pv,
            ct1301_pv,
            ct1302_pv,
            ct1303_pv,
            ct1304_pv,
            ct2109_pv,
            gc0016_pv,
            gf014_daca_pv,
            gf022_daca_pv,
            gf037_daca_pv,
            gf045_daca_pv,
            gf052_01_daca_pv,
            gf053_01_daca_pv,
            gf054_01_daca_pv,
            gh001_daca_pv,
            gh002_daca_pv,
            gh020pc_prs_daca_pv,
            gx13321_pv,
            gx23321_pv,
            gx33321_pv,
            lm025_01_daca_pv,
            lm032_daca_pv,
            lm035_01_daca_pv,
            lm041_daca_pv,
            ob001_02_daca_pv,
            oe012_01_daca_pv,
            om013_01_daca_pv,
            om019_daca_pv,
            om020_daca_pv,
            om034_01_daca_pv,
            om040_daca_pv,
            om041_daca_pv,
            pm102_01_daca_pv,
            pm102_18_daca_pv,
            pm202_01_daca_pv,
            pm204_01_daca_pv,
            pm302_01_daca_pv,
            pm526_daca_pv,
            pm619_daca_pv,
            pr_fit001_01_daca_pv,
            pr_fit002_01_daca_pv,
            pr_pdit001_daca_pv,
            pr_pdit002_daca_pv,
            pr_pit001_daca_pv,
            pr_pit003_daca_pv,
            pr_pit005_daca_pv,
            pr_pit006_daca_pv,
            pr033_01_daca_pv,
            rg009_01_daca_pv,
            rg010_01_daca_pv,
            rg011_01_daca_pv,
            rg012_01_daca_pv,
            sg0002_pv,
            sg0006_pv,
            sg0008_pv,
            sg0012_pv,
            sg0016_pv,
            sg0018_pv,
            trimcal_rg018_pv,
            trimcal_rg019_pv,
            tx038_daca_pv,
            tx065_daca_pv,
            tx138_daca_pv,
            tx165_daca_pv,
            ds_date
        FROM
            hlng.h2521_ias_ias_no1_pivot
    ) ias_no1
    FULL JOIN (
        SELECT
            from_unixtime (
                CAST(
                    floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                ) * 60,
                'yyyy-MM-dd HH:mm'
            ) d,
            row_number() OVER (
                PARTITION BY
                    ds_date,
                    from_unixtime (
                        CAST(
                            floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                        ) * 60,
                        'yyyy-MM-dd HH:mm'
                    )
                ORDER BY
                    ds_timestamp ASC
            ) rank,
            gf017_digacqa_pvfl,
            gf040_digacqa_pvfl,
            gx109_digacqa_pvfl,
            gx208_digacqa_pvfl,
            gx209_digacqa_pvfl,
            gx308_digacqa_pvfl,
            gx309_digacqa_pvfl,
            om_om002_02_digacqa_pvfl,
            om_om104_02_digacqa_pvfl,
            om_om204_02_digacqa_pvfl,
            om_om208_02_digacqa_pvfl,
            sg016_digacqa_pvfl,
            sg017_digacqa_pvfl,
            ds_date
        FROM
            hlng.h2521_ias_ias_no2_pivot
    ) ias_no2 ON ias_no1.ds_date = ias_no2.ds_date
    AND ias_no1.d = ias_no2.d
    FULL JOIN (
        SELECT
            from_unixtime (
                CAST(
                    floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                ) * 60,
                'yyyy-MM-dd HH:mm'
            ) d,
            row_number() OVER (
                PARTITION BY
                    ds_date,
                    from_unixtime (
                        CAST(
                            floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                        ) * 60,
                        'yyyy-MM-dd HH:mm'
                    )
                ORDER BY
                    ds_timestamp ASC
            ) rank,
            ob1003_00_pv,
            ob1003_01_pv,
            ob2003_00_pv,
            ob2003_01_pv,
            pm511sg_devctla_pv,
            pm611sg_devctla_pv,
            ds_date
        FROM
            hlng.h2521_ias_ias_no3_pivot
    ) ias_no3 ON ias_no1.ds_date = ias_no3.ds_date
    AND ias_no1.d = ias_no3.d
    FULL JOIN (
        SELECT
            from_unixtime (
                CAST(
                    floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                ) * 60,
                'yyyy-MM-dd HH:mm'
            ) d,
            row_number() OVER (
                PARTITION BY
                    ds_date,
                    from_unixtime (
                        CAST(
                            floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                        ) * 60,
                        'yyyy-MM-dd HH:mm'
                    )
                ORDER BY
                    ds_timestamp ASC
            ) rank,
            as0001_1_pv,
            as0001_3_pv,
            ds_date
        FROM
            hlng.h2521_ias_ias_no4_pivot
    ) ias_no4 ON ias_no1.ds_date = ias_no4.ds_date
    AND ias_no1.d = ias_no4.d
    FULL JOIN (
        SELECT
            from_unixtime (
                CAST(
                    floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                ) * 60,
                'yyyy-MM-dd HH:mm'
            ) d,
            row_number() OVER (
                PARTITION BY
                    ds_date,
                    from_unixtime (
                        CAST(
                            floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                        ) * 60,
                        'yyyy-MM-dd HH:mm'
                    )
                ORDER BY
                    ds_timestamp ASC
            ) rank,
            heading,
            ds_date
        FROM
            hlng.h2521_vdr_hehdt_pivot
    ) hehdt ON ias_no1.ds_date = hehdt.ds_date
    AND ias_no1.d = hehdt.d
    FULL JOIN (
        SELECT
            from_unixtime (
                CAST(
                    floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                ) * 60,
                'yyyy-MM-dd HH:mm'
            ) d,
            row_number() OVER (
                PARTITION BY
                    ds_date,
                    from_unixtime (
                        CAST(
                            floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                        ) * 60,
                        'yyyy-MM-dd HH:mm'
                    )
                ORDER BY
                    ds_timestamp ASC
            ) rank,
            depth,
            ds_date
        FROM
            hlng.h2521_vdr_sddpt_pivot
    ) sddpt ON ias_no1.ds_date = sddpt.ds_date
    AND ias_no1.d = sddpt.d
    FULL JOIN (
        SELECT
            from_unixtime (
                CAST(
                    floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                ) * 60,
                'yyyy-MM-dd HH:mm'
            ) d,
            row_number() OVER (
                PARTITION BY
                    ds_date,
                    from_unixtime (
                        CAST(
                            floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                        ) * 60,
                        'yyyy-MM-dd HH:mm'
                    )
                ORDER BY
                    ds_timestamp ASC
            ) rank,
            longitudinal_water_speed,
            stern_traverse_ground_speed,
            stern_traverse_water_speed,
            traverse_water_speed,
            ds_date
        FROM
            hlng.h2521_vdr_vdvbw_pivot
    ) vdvbw ON ias_no1.ds_date = vdvbw.ds_date
    AND ias_no1.d = vdvbw.d
    FULL JOIN (
        SELECT
            from_unixtime (
                CAST(
                    floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                ) * 60,
                'yyyy-MM-dd HH:mm'
            ) d,
            row_number() OVER (
                PARTITION BY
                    ds_date,
                    from_unixtime (
                        CAST(
                            floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                        ) * 60,
                        'yyyy-MM-dd HH:mm'
                    )
                ORDER BY
                    ds_timestamp ASC
            ) rank,
            wind_angle,
            wind_speed,
            ds_date
        FROM
            hlng.h2521_vdr_yxmwv_pivot
    ) yxmwv ON ias_no1.ds_date = yxmwv.ds_date
    AND ias_no1.d = yxmwv.d
    FULL JOIN (
        SELECT
            from_unixtime (
                CAST(
                    floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                ) * 60,
                'yyyy-MM-dd HH:mm'
            ) d,
            row_number() OVER (
                PARTITION BY
                    ds_date,
                    from_unixtime (
                        CAST(
                            floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                        ) * 60,
                        'yyyy-MM-dd HH:mm'
                    )
                ORDER BY
                    ds_timestamp ASC
            ) rank,
            course_over_ground,
            latitude,
            longitude,
            speed_over_ground,
            ds_date
        FROM
            hlng.h2521_vdr_aivdo1_pivot
    ) aivdo1 ON ias_no1.ds_date = aivdo1.ds_date
    AND ias_no1.d = aivdo1.d
    FULL JOIN (
        SELECT
            from_unixtime (
                CAST(
                    floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                ) * 60,
                'yyyy-MM-dd HH:mm'
            ) d,
            row_number() OVER (
                PARTITION BY
                    ds_date,
                    from_unixtime (
                        CAST(
                            floor(CAST(ds_timestamp AS BIGINT) DIV 1000 DIV 60) AS INT
                        ) * 60,
                        'yyyy-MM-dd HH:mm'
                    )
                ORDER BY
                    ds_timestamp ASC
            ) rank,
            port_rudder_sensor,
            starboard_rudder_sensor,
            ds_date
        FROM
            hlng.h2521_vdr_agrsa_pivot
    ) agrsa ON ias_no1.ds_date = agrsa.ds_date
    AND ias_no1.d = agrsa.d
WHERE
    ias_no1.rank = 1
    AND ias_no2.rank = 1
    AND ias_no3.rank = 1
    AND ias_no4.rank = 1
    AND hehdt.rank = 1
    AND sddpt.rank = 1
    AND vdvbw.rank = 1
    AND yxmwv.rank = 1
    AND aivdo1.rank = 1
    AND agrsa.rank = 1;
