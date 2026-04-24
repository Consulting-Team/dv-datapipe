# 데이터가시화 데이터파이프

데이터가시화 과제를 위한 데이터 파이프코드

| 실행 파일          | 기능                                                                               |
| :----------------- | :--------------------------------------------------------------------------------- |
| dv_datapipe.py     | 기존의 Hive 테이블에 저장된 데이터를 조회하여 새로운 iceberg 테이블을 생성 후 적재 |
| update_metadata.py | CSV 양식으로 저장된 메타데이터 postgresql 데이터베이스에 업로드                    |

## 실행 방법

**주의**: 코드 실행 전 메타데이터가 반드시 resource/csv 폴더내에 H0000_metadata.csv 형식으로 저장돼 있어야 함

### dv_datapipe.py

| 옵션    | 설명                                                         |
| ------- | ------------------------------------------------------------ |
| -h      | help                                                         |
| --hull  | 호선번호                                                     |
| --start | 데이터 조회 시작 날짜, YYYY-MM-DD                            |
| --end   | 데이터 조회 끝 날짜, YYYY-MM-DD                              |
| -c      | 테이블 생성 옵션, 초기 iceberg 테이블 생성을 위해 1회만 적용 |

```bash
# 인풋 옵션 출력
python dv_datapipe.py -h

# H2521 호선의 2026-03-01 ~ 2026-04-30 데이터 적재
# 테이블 생성 옵션(-c) 적용
python dv_datapipe.py --hull H2521 --start 2026-03-01 --end 2026-03-31 -c

# 이후 기간 데이터 적재
python dv_datapipe.py --hull H2521 --start 2026-04-01 --end 2026-04-20
```

### update_metadata.py

- hs4-dv 서버의 PostgreSQL에 저장됨
- hs4dv_service.data_visualization.metadata

hs4_service.

| 옵션  | 설명            |
| ----- | --------------- |
| -h    | help            |
| -path | 메타데이터 위치 |

```bash
# 인풋 옵션 출력
python update_metadata.py -h

# H2532 메타데이터 업로드
python update_metadata.py --path resources/csv/H2532_metadata.csv
```

## 메타데이터

Hive 테이블에서 데이터를 조회하기 위해 해당 호선에 대한 메타데이터 작성해야 함.

| hnum  |   col_name    | db_name |       table_name       |   tag    | description | unit  | data_type |
| :---: | :-----------: | :-----: | :--------------------: | :------: | :---------: | :---: | :-------: |
| H2521 | vdr_aivdo_lat |  hlng   | h2521_vdr_aivdo1_pivot | latitude |  latitude   |  deg  |   FLOAT   |

### 메타데이터 위치 (중요)

반드시 **resources/csv** 폴더내 **H0000_metadata.csv** 형식으로 저장돼 있어 함

```python
# dv_datapipe.py

# 메타데이터 읽기
metadata_list = read_metadata(f"resources/csv/{config.hull}_metadata.csv")
```

### 호선별 예외처리 (중요)

- 특정 호선의 경우 표준 태그에 대응되는 태그가 없어서 계산이 필요함
- H2508의 경우 ge1_load/ge2_load/ge3_load/ge4_load 태그 없음
- 따라서 ge1_power/ge2_power/ge3_power/ge4_power로 부터 계산해서 적재해야 함
- 메타데이터 파일에서 ge1_load/ge2_load/ge3_load/ge4_load에 해당 하는 태그 제외
- utils.metadata.py 모듈의 `apend_additional_cols` 함수에 생성이 필요한 태그 추가
- utils.metadata.py 모듈의 `calculate_additional_cols` 함수에 계산 로직 반영

### 테이블 및 데이터 파일 저장 위치

Azure 오브젝트 스토리지에 테이블과 데이터 파일 저장됨.

| 테이블     | 설명                                 | 위치                                                                             |
| ---------- | ------------------------------------ | -------------------------------------------------------------------------------- |
| H0000_raw  | Hive 테이블에서 복사해온 원본 데이터 | abfss://data@dsmecdpseoul.dfs.core.windows.net/user/hive/hocean/hs4v2/H0000/raw  |
| H0000_calc | 원본 데이터를 가공한 데이터          | abfss://data@dsmecdpseoul.dfs.core.windows.net/user/hive/hocean/hs4v2/H0000/clac |

데이터 파일(.parquet)은 각 테이블 위치의 `data` 폴더내 저장.
