데이터 모델
==========

MongoDB, PostgreSQL, Redis 전체 데이터 구조와 컬렉션 간 관계를 정리한 문서.

---

## 목차

1. [전체 구조 개요](#전체-구조-개요)
2. [MongoDB 컬렉션](#mongodb-컬렉션)
   - [핵심 컬렉션](#핵심-컬렉션)
   - [리뷰/승인 컬렉션](#리뷰승인-컬렉션)
   - [Staging 컬렉션](#staging-컬렉션)
   - [Production 컬렉션](#production-컬렉션)
   - [거버넌스 컬렉션](#거버넌스-컬렉션)
   - [자가치유 컬렉션](#자가치유-컬렉션)
   - [인증 컬렉션](#인증-컬렉션)
3. [PostgreSQL 테이블](#postgresql-테이블)
   - [공통코드](#공통코드)
   - [소스 마스터](#소스-마스터)
   - [도메인 데이터 테이블](#도메인-데이터-테이블)
   - [승격 이력](#승격-이력)
4. [Redis 캐시 패턴](#redis-캐시-패턴)
5. [데이터 흐름](#데이터-흐름)
6. [관계 및 제약조건](#관계-및-제약조건)
7. [보존 정책](#보존-정책)

---

## 전체 구조 개요

```
┌──────────────────────────────────────────────────────────────┐
│ MongoDB (애플리케이션 데이터)                                   │
│                                                              │
│  [핵심]  sources, crawlers, crawl_results, error_logs        │
│  [리뷰]  data_reviews                                        │
│  [스테이징]  staging_news, staging_financial, staging_data    │
│  [프로덕션]  news_articles, financial_data, stock_prices,    │
│              exchange_rates, market_indices, announcements,   │
│              crawl_data                                       │
│  [거버넌스]  data_lineage, data_contracts, schema_registry,  │
│              data_catalog, data_versions                      │
│  [자가치유]  healing_sessions, wellknown_cases,               │
│              healing_schedules                                │
│  [인증]  auth_configs, auth_credentials, auth_sessions       │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│ PostgreSQL (Airflow 메타데이터 + 프로덕션 듀얼라이트)            │
│                                                              │
│  [Airflow]  dag_run, task_instance, xcom 등 (내부 관리)      │
│  [공통코드]  tb_common_code_group, tb_common_code            │
│  [소스]  tb_source_master, tb_source_field                   │
│  [데이터]  tb_data_news, tb_data_finance,                    │
│            tb_data_announcement, tb_data_generic             │
│  [이력]  tb_promotion_log                                    │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│ Redis (캐시 + Rate Limit)                                    │
│                                                              │
│  cache:dashboard:stats (60초)                                │
│  cache:sources:list (30초)                                   │
│  cache:source:{id} (60초)                                    │
│  cache:monitoring:health (15초)                              │
│  cache:errors:recent (30초)                                  │
│  ratelimit:{ip} (Rate Limit 카운터)                          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## MongoDB 컬렉션

### 핵심 컬렉션

#### sources

크롤링 소스 정의. URL, 추출 필드, 스케줄 등 소스의 모든 설정을 저장.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| name | String | 소스 이름 (Unique) |
| url | String | 크롤링 대상 URL |
| type | String | 소스 유형 (`html`, `pdf`, `excel`, `csv`, `api`) |
| fields | Array | 추출 필드 정의 목록 |
| fields[].name | String | 필드 이름 |
| fields[].selector | String | CSS 셀렉터 또는 XPath |
| fields[].data_type | String | 데이터 타입 (`string`, `number`, `date`, `boolean`, `currency`) |
| fields[].is_list | Boolean | 목록 여부 |
| fields[].attribute | String | HTML 속성 (선택) |
| fields[].pattern | String | 정규식 패턴 (선택) |
| schedule | String | Cron 표현식 |
| status | String | 상태 (`active`, `inactive`, `error`) |
| last_run | ISODate | 마지막 실행 시각 |
| last_success | ISODate | 마지막 성공 시각 |
| error_count | Integer | 연속 에러 횟수 (기본 0) |
| created_at | ISODate | 생성 시각 |
| updated_at | ISODate | 수정 시각 |

**인덱스**: `{name: 1}` UNIQUE, `{status: 1, created_at: -1}`, `{type: 1}`, `{created_at: -1}`

---

#### crawlers

GPT가 자동 생성한 크롤러 코드와 메타데이터.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| source_id | ObjectId | FK → sources._id |
| code | String | Python 크롤러 소스코드 |
| version | Integer | 코드 버전 (자동 증가) |
| status | String | 상태 (`active`, `testing`, `deprecated`) |
| dag_id | String | Airflow DAG ID (Sparse Unique) |
| created_at | ISODate | 생성 시각 |
| created_by | String | 생성 주체 (`gpt`, `manual`) |

**인덱스**: `{source_id: 1}`, `{dag_id: 1}` UNIQUE SPARSE, `{status: 1}`, `{created_at: -1}`

---

#### crawler_history

크롤러 코드 변경 이력. 자가치유로 인한 코드 수정 내역 추적.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| crawler_id | ObjectId | FK → crawlers._id |
| version | Integer | 버전 번호 |
| code | String | 해당 버전의 전체 소스코드 |
| change_reason | String | 변경 사유 (`error_recovery`, `structure_change`, `manual_edit`, `rollback`) |
| change_detail | String | 상세 변경 내역 (선택) |
| changed_at | ISODate | 변경 시각 |
| changed_by | String | 변경 주체 (`gpt`, `user`, `system`) |

**인덱스**: `{crawler_id: 1, version: 1}` UNIQUE, `{changed_at: -1}`

---

#### crawl_results

크롤링 실행 결과. 추출된 데이터와 실행 상태를 저장.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| source_id | ObjectId | FK → sources._id |
| crawler_id | ObjectId | FK → crawlers._id |
| run_id | String | Airflow 실행 ID |
| status | String | 결과 (`success`, `failed`, `partial`) |
| data | Array | 추출된 레코드 배열 |
| record_count | Integer | 추출 건수 |
| error_code | String | 에러 코드 (실패 시) |
| error_message | String | 에러 메시지 (실패 시) |
| execution_time_ms | Integer | 실행 시간 (ms) |
| executed_at | ISODate | 실행 시각 |

**인덱스**: `{source_id: 1, executed_at: -1}`, `{executed_at: -1}`, `{status: 1}`, `{run_id: 1}`
**TTL**: `{executed_at: 1}` → 90일 (7,776,000초)

---

#### error_logs

에러 로그. 모든 크롤링 오류와 해결 상태를 기록.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| source_id | ObjectId | FK → sources._id |
| crawler_id | ObjectId | FK → crawlers._id (선택) |
| run_id | String | Airflow 실행 ID (선택) |
| error_code | String | 표준 에러 코드 (E001~E010) |
| error_type | String | 분류 (`network`, `parsing`, `validation`, `system`) |
| message | String | 에러 메시지 |
| stack_trace | String | Python 스택 트레이스 (선택) |
| auto_recoverable | Boolean | 자동 복구 가능 여부 |
| resolved | Boolean | 해결 여부 |
| resolved_at | ISODate | 해결 시각 |
| resolution_method | String | 해결 방법 (`auto`, `manual`, `retry`) |
| resolution_detail | String | 해결 상세 내역 |
| created_at | ISODate | 생성 시각 |

**인덱스**: `{source_id: 1, created_at: -1}`, `{error_code: 1}`, `{resolved: 1}`, `{created_at: -1}`

---

### 리뷰/승인 컬렉션

#### data_reviews

크롤링 데이터 리뷰/승인 워크플로우. 추출된 각 레코드에 대한 검토 상태를 관리.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| crawl_result_id | ObjectId | FK → crawl_results._id |
| source_id | ObjectId | FK → sources._id |
| data_record_index | Integer | crawl_result 내 레코드 인덱스 |
| review_status | String | 리뷰 상태 (아래 참조) |
| reviewer_id | String | 리뷰어 ID |
| reviewed_at | ISODate | 리뷰 시각 |
| original_data | Object | 원본 데이터 (필드:값 쌍) |
| corrected_data | Object | 수정된 데이터 (선택) |
| corrections | Array | 필드별 수정 내역 |
| corrections[].field | String | 수정 필드명 |
| corrections[].original_value | Any | 원래 값 |
| corrections[].corrected_value | Any | 수정 값 |
| corrections[].reason | String | 수정 사유 |
| confidence_score | Float | 신뢰도 점수 (0~1) |
| ocr_confidence | Float | OCR 신뢰도 (선택) |
| ai_confidence | Float | AI 신뢰도 (선택) |
| needs_number_review | Boolean | 숫자 데이터 검증 필요 여부 |
| notes | String | 리뷰어 메모 |
| created_at | ISODate | 생성 시각 |
| updated_at | ISODate | 수정 시각 |

**review_status 값**:
- `pending` → 리뷰 대기
- `approved` → 승인 (프로덕션 승격 대상)
- `on_hold` → 보류 (수동 확인 필요)
- `needs_correction` → 수정 필요
- `corrected` → 수정 완료 (프로덕션 승격 대상)

**인덱스**: `{review_status: 1, created_at: -1}`, `{review_status: 1, confidence_score: 1}`, `{source_id: 1, review_status: 1}`, `{crawl_result_id: 1, data_record_index: 1}`

---

### Staging 컬렉션

크롤링 결과를 도메인별로 분류해 임시 저장하는 중간 레이어. 리뷰 승인 전까지 여기에 보관.

모든 staging 컬렉션은 아래 **공통 메타데이터 필드**를 포함:

| 필드 | 타입 | 설명 |
|------|------|------|
| _source_id | ObjectId | FK → sources._id |
| _crawl_result_id | ObjectId | FK → crawl_results._id |
| _record_index | Integer | crawl_result 내 순서 |
| _review_status | String | 리뷰 상태 |
| _collection_type | String | 도메인 분류 |
| _crawled_at | ISODate | 크롤링 시각 |
| _staged_at | ISODate | 스테이징 시각 |

#### staging_news

| 필드 | 타입 | 설명 |
|------|------|------|
| title | String | 기사 제목 |
| content | String | 본문 |
| summary | String | 요약 |
| author | String | 작성자 |
| press | String | 언론사 |
| published_at | ISODate | 발행일 |
| category | String | 카테고리 |
| url | String | 원문 URL |
| image_url | String | 대표 이미지 URL |
| tags | Array | 태그 목록 |

#### staging_financial

| 필드 | 타입 | 설명 |
|------|------|------|
| ticker | String | 종목 코드 |
| name | String | 종목명 |
| price | Decimal | 현재가 |
| open_price | Decimal | 시가 |
| high_price | Decimal | 고가 |
| low_price | Decimal | 저가 |
| close_price | Decimal | 종가 |
| volume | Integer | 거래량 |
| change_amount | Decimal | 전일 대비 |
| change_rate | Decimal | 등락률 |
| market_cap | Decimal | 시가총액 |
| market | String | 시장 (KOSPI, KOSDAQ 등) |
| currency | String | 통화 |
| trading_date | Date | 거래일 |

#### staging_data

범용 스테이징. 뉴스/금융 외 데이터를 유연한 구조로 저장.

| 필드 | 타입 | 설명 |
|------|------|------|
| data_json | Object | 범용 데이터 (자유 구조) |
| category_code | String | 분류 코드 |
| title | String | 제목 |
| url | String | 원본 URL |

---

### Production 컬렉션

리뷰 승인된 데이터가 최종 저장되는 컬렉션. staging에서 승격(promote)되어 들어옴.

모든 production 컬렉션은 아래 **검증 메타데이터 필드**를 추가로 포함:

| 필드 | 타입 | 설명 |
|------|------|------|
| _staging_id | ObjectId | 원본 staging 레코드 ID |
| _verified | Boolean | 검증 완료 여부 |
| _verified_at | ISODate | 검증 시각 |
| _verified_by | String | 검증자 |
| _has_corrections | Boolean | 수정 이력 존재 여부 |
| _promoted_at | ISODate | 프로덕션 승격 시각 |
| _data_date | String | 데이터 기준일 (ISO) |

#### news_articles

뉴스 기사 프로덕션 데이터. staging_news와 동일 필드 + 검증 메타데이터.

#### financial_data

금융 데이터 프로덕션. staging_financial과 동일 필드 + 검증 메타데이터.

#### stock_prices

주가 시계열 데이터.

**인덱스**: `{stock_code: 1, _data_date: 1}` UNIQUE SPARSE, `{trade_date: -1}`

#### exchange_rates

환율 데이터. TTL 365일.

| 필드 | 타입 | 설명 |
|------|------|------|
| currency_code | String | 통화 코드 |
| rate | Decimal | 환율 |
| base_currency | String | 기준 통화 |
| _data_date | String | 기준일 |

**인덱스**: `{currency_code: 1, _data_date: 1}` UNIQUE SPARSE

#### market_indices

시장지수 데이터. TTL 365일.

| 필드 | 타입 | 설명 |
|------|------|------|
| index_code | String | 지수 코드 |
| index_name | String | 지수 이름 |
| value | Decimal | 지수 값 |
| change | Decimal | 전일 대비 |
| change_rate | Decimal | 등락률 |
| _data_date | String | 기준일 |

**인덱스**: `{index_code: 1, _data_date: 1}` UNIQUE SPARSE

#### announcements

공시 데이터. TTL 365일.

| 필드 | 타입 | 설명 |
|------|------|------|
| company | String | 회사명 |
| company_code | String | 회사 코드 |
| title | String | 공시 제목 |
| content | String | 공시 내용 |
| filing_date | Date | 공시일 |
| category | String | 분류 |
| sub_category | String | 세부 분류 |
| market | String | 시장 |
| url | String | 원문 URL |
| attachments | Array | 첨부파일 목록 |

**인덱스**: `{company_code: 1}`, `{filing_date: -1}`, `{category: 1}`, `{content_hash: 1}` UNIQUE SPARSE
**TTL**: `{_crawled_at: 1}` → 365일

#### crawl_data

범용 프로덕션 데이터. staging_data에서 승격. TTL 90일.

**인덱스**: `{_source_id: 1, _data_date: -1}`, `{_crawled_at: -1}`, `{data_json}` GIN

---

### 거버넌스 컬렉션

#### data_lineage

staging → production 이동 이력. 감사 추적용.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| staging_id | ObjectId | 원본 staging 레코드 |
| staging_collection | String | staging 컬렉션명 |
| production_id | ObjectId | production 레코드 |
| production_collection | String | production 컬렉션명 |
| source_id | ObjectId | FK → sources._id |
| crawl_result_id | ObjectId | FK → crawl_results._id |
| reviewer_id | String | 리뷰어 |
| has_corrections | Boolean | 수정 여부 |
| corrections | Array | 수정 내역 |
| moved_at | ISODate | 이동 시각 |

#### schema_registry

스키마 버전 관리. 소스별 데이터 구조 변경을 추적.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| source_id | String | 소스 또는 컬렉션 식별자 |
| version | Integer | 스키마 버전 |
| schema | Object | 스키마 정의 (필드 구조) |
| fingerprint | String | 스키마 해시 |
| created_at | ISODate | 생성 시각 |
| created_by | String | 생성 주체 |
| change_description | String | 변경 설명 |
| is_active | Boolean | 활성 여부 |
| compatibility_mode | String | 호환성 모드 (`BACKWARD`, `FORWARD`, `FULL`) |
| tags | Array | 태그 |

**인덱스**: `{source_id: 1, version: -1}`, `{source_id: 1, is_active: 1}`, `{fingerprint: 1}`

#### data_catalog

데이터셋 메타데이터 레지스트리.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| name | String | 데이터셋 이름 |
| display_name | String | 표시 이름 |
| description | String | 설명 |
| dataset_type | String | 유형 (`SOURCE`, `STAGING`, `FINAL`) |
| domain | String | 도메인 |
| owner | String | 소유자 |
| columns | Array | 컬럼 정의 배열 |
| columns[].name | String | 컬럼 이름 |
| columns[].type | String | 데이터 타입 |
| columns[].description | String | 컬럼 설명 |
| columns[].nullable | Boolean | NULL 허용 여부 |
| tags | Array | 태그 (name, category 쌍) |
| quality_metrics | Object | 품질 지표 |
| sensitivity_level | String | 민감도 (`PUBLIC`, `INTERNAL`, `CONFIDENTIAL`) |
| created_at | ISODate | 생성 시각 |
| updated_at | ISODate | 수정 시각 |

#### data_contracts

품질 계약 정의. 데이터셋별 SLA와 품질 기준을 관리.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| dataset_id | ObjectId | FK → data_catalog._id |
| contract_version | Integer | 계약 버전 |
| schema_requirements | Object | 스키마 요구사항 |
| sla.freshness_hours | Integer | 데이터 신선도 (시간) |
| sla.quality_threshold | Float | 품질 임계값 (0~1) |
| sla.availability_target | Float | 가용성 목표 (0~1) |
| monitors | Array | 모니터링 규칙 |
| created_at | ISODate | 생성 시각 |
| enforced_at | ISODate | 적용 시각 |

---

### 자가치유 컬렉션

#### healing_sessions

자동 에러 복구 세션. 실패 감지부터 해결까지 전 과정을 추적.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| session_id | String | 세션 식별자 (Unique) |
| source_id | ObjectId | FK → sources._id |
| crawler_id | ObjectId | FK → crawlers._id |
| error_code | String | 에러 코드 |
| error_message | String | 에러 메시지 |
| stack_trace | String | 스택 트레이스 |
| status | String | 상태 (아래 참조) |
| diagnosis | Object | 진단 정보 |
| matched_case | ObjectId | FK → wellknown_cases._id |
| attempts | Array | 복구 시도 이력 |
| attempts[].attempt_num | Integer | 시도 번호 |
| attempts[].action | String | 수행 작업 |
| attempts[].result | String | 결과 |
| attempts[].timestamp | ISODate | 시도 시각 |
| created_at | ISODate | 생성 시각 |
| updated_at | ISODate | 수정 시각 |

**status 값**: `pending` → `diagnosing` → `source_check` → `ai_solving` → `resolved` / `failed` / `waiting_admin`

**인덱스**: `{session_id: 1}` UNIQUE, `{source_id: 1, created_at: -1}`, `{status: 1}`

#### wellknown_cases

기존 해결책 DB. 에러 패턴과 검증된 해결 코드를 저장.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| case_id | String | 케이스 식별자 (Unique) |
| error_pattern | String | 에러 매칭 정규식 |
| error_category | String | 에러 분류 (아래 참조) |
| solution_code | String | 해결 Python 코드 |
| solution_description | String | 해결책 설명 |
| success_count | Integer | 성공 횟수 |
| failure_count | Integer | 실패 횟수 |
| last_used | ISODate | 마지막 사용 시각 |
| created_at | ISODate | 생성 시각 |
| created_by | String | 생성 주체 (`ai`, `admin`) |

**error_category 값**: `source_not_updated`, `structure_changed`, `selector_broken`, `auth_required`, `rate_limited`, `network_error`, `parse_error`, `data_validation`, `unknown`

**인덱스**: `{error_pattern: 1}`, `{error_category: 1}`, `{success_count: -1}`

#### healing_schedules

재시도 스케줄 관리. 지수 백오프(3분, 10분, 30분, 2시간, 12시간, 1일)로 재시도.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| session_id | ObjectId | FK → healing_sessions._id |
| scheduled_at | ISODate | 예정 재시도 시각 |
| retry_attempt | Integer | 재시도 번호 |
| scheduled_by | String | 스케줄러 |

---

### 인증 컬렉션

#### auth_configs

소스별 인증 설정.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| source_id | ObjectId | FK → sources._id (Unique) |
| auth_type | String | 인증 유형 (`form`, `oauth`, `api_key`, `cookie`, `basic`, `bearer`, `custom`) |
| session_duration_hours | Integer | 세션 유지 시간 (1~720) |
| auto_refresh | Boolean | 자동 갱신 여부 |
| created_at | ISODate | 생성 시각 |

#### auth_credentials

암호화된 인증 정보. 소스별 1:1.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| source_id | ObjectId | FK → sources._id (Unique) |
| auth_type | String | 인증 유형 |
| username | String | 사용자명 (암호화) |
| password | String | 비밀번호 (암호화) |
| api_key | String | API 키 (암호화) |
| oauth_token | String | OAuth 토큰 (암호화) |
| created_at | ISODate | 생성 시각 |

#### auth_sessions

활성 인증 세션.

| 필드 | 타입 | 설명 |
|------|------|------|
| _id | ObjectId | Primary Key |
| source_id | ObjectId | FK → sources._id (Unique) |
| is_valid | Boolean | 유효 여부 |
| cookies | Object | 세션 쿠키 |
| headers | Object | 인증 헤더 |
| expires_at | ISODate | 만료 시각 |
| created_at | ISODate | 생성 시각 |

---

## PostgreSQL 테이블

MongoDB와 듀얼라이트 구조. 프로덕션 데이터가 승격될 때 PostgreSQL에도 동시 저장.

### 공통코드

#### tb_common_code_group

```
group_code    VARCHAR(30)   PK    코드 그룹 식별자
group_name    VARCHAR(100)        그룹 이름
description   TEXT                설명
is_active     BOOLEAN             활성 여부
sort_order    INTEGER             정렬 순서
created_at    TIMESTAMP           생성 시각
updated_at    TIMESTAMP           수정 시각
```

#### tb_common_code

```
id            SERIAL        PK    자동 증가 ID
group_code    VARCHAR(30)   FK    → tb_common_code_group
code          VARCHAR(50)         코드 값
code_name     VARCHAR(200)        코드명 (한국어)
code_name_en  VARCHAR(200)        코드명 (영어)
description   TEXT                설명
extra_value1  VARCHAR(200)        확장 값 1
extra_value2  VARCHAR(200)        확장 값 2
is_active     BOOLEAN             활성 여부
sort_order    INTEGER             정렬 순서
created_at    TIMESTAMP           생성 시각
updated_at    TIMESTAMP           수정 시각
```

**UNIQUE**: `(group_code, code)`

**주요 코드 그룹**:

| group_code | 설명 | 값 예시 |
|------------|------|---------|
| SOURCE_TYPE | 소스 유형 | HTML, PDF, EXCEL, CSV, API, JSON |
| FIELD_TYPE | 필드 데이터 타입 | STRING, NUMBER, DATE, BOOLEAN, CURRENCY |
| REVIEW_STATUS | 리뷰 상태 | PENDING, APPROVED, CORRECTED, REJECTED |
| DATA_CATEGORY | 데이터 분류 | NEWS, FINANCE, ANNOUNCEMENT, GENERIC |
| SCHEDULE_TYPE | 스케줄 유형 | HOURLY, DAILY, WEEKLY, MONTHLY, CUSTOM |
| ERROR_TYPE | 에러 유형 | CONNECTION, TIMEOUT, PARSING, VALIDATION |

---

### 소스 마스터

#### tb_source_master

MongoDB sources와 동기화되는 소스 마스터 테이블.

```
id            SERIAL        PK    자동 증가 ID
mongo_id      VARCHAR(24)   UQ    MongoDB ObjectId (동기화 키)
name          VARCHAR(200)        소스 이름
url           TEXT                크롤링 URL
source_type   VARCHAR(30)         소스 유형
data_category VARCHAR(30)         데이터 분류 (NEWS, FINANCE, ANNOUNCEMENT, GENERIC)
schedule      VARCHAR(50)         Cron 표현식
status        VARCHAR(20)         상태
description   TEXT                설명
is_active     BOOLEAN             활성 여부
created_at    TIMESTAMP           생성 시각
updated_at    TIMESTAMP           수정 시각
```

#### tb_source_field

소스별 필드 정의.

```
id            SERIAL        PK    자동 증가 ID
source_id     INTEGER       FK    → tb_source_master
field_name    VARCHAR(100)        필드 이름
field_type    VARCHAR(30)         필드 데이터 타입
pg_column_name VARCHAR(100)       PostgreSQL 컬럼명 매핑
selector      TEXT                CSS 셀렉터
is_list       BOOLEAN             목록 여부
is_required   BOOLEAN             필수 여부
sort_order    INTEGER             정렬 순서
created_at    TIMESTAMP           생성 시각
```

**UNIQUE**: `(source_id, field_name)`

---

### 도메인 데이터 테이블

모든 도메인 테이블은 아래 **공통 메타데이터 컬럼**을 포함:

```
id              BIGSERIAL     PK    자동 증가 ID
source_id       INTEGER       FK    → tb_source_master
mongo_review_id VARCHAR(24)         MongoDB 리뷰 ID
mongo_prod_id   VARCHAR(24)   UQ    MongoDB 프로덕션 ID
data_date       DATE                데이터 기준일
crawled_at      TIMESTAMP           크롤링 시각
verified_at     TIMESTAMP           검증 시각
verified_by     VARCHAR(100)        검증자
has_corrections BOOLEAN             수정 이력 존재 여부
promoted_at     TIMESTAMP           승격 시각
created_at      TIMESTAMP           생성 시각
```

#### tb_data_news

```
title          TEXT          기사 제목
content        TEXT          본문
summary        TEXT          요약
author         VARCHAR(200)  작성자
press          VARCHAR(200)  언론사
published_at   TIMESTAMP     발행일
category       VARCHAR(100)  카테고리
url            TEXT          원문 URL
image_url      TEXT          이미지 URL
tags           JSONB         태그 배열
```

#### tb_data_finance

```
ticker         VARCHAR(20)       종목 코드
name           VARCHAR(200)      종목명
price          NUMERIC(18,4)     현재가
open_price     NUMERIC(18,4)     시가
high_price     NUMERIC(18,4)     고가
low_price      NUMERIC(18,4)     저가
close_price    NUMERIC(18,4)     종가
volume         BIGINT            거래량
change_amount  NUMERIC(18,4)     전일 대비
change_rate    NUMERIC(10,4)     등락률
market_cap     NUMERIC(24,0)     시가총액
market         VARCHAR(50)       시장
currency       VARCHAR(10)       통화
trading_date   DATE              거래일
extra_data     JSONB             확장 데이터
```

#### tb_data_announcement

```
company        VARCHAR(200)      회사명
company_code   VARCHAR(20)       회사 코드
title          TEXT               공시 제목
content        TEXT               공시 내용
filing_date    DATE               공시일
category       VARCHAR(100)       분류
sub_category   VARCHAR(100)       세부 분류
market         VARCHAR(50)        시장
url            TEXT               원문 URL
attachments    JSONB              첨부파일 목록
```

#### tb_data_generic

```
category_code  VARCHAR(30)       분류 코드
data_json      JSONB             범용 데이터 (GIN 인덱스)
title          TEXT               제목
url            TEXT               원본 URL
```

---

### 승격 이력

#### tb_promotion_log

데이터 승격/롤백 감사 로그.

```
id              BIGSERIAL     PK    자동 증가 ID
source_id       INTEGER       FK    → tb_source_master
mongo_review_id VARCHAR(24)         리뷰 ID
mongo_staging_id VARCHAR(24)        스테이징 ID
target_table    VARCHAR(63)         대상 테이블명
target_id       BIGINT              대상 레코드 ID
action          VARCHAR(20)         작업 (promote, rollback)
reviewer_id     VARCHAR(100)        리뷰어
has_corrections BOOLEAN             수정 여부
corrections     JSONB               수정 내역
promoted_at     TIMESTAMP           승격 시각
rolled_back_at  TIMESTAMP           롤백 시각 (선택)
rollback_reason TEXT                롤백 사유 (선택)
```

---

## Redis 캐시 패턴

| 키 패턴 | TTL | 설명 |
|---------|-----|------|
| `cache:dashboard:stats` | 60초 | 대시보드 통계 (소스 수, 성공률 등) |
| `cache:sources:list` | 30초 | 소스 목록 |
| `cache:source:{id}` | 60초 | 개별 소스 상세 |
| `cache:monitoring:health` | 15초 | 시스템 헬스 상태 |
| `cache:errors:recent` | 30초 | 최근 에러 목록 |
| `ratelimit:{ip}` | 설정별 | Rate Limit 카운터 (슬라이딩 윈도우) |

---

## 데이터 흐름

### ETL 파이프라인

```
사용자: 소스 정의 (URL, 필드, 스케줄)
         │
         ▼
    ┌──────────┐
    │ sources  │  소스 설정 저장
    └────┬─────┘
         │ GPT 코드 생성
         ▼
    ┌──────────┐
    │ crawlers │  크롤러 코드 + DAG 등록
    └────┬─────┘
         │ Airflow 스케줄 실행
         ▼
    ┌──────────────┐
    │ crawl_results│  추출 데이터 + 실행 상태
    └────┬─────────┘
         │ 도메인 분류
         ▼
    ┌──────────────────────────────────┐
    │ staging_news / staging_financial │  중간 저장
    │ staging_data                     │
    └────┬─────────────────────────────┘
         │ 리뷰 대기
         ▼
    ┌──────────────┐
    │ data_reviews │  승인 / 수정 / 거절
    └────┬─────────┘
         │ 승인 시
         ▼
    ┌──────────────────────────────────────┐
    │ MongoDB production 컬렉션            │
    │ (news_articles, financial_data 등)   │
    │          +                           │
    │ PostgreSQL 듀얼라이트                │
    │ (tb_data_news, tb_data_finance 등)   │
    └────┬─────────────────────────────────┘
         │
         ▼
    ┌──────────────┐
    │ data_lineage │  이동 이력 기록
    └──────────────┘
```

### 자가치유 흐름

```
crawl_results (실패)
         │
         ▼
    ┌────────────┐
    │ error_logs │  에러 기록
    └────┬───────┘
         │
         ▼
    ┌──────────────────┐
    │ healing_sessions │  복구 세션 생성
    └────┬─────────────┘
         │ 기존 사례 검색
         ▼
    ┌─────────────────┐
    │ wellknown_cases │  매칭되는 해결책 조회
    └────┬────────────┘
         │
         ├── 해결책 있음 → 패치 적용 → 재실행 → 성공 시 success_count++
         │
         └── 없음 → GPT 분석 → 코드 수정 → 재실행
                                                │
                                                └── 성공 → wellknown_cases에 새 사례 등록
```

---

## 관계 및 제약조건

### 주요 참조 관계

```
sources._id
    ├── crawlers.source_id
    ├── crawl_results.source_id
    ├── error_logs.source_id
    ├── data_reviews.source_id
    ├── healing_sessions.source_id
    ├── auth_configs.source_id      (1:1)
    ├── auth_credentials.source_id  (1:1)
    └── auth_sessions.source_id     (1:1)

crawlers._id
    ├── crawler_history.crawler_id
    ├── crawl_results.crawler_id
    ├── error_logs.crawler_id
    └── healing_sessions.crawler_id

crawl_results._id
    └── data_reviews.crawl_result_id

healing_sessions._id
    └── healing_schedules.session_id

wellknown_cases._id
    └── healing_sessions.matched_case

data_catalog._id
    └── data_contracts.dataset_id
```

### Unique 제약조건

| 컬렉션/테이블 | 필드 | 비고 |
|---------------|------|------|
| sources | name | 소스명 전역 고유 |
| crawlers | dag_id | Sparse (null 허용) |
| crawler_history | (crawler_id, version) | 복합 유니크 |
| auth_configs | source_id | 소스당 1개 |
| auth_credentials | source_id | 소스당 1개 |
| auth_sessions | source_id | 소스당 1개 |
| healing_sessions | session_id | 세션 ID 고유 |
| stock_prices | (stock_code, _data_date) | Sparse |
| exchange_rates | (currency_code, _data_date) | Sparse |
| market_indices | (index_code, _data_date) | Sparse |
| announcements | content_hash | Sparse (중복 방지) |
| tb_common_code | (group_code, code) | 복합 유니크 |
| tb_source_field | (source_id, field_name) | 복합 유니크 |

---

## 보존 정책

| 데이터 | 보존 기간 | 방식 |
|--------|-----------|------|
| sources, crawlers | 영구 | 아카이브 |
| crawler_history | 영구 | 변경 이력 |
| crawl_results | 90일 | TTL 인덱스 자동 삭제 |
| error_logs | 90일+ | 해결 후 조회 빈도 감소 |
| data_reviews | 365일+ | 감사 목적 장기 보존 |
| staging_* | 30일 | 승격 후 정리 |
| production (MongoDB) | 영구 또는 TTL | 컬렉션별 상이 |
| production (PostgreSQL) | 영구 | SQL 분석용 |
| data_lineage | 영구 | 감사 추적 |
| schema_registry | 영구 | 스키마 이력 |
| healing_sessions | 90일 | 패턴 분석 후 정리 |
| wellknown_cases | 영구 | 학습된 해결책 |
| announcements | 365일 | TTL 자동 삭제 |
| exchange_rates | 365일 | TTL 자동 삭제 |
| market_indices | 365일 | TTL 자동 삭제 |
