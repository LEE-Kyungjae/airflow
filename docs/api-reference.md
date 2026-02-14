API 레퍼런스
============

FastAPI 기반 REST API 엔드포인트 전체 레퍼런스.
OpenAPI 문서: http://localhost:8000/docs (Swagger UI), http://localhost:8000/redoc (ReDoc)

---

## 목차

1. [인증](#인증)
2. [엔드포인트 목록](#엔드포인트-목록)
   - [헬스체크](#헬스체크)
   - [인증 (auth)](#인증-auth)
   - [소스 관리 (sources)](#소스-관리-sources)
   - [크롤러 (crawlers)](#크롤러-crawlers)
   - [에러 (errors)](#에러-errors)
   - [대시보드 (dashboard)](#대시보드-dashboard)
   - [퀵 추가 (quick-add)](#퀵-추가-quick-add)
   - [모니터링 (monitoring)](#모니터링-monitoring)
   - [리뷰 (reviews)](#리뷰-reviews)
   - [데이터 품질 (data-quality)](#데이터-품질-data-quality)
   - [리니지 (lineage)](#리니지-lineage)
   - [내보내기 (export)](#내보내기-export)
   - [백업 (backup)](#백업-backup)
   - [계약 (contracts)](#계약-contracts)
   - [스키마 (schemas)](#스키마-schemas)
   - [카탈로그 (catalog)](#카탈로그-catalog)
   - [버전 (versions)](#버전-versions)
   - [소스 인증 설정 (auth-config)](#소스-인증-설정-auth-config)
   - [E2E 파이프라인 (e2e)](#e2e-파이프라인-e2e)
   - [프로덕션 데이터 (production)](#프로덕션-데이터-production)
   - [메트릭 (metrics)](#메트릭-metrics)
3. [공통 사항](#공통-사항)

---

## 인증

### 인증 방식

| 방식 | 헤더 | 용도 |
|------|------|------|
| JWT Bearer Token | `Authorization: Bearer <token>` | 사용자 로그인 (POST /api/auth/login) |
| API Key | `X-API-Key: <key>` | 외부 시스템 연동 |

### 권한 스코프

| 스코프 | 설명 |
|--------|------|
| `read` | 조회 전용 |
| `write` | 생성/수정 |
| `delete` | 삭제 |
| `admin` | 전체 권한 (백업, API 키 관리 등) |

### Rate Limiting

| 조건 | 제한 |
|------|------|
| 인증된 요청 | 100 req/min |
| 미인증 요청 | 20 req/min |

---

## 엔드포인트 목록

### 헬스체크

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/health` | - | 로드밸런서 헬스체크 |
| GET | `/` | - | API 정보 및 문서 링크 |

---

### 인증 (auth)

**Prefix**: `/api/auth`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| POST | `/api/auth/login` | - | 로그인. JWT 토큰 발급 |
| POST | `/api/auth/refresh` | - | 토큰 갱신 |
| GET | `/api/auth/me` | auth | 현재 사용자 정보 조회 |
| GET | `/api/auth/verify` | auth | 인증 상태 확인 |
| POST | `/api/auth/api-keys` | admin | API Key 생성 |
| GET | `/api/auth/api-keys` | admin | API Key 목록 |
| DELETE | `/api/auth/api-keys/{key_id}` | admin | API Key 폐기 |

**요청 예시 - 로그인**:
```json
POST /api/auth/login
{
  "username": "admin",
  "password": "your-password"
}
```

**응답**:
```json
{
  "access_token": "eyJ...",
  "refresh_token": "eyJ...",
  "token_type": "bearer",
  "expires_in": 3600
}
```

---

### 소스 관리 (sources)

**Prefix**: `/api/sources`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/sources` | auth | 소스 목록 조회 |
| POST | `/api/sources` | write | 소스 생성 (+ DAG 자동 등록) |
| GET | `/api/sources/{source_id}` | auth | 소스 상세 조회 |
| PUT | `/api/sources/{source_id}` | write | 소스 수정 |
| DELETE | `/api/sources/{source_id}` | delete | 소스 삭제 (관련 데이터 포함) |
| POST | `/api/sources/{source_id}/trigger` | write | 수동 크롤링 실행 |
| GET | `/api/sources/{source_id}/results` | auth | 크롤링 결과 이력 |

**쿼리 파라미터** (GET 목록):

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| status | string | - | 필터: active, inactive, error |
| skip | int | 0 | 페이지네이션 오프셋 |
| limit | int | 100 | 최대 결과 수 (1~500) |
| include_crawler_info | bool | false | 크롤러 정보 포함 여부 |

**요청 예시 - 소스 생성**:
```json
POST /api/sources
{
  "name": "네이버 뉴스",
  "url": "https://news.naver.com/main/list.naver",
  "type": "html",
  "fields": [
    {"name": "title", "selector": ".news_tit", "data_type": "string"},
    {"name": "date", "selector": ".info_group span", "data_type": "date"}
  ],
  "schedule": "0 */6 * * *"
}
```

---

### 크롤러 (crawlers)

**Prefix**: `/api/crawlers`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/crawlers` | auth | 크롤러 목록 |
| GET | `/api/crawlers/{crawler_id}` | auth | 크롤러 상세 (코드 포함) |
| GET | `/api/crawlers/{crawler_id}/code` | auth | 크롤러 코드만 조회 |
| GET | `/api/crawlers/{crawler_id}/history` | auth | 버전 이력 |
| GET | `/api/crawlers/{crawler_id}/history/{version}` | auth | 특정 버전 코드 |
| POST | `/api/crawlers/{crawler_id}/rollback/{version}` | write | 이전 버전으로 롤백 |

**쿼리 파라미터**:

| 파라미터 | 타입 | 설명 |
|----------|------|------|
| source_id | string | 소스별 필터 |
| status | string | 상태 필터: active, testing, deprecated |
| include_source_info | bool | 소스 정보 포함 ($lookup) |

---

### 에러 (errors)

**Prefix**: `/api/errors`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/errors` | auth | 에러 로그 목록 |
| GET | `/api/errors/unresolved` | auth | 미해결 에러만 |
| GET | `/api/errors/stats` | auth | 에러 통계 |
| GET | `/api/errors/{error_id}` | auth | 에러 상세 |
| POST | `/api/errors/{error_id}/resolve` | write | 에러 해결 처리 |
| POST | `/api/errors/{error_id}/retry` | write | 크롤링 재시도 |
| POST | `/api/errors/{error_id}/regenerate` | write | GPT로 크롤러 재생성 |

**쿼리 파라미터**:

| 파라미터 | 타입 | 설명 |
|----------|------|------|
| resolved | bool | 해결 여부 필터 |
| source_id | string | 소스별 필터 |
| error_code | string | 에러 코드 필터 (E001~E010) |

---

### 대시보드 (dashboard)

**Prefix**: `/api/dashboard`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/dashboard` | auth | 대시보드 통계 개요 |
| GET | `/api/dashboard/recent-activity` | auth | 최근 활동 내역 |
| GET | `/api/dashboard/sources-status` | auth | 소스 상태 현황 |
| GET | `/api/dashboard/execution-trends` | auth | 실행 트렌드 (기간별) |
| GET | `/api/dashboard/system-health` | auth | 시스템 건강 점수 (0~100) |

**쿼리 파라미터**:

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| optimized | bool | true | 최적화된 집계 사용 |
| hours | int | 24 | 최근 활동 기간 (시간) |
| days | int | 7 | 트렌드 기간 (일, 최대 90) |

---

### 퀵 추가 (quick-add)

**Prefix**: `/api/quick-add`

URL만으로 소스를 자동 분석/등록하는 간편 인터페이스.

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| POST | `/api/quick-add/analyze` | auth | URL 분석 (등록하지 않음) |
| POST | `/api/quick-add/test` | write | 테스트 크롤링 실행 |
| POST | `/api/quick-add` | write | 원클릭 소스 등록 |
| POST | `/api/quick-add/create` | write | 커스텀 필드로 생성 |
| POST | `/api/quick-add/batch` | write | 다중 URL 일괄 등록 (최대 50개) |
| GET | `/api/quick-add/templates` | auth | 소스 템플릿 목록 |
| POST | `/api/quick-add/from-template/{template_id}` | write | 템플릿으로 생성 |

**요청 예시 - 원클릭 등록**:
```json
POST /api/quick-add
{
  "url": "https://example.com/data",
  "name": "예제 데이터",
  "hint": "뉴스 기사 목록 페이지",
  "auto_start": true
}
```

**응답**:
```json
{
  "source_id": "...",
  "crawler_id": "...",
  "dag_id": "crawler_...",
  "discovery": {
    "page_type": "news_list",
    "fields": [...],
    "confidence_score": 0.85,
    "sample_data": [...]
  },
  "message": "소스 등록 완료"
}
```

---

### 모니터링 (monitoring)

**Prefix**: `/api/monitoring`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/monitoring/pipeline-status` | auth | 파이프라인 실시간 상태 |
| GET | `/api/monitoring/error-summary` | auth | 에러 분류 요약 |
| GET | `/api/monitoring/healing-sessions` | auth | 자가치유 세션 목록 |
| GET | `/api/monitoring/system-health` | auth | 시스템 건강 상태 |
| WS | `/api/monitoring/ws` | auth | WebSocket 실시간 업데이트 |

---

### 리뷰 (reviews)

**Prefix**: `/api/reviews`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/reviews/dashboard` | auth | 리뷰 통계 대시보드 |
| GET | `/api/reviews/queue` | auth | 리뷰 대기 큐 |
| GET | `/api/reviews/{review_id}` | auth | 리뷰 상세 |
| PATCH | `/api/reviews/{review_id}` | write | 리뷰 상태 변경 |
| POST | `/api/reviews/bulk-approve` | write | 다건 일괄 승인 |
| POST | `/api/reviews/bulk-reject` | write | 다건 일괄 거절 |
| POST | `/api/reviews/bulk-filter` | write | 필터링 및 내보내기 |

**요청 예시 - 리뷰 상태 변경**:
```json
PATCH /api/reviews/{review_id}
{
  "review_status": "approved",
  "notes": "데이터 확인 완료"
}
```

**요청 예시 - 일괄 승인**:
```json
POST /api/reviews/bulk-approve
{
  "review_ids": ["id1", "id2", "id3"],
  "notes": "배치 승인"
}
```

---

### 데이터 품질 (data-quality)

**Prefix**: `/api/data-quality`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/data-quality/validation-results` | auth | 검증 결과 목록 |
| GET | `/api/data-quality/validation-results/{id}` | auth | 검증 결과 상세 |
| GET | `/api/data-quality/quality-trend/{source_id}` | auth | 품질 추이 |
| GET | `/api/data-quality/anomalies` | auth | 이상 탐지 목록 |
| POST | `/api/data-quality/anomalies/{id}/acknowledge` | write | 이상 확인 처리 |
| GET | `/api/data-quality/report/{source_id}` | auth | 품질 리포트 |
| POST | `/api/data-quality/validate` | write | 수동 데이터 검증 |

**쿼리 파라미터**:

| 파라미터 | 타입 | 설명 |
|----------|------|------|
| source_id | string | 소스 필터 |
| min_score / max_score | float | 품질 점수 범위 (0~100) |
| has_issues | bool | 이슈 유무 필터 |
| days | int | 기간 (1~90, 기본 7) |

---

### 리니지 (lineage)

**Prefix**: `/api/lineage`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/lineage/nodes` | auth | 리니지 노드 목록 |
| POST | `/api/lineage/nodes` | write | 리니지 노드 생성 |
| GET | `/api/lineage/nodes/{node_id}` | auth | 노드 상세 |
| POST | `/api/lineage/edges` | write | 리니지 엣지 생성 |
| GET | `/api/lineage/graph/{node_id}` | auth | 리니지 그래프 조회 |
| POST | `/api/lineage/impact-analysis` | auth | 변경 영향 분석 |

**요청 예시 - 영향 분석**:
```json
POST /api/lineage/impact-analysis
{
  "node_id": "...",
  "change_type": "modify",
  "changed_fields": ["title", "date"]
}
```

---

### 내보내기 (export)

**Prefix**: `/api/export`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/export/csv` | auth | CSV 내보내기 (스트리밍) |
| GET | `/api/export/excel` | auth | Excel 내보내기 (스트리밍) |
| GET | `/api/export/json` | auth | JSON 내보내기 (스트리밍) |
| POST | `/api/export/async` | write | 비동기 대량 내보내기 |
| GET | `/api/export/jobs` | auth | 내보내기 작업 목록 |
| GET | `/api/export/jobs/{job_id}` | auth | 작업 상태 조회 |
| POST | `/api/export/jobs/{job_id}/cancel` | write | 작업 취소 |

**쿼리 파라미터** (동기 내보내기):

| 파라미터 | 타입 | 설명 |
|----------|------|------|
| collection | string | 대상 컬렉션 (crawl_results, data_reviews, sources, error_logs) |
| source_id | string | 소스 필터 |
| date_from / date_to | datetime | 날짜 범위 |
| encoding | string | 인코딩 (utf-8-sig, utf-8, euc-kr) |

---

### 백업 (backup)

**Prefix**: `/api/backup`
**권한**: 모든 엔드포인트 admin 전용

| Method | Path | 설명 |
|--------|------|------|
| POST | `/api/backup/trigger` | 즉시 백업 실행 |
| GET | `/api/backup/list` | 백업 목록 |
| GET | `/api/backup/status` | 백업 시스템 상태 |
| GET | `/api/backup/{backup_id}/download` | 백업 다운로드 |
| POST | `/api/backup/restore` | 복구 실행 |
| DELETE | `/api/backup/{backup_id}` | 백업 삭제 |
| GET | `/api/backup/config` | 백업 설정 조회 |
| PUT | `/api/backup/config` | 백업 설정 변경 |

---

### 계약 (contracts)

**Prefix**: `/api/contracts`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/contracts` | auth | 데이터 계약 목록 |
| POST | `/api/contracts` | write | 계약 생성 (템플릿 기반) |
| GET | `/api/contracts/templates` | auth | 계약 템플릿 목록 |
| GET | `/api/contracts/{contract_id}` | auth | 계약 상세 |
| DELETE | `/api/contracts/{contract_id}` | delete | 계약 삭제 |
| POST | `/api/contracts/validate` | write | 계약 기준 데이터 검증 |

---

### 스키마 (schemas)

**Prefix**: `/api/schemas`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/schemas/{source_id}` | auth | 소스 스키마 조회 |
| POST | `/api/schemas/{source_id}` | write | 스키마 버전 등록 |
| GET | `/api/schemas/{source_id}/versions` | auth | 스키마 버전 이력 |
| POST | `/api/schemas/detect` | auth | 데이터에서 스키마 자동 감지 |
| POST | `/api/schemas/validate` | auth | 스키마 기준 데이터 검증 |
| POST | `/api/schemas/validate-batch` | auth | 배치 검증 |

---

### 카탈로그 (catalog)

**Prefix**: `/api/catalog`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/catalog` | auth | 데이터셋 목록 |
| POST | `/api/catalog` | write | 데이터셋 등록 |
| GET | `/api/catalog/{dataset_id}` | auth | 데이터셋 메타데이터 |
| PUT | `/api/catalog/{dataset_id}` | write | 메타데이터 수정 |
| DELETE | `/api/catalog/{dataset_id}` | delete | 데이터셋 삭제 |
| POST | `/api/catalog/search` | auth | 데이터셋 검색 |
| GET | `/api/catalog/{dataset_id}/lineage` | auth | 데이터셋 리니지 |

---

### 버전 (versions)

**Prefix**: `/api/versions`

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/versions/{source_id}` | auth | 데이터 버전 목록 |
| POST | `/api/versions/{source_id}` | write | 버전 스냅샷 생성 |
| GET | `/api/versions/{source_id}/{version_id}` | auth | 버전 상세 |
| GET | `/api/versions/{source_id}/diff` | auth | 두 버전 비교 (diff) |
| POST | `/api/versions/{source_id}/rollback` | write | 버전 롤백 |
| GET | `/api/versions/{source_id}/snapshots` | auth | 스냅샷 목록 |
| POST | `/api/versions/{source_id}/snapshots` | write | 스냅샷 생성 |

---

### 소스 인증 설정 (auth-config)

**Prefix**: `/api/sources/{source_id}/auth`

인증이 필요한 크롤링 소스의 로그인 설정 관리.

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| POST | `/api/sources/{source_id}/auth` | write | 인증 설정 생성 |
| GET | `/api/sources/{source_id}/auth` | auth | 인증 설정 조회 |
| PUT | `/api/sources/{source_id}/auth` | write | 인증 설정 수정 |
| DELETE | `/api/sources/{source_id}/auth` | delete | 인증 설정 삭제 |
| POST | `/api/sources/{source_id}/auth/test` | write | 로그인 테스트 |
| GET | `/api/sources/{source_id}/auth/session` | auth | 세션 상태 조회 |
| POST | `/api/sources/{source_id}/auth/session/refresh` | write | 세션 갱신 |

**auth_type 값**: `none`, `basic`, `form`, `oauth`, `api_key`, `cookie`, `bearer`, `custom`

---

### E2E 파이프라인 (e2e)

**Prefix**: `/api/e2e`

URL 입력부터 크롤러 생성/실행까지 전체 자동화 파이프라인.

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| POST | `/api/e2e/pipeline` | auth | 동기 파이프라인 실행 |
| POST | `/api/e2e/pipeline/async` | auth | 비동기 파이프라인 실행 |
| GET | `/api/e2e/pipeline/{pipeline_id}` | auth | 파이프라인 상태 조회 |
| GET | `/api/e2e/pipelines` | auth | 최근 파이프라인 목록 |

**요청 예시**:
```json
POST /api/e2e/pipeline
{
  "url": "https://example.com/data",
  "name": "예제 데이터",
  "hint": "표 형태의 금융 데이터",
  "auto_start": true
}
```

**응답 (파이프라인 상태)**:
```json
{
  "pipeline_id": "...",
  "url": "https://example.com/data",
  "current_stage": "crawl_execute",
  "stages_completed": ["url_analyze", "source_create", "crawler_generate"],
  "stage_results": {...},
  "source_id": "...",
  "dag_id": "crawler_..."
}
```

---

### 프로덕션 데이터 (production)

**Prefix**: `/api/production`

PostgreSQL 듀얼라이트 데이터 조회. 공통코드 및 도메인 데이터 접근.

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/api/production/status` | - | PostgreSQL 연결 상태 |
| GET | `/api/production/common-codes` | - | 공통코드 그룹 목록 |
| GET | `/api/production/common-codes/{group_code}` | - | 그룹별 코드 목록 |
| GET | `/api/production/domains` | - | 도메인 테이블 목록 |
| GET | `/api/production/domains/{category}` | - | 도메인 데이터 조회 |
| GET | `/api/production/promotions` | - | 승격 이력 |

**category 값**: `NEWS`, `FINANCE`, `ANNOUNCEMENT`, `GENERIC`

**쿼리 파라미터** (도메인 조회):

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| source_id | int | - | 소스 필터 |
| search | string | - | 전문 검색 |
| order_by | string | promoted_at DESC | 정렬 기준 |
| page | int | 1 | 페이지 번호 |
| page_size | int | 50 | 페이지 크기 (1~200) |

---

### 메트릭 (metrics)

**Prefix**: `/metrics` (주의: /api 접두사 없음)

| Method | Path | 인증 | 설명 |
|--------|------|------|------|
| GET | `/metrics` | - | Prometheus 메트릭 (text/plain) |
| GET | `/metrics/health` | - | 메트릭 시스템 헬스체크 |

Prometheus가 이 엔드포인트를 스크래핑하여 Grafana 대시보드에 표시.

---

## 공통 사항

### 에러 응답 형식

모든 에러는 아래 형식으로 반환:

```json
{
  "detail": "에러 메시지",
  "error_code": "E001",
  "timestamp": "2024-01-01T00:00:00Z"
}
```

### HTTP 상태 코드

| 코드 | 의미 | 발생 상황 |
|------|------|-----------|
| 200 | 성공 | 정상 응답 |
| 201 | 생성 | 리소스 생성 완료 |
| 400 | 잘못된 요청 | 파라미터 오류, 유효성 검증 실패 |
| 401 | 미인증 | 토큰 없음 또는 만료 |
| 403 | 권한 부족 | 스코프 부족 |
| 404 | 없음 | 리소스를 찾을 수 없음 |
| 429 | 요청 과다 | Rate Limit 초과 |
| 500 | 서버 에러 | 내부 오류 |

### 페이지네이션

목록 조회 API는 공통적으로 `skip`/`limit` 또는 `page`/`page_size` 패턴을 사용:

```
# skip/limit 패턴
GET /api/sources?skip=0&limit=100

# page/page_size 패턴
GET /api/production/domains/NEWS?page=1&page_size=50
```

### 미들웨어

| 미들웨어 | 설명 |
|----------|------|
| CORS | 개발: 전체 허용, 프로덕션: 설정된 도메인만 |
| Rate Limiting (SlowAPI) | IP 기반 요청 제한 |
| Correlation ID | 요청 추적용 고유 ID 자동 부여 |
| Trusted Host | 프로덕션 환경 Host 헤더 검증 |
| Exception Handler | 전역 에러 핸들링 |
