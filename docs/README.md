Airflow Crawler System 문서
==========================

프로젝트 문서 통합 인덱스.

---

## 문서 목록

### 시스템 이해

| 문서 | 설명 | 대상 |
|------|------|------|
| [architecture.md](./architecture.md) | 시스템 아키텍처, 서비스 구성, 데이터 흐름, 기술 스택 | 전체 |
| [data-model.md](./data-model.md) | MongoDB 컬렉션, PostgreSQL 테이블, Redis 캐시 스키마 및 관계 | 개발자, DBA |
| [api-reference.md](./api-reference.md) | 20개 라우터, 150+ 엔드포인트 상세 레퍼런스 | 개발자 |

### 개발/운영 가이드

| 문서 | 설명 | 대상 |
|------|------|------|
| [onboarding.md](./onboarding.md) | 환경 세팅, 디렉토리 구조, 코드 규칙, 자주 쓰는 API | 신규 개발자 |
| [operations-runbook.md](./operations-runbook.md) | 장애 대응, 백업/복구, 스케일링, 정기 점검 | 운영팀 |
| [EXCEPTION_VALIDATION_MANUAL.md](./EXCEPTION_VALIDATION_MANUAL.md) | 예외처리 체계, 입력 검증, 보안 검증 가이드 | 개발자 |

---

## 빠른 링크

- **처음 시작**: [onboarding.md](./onboarding.md) → 5분 세팅 가이드
- **API 확인**: http://localhost:8000/docs (Swagger UI)
- **대시보드**: http://localhost:3001 (Grafana)
- **소스 코드 구조**: [architecture.md](./architecture.md) → 디렉토리/서비스 구조
- **데이터 구조**: [data-model.md](./data-model.md) → 컬렉션/테이블 스키마
- **장애 대응**: [operations-runbook.md](./operations-runbook.md) → 장애별 체크리스트

---

## 문서 간 관계

```
architecture.md (전체 구조 파악)
    ├── data-model.md (데이터 계층 상세)
    ├── api-reference.md (API 계층 상세)
    └── onboarding.md (개발 환경 구축)
            └── operations-runbook.md (운영 절차)

EXCEPTION_VALIDATION_MANUAL.md (횡단 관심사: 예외처리/검증)
```
