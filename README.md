# Airflow Crawler System

AI 기반 웹 크롤링 자동화 플랫폼. URL만 입력하면 GPT가 크롤러 코드를 자동 생성하고, Airflow가 스케줄링하며, 실패 시 자가치유까지 수행합니다.

## 핵심 기능

| 기능 | 설명 |
|------|------|
| **AI 크롤러 생성** | URL + 필드 정의 → GPT가 크롤러 코드 자동 생성 |
| **Quick Add** | URL만 입력하면 AI가 페이지 분석 후 필드/선택자 자동 설정 |
| **Self-Healing** | 크롤링 실패 시 자동 진단 → 코드 수정 → 재실행 |
| **Playwright 크롤링** | SPA/동적 페이지도 브라우저 기반으로 크롤링 |
| **데이터 리뷰** | 키보드 단축키 기반 빠른 데이터 검수 워크플로우 |
| **스케줄 관리** | Cron 기반 자동 실행 + Airflow 오케스트레이션 |
| **듀얼라이트** | MongoDB + PostgreSQL 동시 저장 |
| **실시간 모니터링** | Prometheus + Grafana 대시보드 |
| **E2E 테스트** | Playwright 기반 자동화 테스트 |
| **CI/CD** | GitHub Actions → 스테이징 → 프로덕션 자동 배포 |

## 기술 스택

| 레이어 | 기술 |
|--------|------|
| **Backend** | FastAPI, Python 3.12 |
| **Frontend** | React 18, TypeScript, TailwindCSS, Vite |
| **Database** | MongoDB 7.0, PostgreSQL (듀얼라이트) |
| **Orchestration** | Apache Airflow 2.10 |
| **AI** | OpenAI GPT-4o-mini (커스텀 모델 지원) |
| **Crawling** | Playwright, BeautifulSoup4, Selenium |
| **Monitoring** | Prometheus, Grafana, Alertmanager |
| **CI/CD** | GitHub Actions, Docker Compose |
| **Testing** | pytest (466), Vitest (19), Playwright E2E |

## 빠른 시작

### 1. 환경 설정

```bash
git clone https://github.com/LEE-Kyungjae/airflow.git
cd airflow-crawler-system

cp .env.example .env
```

`.env` 필수값 설정:

```env
# AI (크롤러 자동생성에 필요)
OPENAI_API_KEY=sk-your-key

# DB 비밀번호
MONGO_ROOT_PASSWORD=your-strong-password
POSTGRES_PASSWORD=your-strong-password

# 보안 키 (각각 고유한 랜덤값)
AIRFLOW_FERNET_KEY=   # python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
JWT_SECRET_KEY=       # python -c "import secrets; print(secrets.token_urlsafe(64))"
ADMIN_PASSWORD=your-admin-password
```

### 2. 실행

```bash
# 개발 모드
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# 또는 Windows
scripts\start_dev.bat
```

### 3. 접속

| 서비스 | URL |
|--------|-----|
| **Frontend** | http://localhost:5173 |
| **API Docs** | http://localhost:8000/docs |
| **Airflow** | http://localhost:8080 |
| **Grafana** | http://localhost:3000 |

## 사용 예시

### Quick Add (가장 간단한 방법)

```bash
curl -X POST http://localhost:8000/api/quick-add \
  -H "Content-Type: application/json" \
  -d '{"url": "https://github.com/trending", "name": "github-trending"}'
```

AI가 페이지를 분석하고 크롤러를 자동 생성합니다.

### 소스 직접 등록

```bash
curl -X POST http://localhost:8000/api/sources \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "tech-news",
    "url": "https://news.example.com",
    "type": "html",
    "schedule": "0 */6 * * *",
    "fields": [
      {"name": "title", "selector": "h2.headline", "data_type": "string"},
      {"name": "date", "selector": "time", "data_type": "date", "attribute": "datetime"}
    ]
  }'
```

### Frontend에서

1. **Dashboard** → 전체 현황 한눈에 확인
2. **Quick Add** → URL 입력 → AI 분석 → 필드 확인 → 등록
3. **Sources** → 소스 관리, 수동 실행, 스케줄 변경
4. **Review** → 크롤링 데이터 검수 (키보드: `a` 승인, `r` 반려, `→` 다음)
5. **Monitoring** → 파이프라인 상태, 자가치유 세션 확인

## 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                        Frontend                             │
│              React + TypeScript + TailwindCSS                │
└──────────────────────────┬──────────────────────────────────┘
                           │ REST API
┌──────────────────────────▼──────────────────────────────────┐
│                     FastAPI Backend                          │
│  Sources │ Crawlers │ Reviews │ Dashboard │ Monitoring       │
│  Quick Add │ Export │ Data Quality │ Schema Registry         │
└─────┬────────────┬──────────────────────┬───────────────────┘
      │            │                      │
┌─────▼─────┐ ┌───▼──────────┐ ┌─────────▼─────────┐
│  MongoDB  │ │  PostgreSQL  │ │  Apache Airflow    │
│  (primary)│ │  (dual-write)│ │  DAG Orchestration │
└───────────┘ └──────────────┘ │  ├─ Source Manager  │
                               │  ├─ Dynamic Crawler │
                               │  ├─ Self-Healing    │
                               │  └─ Backup          │
                               └─────────┬───────────┘
                                         │
                               ┌─────────▼───────────┐
                               │   Crawler Engine     │
                               │  Playwright + BS4    │
                               │  + OpenAI GPT        │
                               └──────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                      Monitoring                             │
│          Prometheus → Grafana → Alertmanager                │
└─────────────────────────────────────────────────────────────┘
```

## 프로젝트 구조

```
airflow-crawler-system/
├── api/                          # FastAPI Backend
│   ├── app/
│   │   ├── auth/                 # JWT 인증
│   │   ├── models/               # Pydantic 스키마
│   │   ├── routers/              # API 엔드포인트
│   │   │   ├── sources.py        # 소스 CRUD
│   │   │   ├── crawlers.py       # 크롤러 관리
│   │   │   ├── reviews.py        # 데이터 리뷰
│   │   │   ├── dashboard.py      # 대시보드 통계
│   │   │   ├── quick_add.py      # AI Quick Add
│   │   │   ├── export.py         # 데이터 내보내기
│   │   │   └── monitoring.py     # 모니터링 API
│   │   └── services/             # 비즈니스 로직
│   │       ├── data_quality/     # 데이터 품질 검증
│   │       ├── schema_registry/  # 스키마 레지스트리
│   │       └── streaming/        # 스트리밍 내보내기
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/                     # React Frontend
│   ├── src/
│   │   ├── components/           # UI 컴포넌트
│   │   ├── pages/                # 페이지
│   │   │   ├── Dashboard.tsx     # 대시보드
│   │   │   ├── Sources.tsx       # 소스 관리
│   │   │   ├── QuickAdd.tsx      # Quick Add 마법사
│   │   │   ├── ReviewPage.tsx    # 데이터 리뷰
│   │   │   ├── Monitoring.tsx    # 모니터링
│   │   │   └── DataQuality.tsx   # 데이터 품질
│   │   └── services/             # API 클라이언트
│   ├── Dockerfile
│   └── package.json
├── airflow/                      # Airflow DAGs
│   └── dags/
│       ├── source_manager_dag.py # 소스 관리 DAG
│       ├── backup_dag.py         # 백업 DAG
│       └── dynamic_crawlers/     # 자동 생성 크롤러
├── crawlers/                     # 크롤러 엔진
├── tests/                        # 테스트
│   ├── api/                      # Backend 테스트 (466)
│   └── e2e/                      # Playwright E2E
│       ├── specs/                # 테스트 스펙
│       └── fixtures/             # 테스트 픽스처
├── .github/workflows/            # CI/CD
│   ├── ci.yml                    # Lint + Test + Build
│   ├── cd.yml                    # Deploy + Rollback
│   ├── e2e.yml                   # E2E 테스트
│   └── security.yml              # 보안 스캔
├── prometheus/                   # 메트릭 수집
├── grafana/                      # 대시보드
├── alertmanager/                 # 알림
├── nginx/                        # 리버스 프록시
├── scripts/
│   ├── deploy.sh                 # 배포 스크립트
│   ├── rollback.sh               # 롤백 스크립트
│   └── backup.sh                 # 백업 스크립트
├── docker-compose.yml            # 기본 구성
├── docker-compose.dev.yml        # 개발 오버라이드
└── docker-compose.prod.yml       # 프로덕션 오버라이드
```

## Self-Healing 시스템

크롤링 실패 시 자동 복구 파이프라인:

```
실패 감지 → 에러 분류 → AI 진단 → 코드 수정 → 테스트 → 배포
```

| 에러 코드 | 설명 | 자동 복구 |
|-----------|------|-----------|
| E001 | 요청 타임아웃 | O |
| E002 | 선택자 없음 (사이트 변경) | O |
| E003 | 인증 필요 | X |
| E004 | 사이트 구조 변경 | O |
| E005 | IP 차단 / Rate Limit | O |
| E006 | 데이터 파싱 에러 | O |
| E007 | 연결 에러 | O |
| E008 | HTTP 에러 (4xx/5xx) | O |

## 주요 API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/api/auth/login` | 로그인 |
| `GET` | `/api/sources` | 소스 목록 |
| `POST` | `/api/sources` | 소스 생성 |
| `POST` | `/api/sources/{id}/trigger` | 수동 크롤링 실행 |
| `POST` | `/api/quick-add` | AI Quick Add |
| `POST` | `/api/quick-add/analyze` | URL 분석 |
| `GET` | `/api/crawlers` | 크롤러 목록 |
| `POST` | `/api/crawlers/{id}/rollback/{ver}` | 크롤러 롤백 |
| `GET` | `/api/reviews/queue` | 리뷰 대기열 |
| `PUT` | `/api/reviews/{id}` | 리뷰 처리 |
| `POST` | `/api/reviews/batch-approve` | 일괄 승인 |
| `GET` | `/api/dashboard` | 대시보드 통계 |
| `GET` | `/api/monitoring/pipelines` | 파이프라인 상태 |
| `GET` | `/health` | 헬스체크 |

전체 API 문서: http://localhost:8000/docs

## 배포

### 프로덕션 실행

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### 배포 스크립트

```bash
# 빌드 + 배포
./scripts/deploy.sh

# 빌드만
./scripts/deploy.sh --build-only

# 롤백
./scripts/rollback.sh

# 특정 버전으로 롤백
./scripts/rollback.sh --to 20260213-abc1234

# 배포 히스토리 확인
./scripts/rollback.sh --list

# 현재 배포 상태 검증
./scripts/rollback.sh --verify
```

### CI/CD 파이프라인

```
Push → Lint → Test (466) → Build → Docker → Integration Test
                                                    │
PR to master ──────────────────────────────────────→ E2E Tests
                                                    │
Merge to master → Build Images → Staging Deploy → Smoke Tests → Production
                                                                    │
                                                      Fail? → Auto Rollback
```

## 개발

### 테스트

```bash
# Backend (pytest)
PYTHONPATH=. .venv/bin/pytest tests/ -v

# Frontend (vitest)
cd frontend && npm test

# E2E (Playwright)
cd frontend && npm run test:e2e

# 특정 브라우저
cd frontend && npm run test:e2e:chromium
```

### 코드 품질

```bash
# Python lint
ruff check api/ crawlers/ airflow/dags/

# TypeScript 타입 체크
cd frontend && npx tsc --noEmit
```

## 환경 변수

| 변수 | 필수 | 설명 |
|------|------|------|
| `OPENAI_API_KEY` | O | AI 크롤러 생성 |
| `MONGO_ROOT_PASSWORD` | O | MongoDB 비밀번호 |
| `POSTGRES_PASSWORD` | O | PostgreSQL 비밀번호 |
| `AIRFLOW_FERNET_KEY` | O | Airflow 암호화 키 |
| `JWT_SECRET_KEY` | O | JWT 토큰 서명 키 |
| `ADMIN_PASSWORD` | O | 관리자 로그인 비밀번호 |
| `AI_MODEL` | | AI 모델 (기본: `gpt-4o-mini`) |
| `AI_BASE_URL` | | 커스텀 AI API URL (DeepSeek, Qwen 등) |
| `SMTP_HOST` | | 알림 이메일 SMTP |
| `GRAFANA_ADMIN_PASSWORD` | | Grafana 관리자 비밀번호 |

전체 환경 변수 목록은 `.env.example` 참고.

## 문서

- [아키텍처](docs/architecture.md)
- [API 레퍼런스](docs/api-reference.md)
- [데이터 모델](docs/data-model.md)
- [CI/CD 파이프라인](docs/ci-cd-pipeline.md)
- [운영 런북](docs/operations-runbook.md)
- [온보딩 가이드](docs/onboarding.md)
- [예외처리 매뉴얼](docs/EXCEPTION_VALIDATION_MANUAL.md)

## 라이선스

MIT License
