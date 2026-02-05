# Airflow Crawler System

AI 기반 웹 크롤링 자동화 시스템. GPT를 활용한 크롤러 코드 자동 생성과 Apache Airflow를 통한 스케줄링/모니터링을 지원합니다.

## 주요 기능

- **AI 크롤러 생성**: URL과 추출할 필드만 정의하면 GPT가 크롤러 코드 자동 생성
- **Self-Healing**: 크롤링 실패 시 자동 진단 및 코드 수정
- **다양한 소스 지원**: HTML, PDF, Excel, CSV 파일 크롤링
- **스케줄 관리**: Cron 표현식 기반 자동 실행
- **실시간 모니터링**: 대시보드를 통한 상태 확인
- **에러 추적**: 상세한 에러 로그 및 알림

## 기술 스택

| 구성요소 | 기술 |
|---------|-----|
| API | FastAPI 0.109.0 |
| 데이터베이스 | MongoDB 4.6.1 |
| 오케스트레이션 | Apache Airflow |
| AI | OpenAI GPT-4o-mini |
| 크롤링 | BeautifulSoup4, Selenium, pdfplumber |
| 컨테이너 | Docker, Docker Compose |

## 빠른 시작

### 1. 환경 설정

```bash
# 저장소 클론
git clone https://github.com/your-repo/airflow-crawler-system.git
cd airflow-crawler-system

# 환경 변수 설정
cp .env.example .env
```

`.env` 파일 수정:

```env
# 필수 설정
OPENAI_API_KEY=your-openai-api-key
MONGODB_URI=mongodb://localhost:27017
AIRFLOW_WEBSERVER_URL=http://localhost:8080

# 인증 설정 (프로덕션 필수)
API_MASTER_KEYS=your-secure-api-key
JWT_SECRET_KEY=your-jwt-secret-key
ADMIN_PASSWORD=your-admin-password

# 선택 설정
ENV=development  # development | production
AUTH_MODE=optional  # disabled | optional | required
```

### 2. Docker로 실행

```bash
# 전체 서비스 시작
docker-compose up -d

# 로그 확인
docker-compose logs -f api
```

### 3. 수동 설치

```bash
# API 서버
cd api
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000

# Airflow (별도 터미널)
cd airflow
pip install -r requirements.txt
airflow standalone
```

## API 사용법

### 인증

모든 쓰기 작업은 인증이 필요합니다. 두 가지 방식 지원:

#### 1. API Key (서비스 간 통신용)

```bash
# 헤더 방식
curl -X POST http://localhost:8000/api/sources \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"name": "example", "url": "https://example.com", ...}'

# 쿼리 파라미터 방식
curl "http://localhost:8000/api/sources?api_key=your-api-key"
```

#### 2. JWT Token (사용자 인증용)

```bash
# 로그인
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin123"}'

# 응답에서 access_token 사용
curl -X GET http://localhost:8000/api/sources \
  -H "Authorization: Bearer eyJ..."
```

### 소스 생성

```bash
curl -X POST http://localhost:8000/api/sources \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "tech-news",
    "url": "https://news.example.com",
    "type": "html",
    "schedule": "0 */6 * * *",
    "fields": [
      {
        "name": "title",
        "selector": "h2.article-title",
        "data_type": "string"
      },
      {
        "name": "published_at",
        "selector": "time.date",
        "data_type": "date",
        "attribute": "datetime"
      }
    ]
  }'
```

### Quick Add (URL만으로 자동 설정)

```bash
curl -X POST http://localhost:8000/api/quick-add \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://news.example.com/articles",
    "name": "auto-news"
  }'
```

GPT가 페이지를 분석하여 자동으로 필드와 선택자를 설정합니다.

## 프로젝트 구조

```
airflow-crawler-system/
├── api/                      # FastAPI REST API
│   ├── app/
│   │   ├── auth/             # 인증 모듈
│   │   ├── models/           # Pydantic 스키마
│   │   ├── routers/          # API 엔드포인트
│   │   └── services/         # 비즈니스 로직
│   └── tests/                # API 테스트
├── airflow/                  # Apache Airflow
│   ├── dags/
│   │   ├── dynamic_crawlers/ # 자동 생성된 크롤러 DAG
│   │   └── utils/            # 유틸리티 모듈
│   └── plugins/
├── docs/                     # 문서
├── scripts/                  # 초기화 스크립트
└── docker-compose.yml
```

## 주요 엔드포인트

| 메서드 | 경로 | 설명 | 인증 |
|-------|------|------|-----|
| POST | `/api/auth/login` | 로그인 | - |
| GET | `/api/sources` | 소스 목록 | 선택 |
| POST | `/api/sources` | 소스 생성 | write |
| PUT | `/api/sources/{id}` | 소스 수정 | write |
| DELETE | `/api/sources/{id}` | 소스 삭제 | delete |
| POST | `/api/sources/{id}/trigger` | 수동 실행 | write |
| POST | `/api/quick-add` | 자동 설정 | write |
| GET | `/api/dashboard/stats` | 대시보드 통계 | 선택 |
| GET | `/api/errors` | 에러 로그 | 선택 |
| GET | `/api/monitoring/health` | 서비스 상태 | - |

전체 API 문서: http://localhost:8000/docs

## 환경 변수

### 필수

| 변수 | 설명 | 예시 |
|-----|------|-----|
| `OPENAI_API_KEY` | OpenAI API 키 | `sk-...` |
| `MONGODB_URI` | MongoDB 연결 URI | `mongodb://localhost:27017` |
| `AIRFLOW_WEBSERVER_URL` | Airflow 웹서버 URL | `http://localhost:8080` |

### 인증

| 변수 | 설명 | 기본값 |
|-----|------|-------|
| `API_MASTER_KEYS` | 마스터 API 키 (쉼표 구분) | `dev-api-key-12345` |
| `JWT_SECRET_KEY` | JWT 시크릿 키 | 개발용 기본값 |
| `JWT_EXPIRE_MINUTES` | 토큰 만료 시간 (분) | `60` |
| `ADMIN_PASSWORD` | 관리자 비밀번호 | `admin123` |
| `AUTH_MODE` | 인증 모드 | `optional` |

### 선택

| 변수 | 설명 | 기본값 |
|-----|------|-------|
| `ENV` | 환경 | `development` |
| `ALLOWED_ORIGINS` | CORS 허용 오리진 | `*` |
| `ALLOWED_HOSTS` | 허용 호스트 | - |
| `LOG_LEVEL` | 로그 레벨 | `INFO` |

## Self-Healing 시스템

크롤링 실패 시 자동으로:

1. **에러 분류**: 10가지 에러 코드로 자동 분류
2. **진단**: GPT가 에러 원인 분석
3. **해결책 탐색**: Wellknown case DB 또는 GPT 생성
4. **코드 수정**: 자동 코드 패치 적용
5. **테스트**: 수정된 코드 검증
6. **학습**: 성공한 해결책 저장

### 에러 코드

| 코드 | 설명 | 자동 복구 |
|-----|------|----------|
| E001 | 요청 타임아웃 | ✅ |
| E002 | 선택자 없음 | ✅ |
| E003 | 인증 필요 | ❌ |
| E004 | 사이트 구조 변경 | ✅ |
| E005 | IP 차단/Rate Limit | ✅ |
| E006 | 데이터 파싱 에러 | ✅ |
| E007 | 연결 에러 | ✅ |
| E008 | HTTP 에러 | ✅ |
| E009 | 파일 에러 | ❌ |
| E010 | 알 수 없음 | ❌ |

## 개발

### 테스트 실행

```bash
# API 테스트
cd api
pytest tests/ -v

# 커버리지 포함
pytest tests/ --cov=app --cov-report=html
```

### 코드 스타일

```bash
# Linting
ruff check .

# 포맷팅
ruff format .
```

### 로컬 개발 모드

```bash
# 인증 비활성화
export AUTH_MODE=disabled

# 개발 서버 실행
uvicorn app.main:app --reload
```

## 문서

- [예외처리/Validation 매뉴얼](docs/EXCEPTION_VALIDATION_MANUAL.md)
- [API 문서](http://localhost:8000/docs) (서버 실행 후)

## 라이선스

MIT License

## 기여

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request