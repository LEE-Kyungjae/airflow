신규 개발자 온보딩 가이드
=========================

사전 준비
  Docker Desktop 설치 (메모리 4GB 이상 할당)
  Node.js 18+ (프론트엔드)
  Python 3.12+ (로컬 개발 시)
  Git

프로젝트 세팅 (5분)
  git clone <repo-url>
  cd airflow-crawler-system
  cp .env.example .env
  .env 파일에서 OPENAI_API_KEY 설정 (필수)
  .env 파일에서 MONGO_ROOT_PASSWORD 설정 (필수)

실행
  전체 서비스:  docker compose up -d
  개발 모드:    docker compose -f docker-compose.dev.yml up -d
  프론트엔드:   cd frontend && npm install && npm run dev

확인
  API 문서:     http://localhost:8000/docs
  API 헬스:     http://localhost:8000/health
  Airflow UI:   http://localhost:8080 (airflow/airflow)
  프론트엔드:   http://localhost:5173
  Grafana:      http://localhost:3001 (admin/admin)

디렉토리 구조 핵심
  api/app/routers/     API 엔드포인트 정의. 새 엔드포인트는 여기에 추가.
  api/app/services/    비즈니스 로직. 라우터에서 호출.
  api/app/auth/        JWT, API Key 인증 로직.
  airflow/dags/        DAG 정의. utils/ 하위에 공통 유틸리티.
  crawlers/            크롤러 클래스들. BaseCrawler 상속.
  frontend/src/pages/  페이지 컴포넌트.
  frontend/src/api/    API 호출 함수.
  tests/               테스트. pytest 사용.

코드 작성 규칙
  Python: ruff check + black format. 타입 힌트 사용.
  TypeScript: TypeScript strict mode. 인터페이스 정의 필수.
  커밋: conventional commits (feat:, fix:, chore:)
  브랜치: feature/ , fix/ , chore/ 접두사

새 크롤링 소스 추가 방법
  1. POST /api/sources 로 소스 정의 (URL, 필드, 스케줄)
  2. POST /api/sources/{id}/generate-crawler 로 크롤러 코드 생성
  3. POST /api/sources/{id}/trigger 로 수동 실행 테스트
  4. 성공하면 Airflow가 스케줄에 따라 자동 실행

새 API 엔드포인트 추가 방법
  1. api/app/routers/ 에 라우터 파일 생성 또는 기존 파일에 추가
  2. 필요하면 api/app/services/ 에 서비스 로직 작성
  3. api/app/main.py 에서 app.include_router() 등록
  4. tests/ 에 테스트 작성
  5. ruff check api/ && black --check api/ 확인

테스트 실행
  전체: pytest tests/ -v
  특정: pytest tests/api/test_auth.py -v
  커버리지: pytest tests/ --cov=api/app --cov-report=term-missing

자주 쓰는 API
  GET  /api/sources                  소스 목록
  POST /api/sources                  소스 생성
  POST /api/sources/{id}/trigger     수동 크롤링 실행
  GET  /api/dashboard/stats          대시보드 통계
  GET  /api/errors                   에러 목록
  GET  /api/monitoring/health        시스템 상태
  POST /api/quick-add                URL만으로 자동 소스 생성

문제 해결
  "MongoDB connection failed"  → .env의 MONGO_ROOT_PASSWORD 확인, MongoDB 컨테이너 상태 확인
  "DAG import error"           → airflow/dags/ 파일 문법 오류 확인
  "Rate limit exceeded"        → 인증 없이 20req/min 제한. API Key 사용.
  프론트엔드 CORS 에러         → API의 ALLOWED_ORIGINS 확인, 개발시 AUTH_MODE=disabled
