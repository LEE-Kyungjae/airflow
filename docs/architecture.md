Airflow Crawler System Architecture
====================================

시스템 개요
  AI 기반 웹 크롤링 자동화 플랫폼.
  URL과 필드 정의만으로 GPT가 크롤러 코드를 생성하고,
  Airflow가 스케줄 실행, 실패 시 자가치유까지 자동 처리.

구성 서비스 (13개 컨테이너)
  api (FastAPI)         :8000   REST API, 비즈니스 로직 전체
  airflow-webserver     :8080   DAG 관리 UI
  airflow-scheduler             DAG 스케줄 실행
  airflow-triggerer             이벤트 기반 트리거
  postgres (15)                 Airflow 메타데이터 DB
  mongodb (7.0)                 애플리케이션 데이터 저장소
  selenium-hub (4.16.1)         브라우저 자동화 허브
  chrome                        Selenium 노드 (동적 페이지 크롤링)
  prometheus (v2.48.0)  :9090   메트릭 수집
  pushgateway (v1.6.2)          배치 작업 메트릭 수신
  grafana (10.2.2)      :3001   시각화 대시보드
  nginx                 :80/443 리버스 프록시, SSL 터미네이션
  redis (7-alpine)      :6379   캐시, 세션 저장소

데이터 흐름
  사용자가 소스 정의 (URL, 필드, 스케줄)
  → GPT가 크롤러 Python 코드 생성
  → Airflow DAG 동적 생성
  → 스케줄에 따라 크롤러 실행 (BS4/Selenium/Playwright)
  → 추출 데이터 → MongoDB staging 컬렉션 (30일 TTL)
  → 사용자 리뷰 (승인/거절/수정)
  → 승인된 데이터 → production 컬렉션
  → 전체 과정 리니지 추적, Prometheus 메트릭 수집

자가치유 흐름
  크롤링 실패 감지
  → 에러 코드 자동 분류 (E001~E010)
  → wellknown_cases DB 조회 (기존 해결책)
  → 없으면 GPT가 에러 분석 + 코드 수정 제안
  → 자동 패치 적용 → 재실행
  → 성공 시 해결책 DB 저장 (학습)

API 레이어 구조 (20개 라우터)
  인증      auth, auth_config
  핵심      sources, crawlers, e2e_pipeline
  데이터    reviews, export, production_data
  품질      data_quality, contracts, schemas, catalog, versions, lineage
  운영      dashboard, monitoring, errors, metrics, backup
  편의      quick_add

서비스 레이어 (13개 핵심 서비스)
  mongo_service       MongoDB CRUD, 집계
  postgres_service    PostgreSQL 스키마 감지, 쿼리
  review_service      데이터 리뷰 워크플로우
  review_actions      리뷰 승인/거절/수정 액션 처리
  auto_discovery      AI 필드 자동 감지
  instant_etl         온디맨드 ETL
  e2e_pipeline        URL→크롤러→실행 전체 자동화
  change_detection    크롤링 결과 변경 감지
  data_promotion      staging→production 승격
  export_service      Excel/CSV/JSON 내보내기
  test_crawler        크롤러 코드 테스트 실행
  airflow_trigger     Airflow DAG 트리거 및 상태 조회
  pg_schema           PostgreSQL 동적 스키마 관리

서브시스템
  alerts          Discord/Slack/Email/Webhook 알림 + 쓰로틀링
  data_catalog    자산 레지스트리, 리니지, 검색
  data_contracts  품질 계약 정의, 검증, 리포팅
  data_quality    품질 규칙 엔진, 모니터링
  data_versioning 스냅샷, diff, 히스토리
  streaming       MongoDB Change Streams, 이벤트 처리
  schema_registry 스키마 감지, 호환성, 진화
  observability   Prometheus 메트릭, SLA, freshness
  idempotency     중복 방지, 멱등성 키 관리

크롤러 타입 (9종)
  HTMLCrawler          BeautifulSoup4 정적 페이지
  SPACrawler           Selenium 동적 SPA
  PlaywrightCrawler    Playwright 브라우저 자동화
  PDFCrawler           pdfplumber PDF 추출
  ExcelCrawler         openpyxl 엑셀 파싱
  CSVCrawler           CSV/TSV 파싱
  DynamicTableCrawler  동적 테이블 추출
  AuthCrawler          인증 필요 사이트
  OCRCrawler           이미지/스캔 문서 OCR

기술 스택
  언어        Python 3.12, TypeScript
  API         FastAPI 0.128.2, Uvicorn
  프론트엔드  React 18, Vite, Tailwind CSS, TanStack Query
  오케스트레이션 Apache Airflow 2.10+
  DB          MongoDB 7.0, PostgreSQL 15
  캐시        Redis 7
  AI          OpenAI GPT-4o-mini (코드생성, 자가치유, 텍스트정제)
  모니터링    Prometheus, Grafana, Loki
  크롤링      BeautifulSoup4, Selenium 4.16, Playwright
  인증        JWT + API Key, Rate Limiting (SlowAPI)
  로깅        structlog (구조화 로깅) + Correlation ID

네트워크
  모든 서비스는 crawler-network 브리지 네트워크에서 통신.
  외부 노출 포트: 80/443(nginx), 8080(airflow), 9090(prometheus), 3001(grafana)
  내부 전용: mongodb:27017, postgres:5432, redis:6379, selenium-hub:4444, pushgateway:9091
