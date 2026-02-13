운영 런북 (Operations Runbook)
==============================

서비스 상태 확인
  docker compose ps                            전체 서비스 상태
  curl http://localhost:8000/health            API 헬스체크
  curl http://localhost:8080/health            Airflow 헬스체크
  docker compose logs -f api --tail 100        API 실시간 로그
  docker compose logs -f airflow-scheduler     스케줄러 로그

장애 대응 절차

  [API 무응답]
  1. docker compose logs api --tail 50 으로 에러 확인
  2. docker compose restart api
  3. 30초 후 curl http://localhost:8000/health 확인
  4. 안 되면 docker compose down api && docker compose up -d api
  5. MongoDB 연결 확인: docker compose exec api python -c "from pymongo import MongoClient; c=MongoClient('mongodb://mongodb:27017'); c.admin.command('ping')"

  [Airflow DAG 실행 안 됨]
  1. Airflow UI (http://localhost:8080) 에서 DAG 상태 확인
  2. DAG가 paused 상태인지 확인 → unpause
  3. docker compose logs airflow-scheduler --tail 50
  4. docker compose restart airflow-scheduler
  5. DB 마이그레이션 필요한 경우: docker compose run airflow-init

  [MongoDB 연결 실패]
  1. docker compose logs mongodb --tail 30
  2. 디스크 공간 확인: docker system df
  3. docker compose restart mongodb
  4. 데이터 볼륨 확인: docker volume inspect airflow-crawler-system_mongodb-data

  [크롤링 실패율 급등]
  1. GET /api/errors?limit=20 으로 최근 에러 확인
  2. GET /api/monitoring/health 로 self-healing 상태 확인
  3. 특정 소스만 실패하면: 해당 사이트 접근 가능 여부 확인
  4. 전체 실패면: Selenium/Playwright 컨테이너 확인
     docker compose logs chrome --tail 20
     docker compose restart selenium-hub chrome

  [메모리 부족 / OOM]
  1. docker stats 로 컨테이너별 메모리 사용량 확인
  2. 문제 컨테이너 재시작
  3. docker system prune -f 로 미사용 리소스 정리
  4. 리소스 제한 확인: docker compose config | grep -A2 mem_limit

백업/복구

  수동 백업
    POST /api/backup/trigger  (API로 MongoDB 백업 트리거)

  자동 백업
    Airflow backup_dag 이 설정된 스케줄에 따라 실행
    백업 위치: /data/backups (local) 또는 S3/GCS

  복구
    POST /api/backup/restore  (API로 복구 트리거)
    주의: 복구 시 현재 데이터 덮어씌워짐. 반드시 현재 상태 먼저 백업.

스케일링

  크롤링 병렬성 높이기
    docker-compose.yml에서 chrome 서비스의 SE_NODE_MAX_SESSIONS 값 조정 (기본 3)
    chrome 노드 추가: docker compose up -d --scale chrome=3

  API 인스턴스 추가
    nginx 리버스 프록시 뒤에서 docker compose up -d --scale api=3

로그 확인 경로
  API 로그           docker compose logs api
  Airflow 실행 로그  ./airflow/logs/ (볼륨 마운트)
  Grafana 대시보드   http://localhost:3001 (admin/admin)
  Prometheus 쿼리    http://localhost:9090
  Loki 로그 검색     Grafana > Explore > Loki 데이터소스

정기 점검 항목 (주 1회)
  디스크 사용량        docker system df
  MongoDB 컬렉션 크기  db.stats() 확인
  에러 로그 추이       Grafana 대시보드
  백업 정상 실행 여부  GET /api/backup/
  인증서 만료일        nginx SSL 인증서 확인
  Dependabot 알림     GitHub Security 탭 확인
