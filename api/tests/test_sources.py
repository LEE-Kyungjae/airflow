"""
Sources API Tests
소스 CRUD 테스트
"""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock


class TestListSources:
    """소스 목록 조회 테스트"""

    def test_list_sources_empty(self, client):
        """빈 소스 목록"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.list_sources.return_value = []
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/sources")
            assert response.status_code == 200
            assert response.json() == []

    def test_list_sources_with_data(self, client, sample_source_response):
        """소스 목록 조회"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.list_sources.return_value = [sample_source_response]
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/sources")
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 1
            assert data[0]["name"] == "test-source"

    def test_list_sources_with_filter(self, client):
        """상태 필터링"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.list_sources.return_value = []
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/sources?status=active")
            assert response.status_code == 200
            mock_instance.list_sources.assert_called_with(
                status="active", skip=0, limit=100
            )

    def test_list_sources_pagination(self, client):
        """페이지네이션"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.list_sources.return_value = []
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/sources?skip=10&limit=50")
            assert response.status_code == 200
            mock_instance.list_sources.assert_called_with(
                status=None, skip=10, limit=50
            )

    def test_list_sources_invalid_status(self, client):
        """유효하지 않은 상태 값"""
        response = client.get("/api/sources?status=invalid")
        assert response.status_code == 422

    def test_list_sources_invalid_pagination(self, client):
        """유효하지 않은 페이지네이션"""
        # 음수 skip
        response = client.get("/api/sources?skip=-1")
        assert response.status_code == 422

        # 0 limit
        response = client.get("/api/sources?limit=0")
        assert response.status_code == 422

        # 초과 limit
        response = client.get("/api/sources?limit=1000")
        assert response.status_code == 422


class TestGetSource:
    """단일 소스 조회 테스트"""

    def test_get_source_success(self, client, sample_source_response):
        """소스 조회 성공"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = sample_source_response
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/sources/507f1f77bcf86cd799439011")
            assert response.status_code == 200
            assert response.json()["name"] == "test-source"

    def test_get_source_not_found(self, client):
        """소스 없음"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = None
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/sources/nonexistent-id")
            assert response.status_code == 404


class TestCreateSource:
    """소스 생성 테스트"""

    def test_create_source_success(self, client, auth_headers, sample_source):
        """소스 생성 성공"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source_by_name.return_value = None
            mock_instance.create_source.return_value = "new-source-id"
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            with patch('app.routers.sources.AirflowTrigger') as mock_airflow:
                mock_airflow_instance = MagicMock()
                mock_airflow_instance.trigger_dag = AsyncMock(return_value={
                    "success": True,
                    "run_id": "test-run-id",
                    "message": "DAG triggered"
                })
                mock_airflow.return_value = mock_airflow_instance

                response = client.post(
                    "/api/sources",
                    headers=auth_headers,
                    json=sample_source
                )
                assert response.status_code == 201
                data = response.json()
                assert data["success"] == True
                assert "new-source-id" in data["message"]

    def test_create_source_duplicate(self, client, auth_headers, sample_source):
        """중복 소스 생성 시도"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source_by_name.return_value = {"name": "test-source"}
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.post(
                "/api/sources",
                headers=auth_headers,
                json=sample_source
            )
            assert response.status_code == 409

    def test_create_source_no_auth(self, client, sample_source):
        """인증 없이 생성 시도"""
        response = client.post("/api/sources", json=sample_source)
        # AUTH_MODE=optional이므로 인증 필요 엔드포인트는 401
        assert response.status_code == 401

    def test_create_source_invalid_url(self, client, auth_headers, sample_source):
        """유효하지 않은 URL"""
        sample_source["url"] = "not-a-valid-url"
        response = client.post(
            "/api/sources",
            headers=auth_headers,
            json=sample_source
        )
        assert response.status_code == 422

    def test_create_source_invalid_schedule(self, client, auth_headers, sample_source):
        """유효하지 않은 스케줄"""
        sample_source["schedule"] = "invalid cron"
        response = client.post(
            "/api/sources",
            headers=auth_headers,
            json=sample_source
        )
        assert response.status_code == 422

    def test_create_source_missing_fields(self, client, auth_headers):
        """필수 필드 누락"""
        response = client.post(
            "/api/sources",
            headers=auth_headers,
            json={"name": "test"}  # url, type, fields, schedule 누락
        )
        assert response.status_code == 422

    def test_create_source_empty_fields(self, client, auth_headers, sample_source):
        """빈 필드 목록"""
        sample_source["fields"] = []
        response = client.post(
            "/api/sources",
            headers=auth_headers,
            json=sample_source
        )
        assert response.status_code == 422

    def test_create_source_invalid_type(self, client, auth_headers, sample_source):
        """유효하지 않은 타입"""
        sample_source["type"] = "invalid"
        response = client.post(
            "/api/sources",
            headers=auth_headers,
            json=sample_source
        )
        assert response.status_code == 422


class TestUpdateSource:
    """소스 수정 테스트"""

    def test_update_source_success(self, client, auth_headers, sample_source_response):
        """소스 수정 성공"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = sample_source_response
            mock_instance.update_source.return_value = True
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.put(
                "/api/sources/507f1f77bcf86cd799439011",
                headers=auth_headers,
                json={"name": "updated-name"}
            )
            assert response.status_code == 200

    def test_update_source_not_found(self, client, auth_headers):
        """존재하지 않는 소스 수정"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = None
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.put(
                "/api/sources/nonexistent-id",
                headers=auth_headers,
                json={"name": "updated-name"}
            )
            assert response.status_code == 404

    def test_update_source_empty_body(self, client, auth_headers, sample_source_response):
        """빈 업데이트 데이터"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = sample_source_response
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.put(
                "/api/sources/507f1f77bcf86cd799439011",
                headers=auth_headers,
                json={}
            )
            assert response.status_code == 400

    def test_update_source_no_auth(self, client):
        """인증 없이 수정 시도"""
        response = client.put(
            "/api/sources/507f1f77bcf86cd799439011",
            json={"name": "updated"}
        )
        assert response.status_code == 401


class TestDeleteSource:
    """소스 삭제 테스트"""

    def test_delete_source_success(self, client, auth_headers, sample_source_response):
        """소스 삭제 성공"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = sample_source_response
            mock_instance.delete_source.return_value = True
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.delete(
                "/api/sources/507f1f77bcf86cd799439011",
                headers=auth_headers
            )
            assert response.status_code == 204

    def test_delete_source_not_found(self, client, auth_headers):
        """존재하지 않는 소스 삭제"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = None
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.delete(
                "/api/sources/nonexistent-id",
                headers=auth_headers
            )
            assert response.status_code == 404

    def test_delete_source_no_auth(self, client):
        """인증 없이 삭제 시도"""
        response = client.delete("/api/sources/507f1f77bcf86cd799439011")
        assert response.status_code == 401


class TestTriggerCrawl:
    """수동 크롤링 트리거 테스트"""

    def test_trigger_crawl_success(self, client, auth_headers, sample_source_response):
        """크롤링 트리거 성공"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = sample_source_response
            mock_instance.get_active_crawler.return_value = {
                "dag_id": "crawler_test-source"
            }
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            with patch('app.routers.sources.AirflowTrigger') as mock_airflow:
                mock_airflow_instance = MagicMock()
                mock_airflow_instance.trigger_dag = AsyncMock(return_value={
                    "success": True,
                    "run_id": "manual-run-id",
                    "message": "Triggered"
                })
                mock_airflow.return_value = mock_airflow_instance

                response = client.post(
                    "/api/sources/507f1f77bcf86cd799439011/trigger",
                    headers=auth_headers
                )
                assert response.status_code == 200
                assert response.json()["success"] == True

    def test_trigger_crawl_no_crawler(self, client, auth_headers, sample_source_response):
        """활성 크롤러 없음"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = sample_source_response
            mock_instance.get_active_crawler.return_value = None
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.post(
                "/api/sources/507f1f77bcf86cd799439011/trigger",
                headers=auth_headers
            )
            assert response.status_code == 400

    def test_trigger_crawl_source_not_found(self, client, auth_headers):
        """소스 없음"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = None
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.post(
                "/api/sources/nonexistent-id/trigger",
                headers=auth_headers
            )
            assert response.status_code == 404


class TestGetSourceResults:
    """크롤링 결과 조회 테스트"""

    def test_get_results_success(
        self, client, sample_source_response, sample_crawl_result
    ):
        """결과 조회 성공"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = sample_source_response
            mock_instance.get_crawl_results.return_value = [sample_crawl_result]
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/sources/507f1f77bcf86cd799439011/results")
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 1

    def test_get_results_source_not_found(self, client):
        """소스 없음"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = None
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get("/api/sources/nonexistent-id/results")
            assert response.status_code == 404

    def test_get_results_pagination(self, client, sample_source_response):
        """결과 페이지네이션"""
        with patch('app.routers.sources.MongoService') as mock_mongo:
            mock_instance = MagicMock()
            mock_instance.get_source.return_value = sample_source_response
            mock_instance.get_crawl_results.return_value = []
            mock_mongo.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_mongo.return_value.__exit__ = MagicMock(return_value=False)

            response = client.get(
                "/api/sources/507f1f77bcf86cd799439011/results?skip=5&limit=10"
            )
            assert response.status_code == 200
            mock_instance.get_crawl_results.assert_called_with(
                source_id="507f1f77bcf86cd799439011",
                skip=5,
                limit=10
            )
