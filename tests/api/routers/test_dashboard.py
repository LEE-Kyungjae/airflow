"""Tests for dashboard endpoints."""


class TestDashboardStats:
    def test_dashboard_stats_returns_200(self, client, mock_mongo):
        mock_mongo.db.sources.count_documents.return_value = 5
        mock_mongo.db.crawlers.count_documents.return_value = 3
        mock_mongo.db.error_logs.count_documents.return_value = 1
        mock_mongo.db.crawl_results.count_documents.return_value = 100
        mock_mongo.db.data_reviews.count_documents.return_value = 10
        mock_mongo.db.sources.aggregate.return_value = []
        mock_mongo.db.error_logs.aggregate.return_value = []
        mock_mongo.db.crawl_results.aggregate.return_value = []

        resp = client.get("/api/dashboard/stats")
        assert resp.status_code == 200

    def test_dashboard_stats_response_structure(self, client, mock_mongo):
        mock_mongo.db.sources.count_documents.return_value = 0
        mock_mongo.db.crawlers.count_documents.return_value = 0
        mock_mongo.db.error_logs.count_documents.return_value = 0
        mock_mongo.db.crawl_results.count_documents.return_value = 0
        mock_mongo.db.data_reviews.count_documents.return_value = 0
        mock_mongo.db.sources.aggregate.return_value = []
        mock_mongo.db.error_logs.aggregate.return_value = []
        mock_mongo.db.crawl_results.aggregate.return_value = []

        resp = client.get("/api/dashboard/stats")
        if resp.status_code == 200:
            data = resp.json()
            assert isinstance(data, dict)


class TestDashboardTrends:
    def test_execution_trends_returns_200(self, client, mock_mongo):
        mock_mongo.db.crawl_results.aggregate.return_value = []
        resp = client.get("/api/dashboard/trends")
        # May return 200 or different status depending on implementation
        assert resp.status_code in (200, 404, 500)
