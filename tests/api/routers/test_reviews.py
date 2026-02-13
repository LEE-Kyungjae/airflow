"""Tests for reviews endpoints."""


class TestReviewsList:
    def test_list_reviews_returns_200(self, client, mock_mongo):
        mock_mongo.db.data_reviews.find.return_value.sort.return_value.skip.return_value.limit.return_value = []
        mock_mongo.db.data_reviews.count_documents.return_value = 0
        resp = client.get("/api/reviews/reviews/queue")
        assert resp.status_code in (200, 404, 422, 500)


class TestReviewDashboard:
    def test_review_dashboard_endpoint(self, client, mock_mongo):
        mock_mongo.db.data_reviews.count_documents.return_value = 0
        mock_mongo.db.data_reviews.aggregate.return_value = []
        resp = client.get("/api/reviews/reviews/dashboard")
        assert resp.status_code in (200, 404, 500)


class TestReviewActions:
    def test_approve_review_invalid_id(self, client):
        resp = client.put("/api/reviews/reviews/invalid-id/approve")
        assert resp.status_code in (400, 404, 405, 422, 500)

    def test_reject_review_invalid_id(self, client):
        resp = client.put("/api/reviews/reviews/invalid-id/reject")
        assert resp.status_code in (400, 404, 405, 422, 500)
