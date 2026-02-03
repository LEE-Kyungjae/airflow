"""
MongoDB Service for FastAPI.

Provides database operations for the REST API.
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from bson import ObjectId
from pymongo import MongoClient, DESCENDING

logger = logging.getLogger(__name__)


class MongoService:
    """MongoDB service for API operations."""

    def __init__(self):
        """Initialize MongoDB connection."""
        self.uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        self.database_name = os.getenv('MONGODB_DATABASE', 'crawler_system')
        self._client: Optional[MongoClient] = None

    @property
    def client(self) -> MongoClient:
        if self._client is None:
            self._client = MongoClient(self.uri)
        return self._client

    @property
    def db(self):
        return self.client[self.database_name]

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def _serialize_doc(self, doc: Dict) -> Dict:
        """Convert MongoDB document to JSON-serializable format."""
        if doc is None:
            return None

        result = {}
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                result[key] = str(value)
            elif isinstance(value, datetime):
                result[key] = value
            else:
                result[key] = value
        return result

    # Sources
    def create_source(self, data: Dict[str, Any]) -> str:
        now = datetime.utcnow()
        data.update({
            'status': 'inactive',
            'error_count': 0,
            'created_at': now,
            'updated_at': now
        })
        result = self.db.sources.insert_one(data)
        return str(result.inserted_id)

    def get_source(self, source_id: str) -> Optional[Dict]:
        try:
            doc = self.db.sources.find_one({'_id': ObjectId(source_id)})
            return self._serialize_doc(doc)
        except Exception:
            return None

    def get_source_by_name(self, name: str) -> Optional[Dict]:
        doc = self.db.sources.find_one({'name': name})
        return self._serialize_doc(doc)

    def list_sources(self, status: str = None, skip: int = 0, limit: int = 100) -> List[Dict]:
        query = {}
        if status:
            query['status'] = status
        cursor = self.db.sources.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        return [self._serialize_doc(doc) for doc in cursor]

    def count_sources(self, status: str = None) -> int:
        query = {}
        if status:
            query['status'] = status
        return self.db.sources.count_documents(query)

    def update_source(self, source_id: str, data: Dict[str, Any]) -> bool:
        data['updated_at'] = datetime.utcnow()
        result = self.db.sources.update_one(
            {'_id': ObjectId(source_id)},
            {'$set': data}
        )
        return result.modified_count > 0

    def delete_source(self, source_id: str) -> bool:
        oid = ObjectId(source_id)
        self.db.crawlers.delete_many({'source_id': oid})
        self.db.crawl_results.delete_many({'source_id': oid})
        self.db.crawler_history.delete_many({'source_id': oid})
        self.db.error_logs.delete_many({'source_id': oid})
        result = self.db.sources.delete_one({'_id': oid})
        return result.deleted_count > 0

    # Crawlers
    def get_crawler(self, crawler_id: str) -> Optional[Dict]:
        try:
            doc = self.db.crawlers.find_one({'_id': ObjectId(crawler_id)})
            return self._serialize_doc(doc)
        except Exception:
            return None

    def get_active_crawler(self, source_id: str) -> Optional[Dict]:
        doc = self.db.crawlers.find_one({
            'source_id': ObjectId(source_id),
            'status': 'active'
        })
        return self._serialize_doc(doc)

    def list_crawlers(self, source_id: str = None, status: str = None, skip: int = 0, limit: int = 100) -> List[Dict]:
        query = {}
        if source_id:
            query['source_id'] = ObjectId(source_id)
        if status:
            query['status'] = status
        cursor = self.db.crawlers.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        return [self._serialize_doc(doc) for doc in cursor]

    def count_crawlers(self, status: str = None) -> int:
        query = {}
        if status:
            query['status'] = status
        return self.db.crawlers.count_documents(query)

    def get_crawler_history(self, crawler_id: str, skip: int = 0, limit: int = 50) -> List[Dict]:
        cursor = self.db.crawler_history.find(
            {'crawler_id': ObjectId(crawler_id)}
        ).sort('version', DESCENDING).skip(skip).limit(limit)
        return [self._serialize_doc(doc) for doc in cursor]

    # Results
    def get_crawl_results(self, source_id: str = None, status: str = None, skip: int = 0, limit: int = 100) -> List[Dict]:
        query = {}
        if source_id:
            query['source_id'] = ObjectId(source_id)
        if status:
            query['status'] = status
        cursor = self.db.crawl_results.find(query).sort('executed_at', DESCENDING).skip(skip).limit(limit)
        return [self._serialize_doc(doc) for doc in cursor]

    # Errors
    def list_errors(self, resolved: bool = None, source_id: str = None, skip: int = 0, limit: int = 100) -> List[Dict]:
        query = {}
        if resolved is not None:
            query['resolved'] = resolved
        if source_id:
            query['source_id'] = ObjectId(source_id)
        cursor = self.db.error_logs.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        return [self._serialize_doc(doc) for doc in cursor]

    def get_error(self, error_id: str) -> Optional[Dict]:
        try:
            doc = self.db.error_logs.find_one({'_id': ObjectId(error_id)})
            return self._serialize_doc(doc)
        except Exception:
            return None

    def count_errors(self, resolved: bool = None) -> int:
        query = {}
        if resolved is not None:
            query['resolved'] = resolved
        return self.db.error_logs.count_documents(query)

    def resolve_error(self, error_id: str, method: str, detail: str = '') -> bool:
        result = self.db.error_logs.update_one(
            {'_id': ObjectId(error_id)},
            {'$set': {
                'resolved': True,
                'resolved_at': datetime.utcnow(),
                'resolution_method': method,
                'resolution_detail': detail
            }}
        )
        return result.modified_count > 0

    # Dashboard
    def get_dashboard_stats(self) -> Dict[str, Any]:
        sources_total = self.db.sources.count_documents({})
        sources_active = self.db.sources.count_documents({'status': 'active'})
        sources_error = self.db.sources.count_documents({'status': 'error'})

        crawlers_total = self.db.crawlers.count_documents({})
        crawlers_active = self.db.crawlers.count_documents({'status': 'active'})

        recent_results = list(self.db.crawl_results.find({}, {'status': 1}).sort('executed_at', DESCENDING).limit(100))
        success = sum(1 for r in recent_results if r['status'] == 'success')
        failed = sum(1 for r in recent_results if r['status'] == 'failed')
        total = len(recent_results)

        unresolved = self.db.error_logs.count_documents({'resolved': False})

        return {
            'sources': {'total': sources_total, 'active': sources_active, 'error': sources_error},
            'crawlers': {'total': crawlers_total, 'active': crawlers_active},
            'recent_executions': {
                'total': total,
                'success': success,
                'failed': failed,
                'success_rate': round(success / total * 100, 2) if total > 0 else 0
            },
            'unresolved_errors': unresolved,
            'timestamp': datetime.utcnow()
        }
