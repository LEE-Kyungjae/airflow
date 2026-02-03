"""
MongoDB Service for data persistence.

This module handles all database operations for the crawler system,
including sources, crawlers, results, history, and error logs.
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from bson import ObjectId
from pymongo import MongoClient, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

logger = logging.getLogger(__name__)


class MongoService:
    """Service for MongoDB operations."""

    def __init__(
        self,
        uri: Optional[str] = None,
        database: Optional[str] = None
    ):
        """
        Initialize MongoDB Service.

        Args:
            uri: MongoDB connection URI. Defaults to MONGODB_URI env var.
            database: Database name. Defaults to MONGODB_DATABASE env var.
        """
        self.uri = uri or os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        self.database_name = database or os.getenv('MONGODB_DATABASE', 'crawler_system')

        self._client: Optional[MongoClient] = None
        self._db: Optional[Database] = None

    @property
    def client(self) -> MongoClient:
        """Get or create MongoDB client."""
        if self._client is None:
            self._client = MongoClient(self.uri)
        return self._client

    @property
    def db(self) -> Database:
        """Get database instance."""
        if self._db is None:
            self._db = self.client[self.database_name]
        return self._db

    def close(self):
        """Close the MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None

    # ==================== Sources Collection ====================

    def create_source(self, source_data: Dict[str, Any]) -> str:
        """
        Create a new source.

        Args:
            source_data: Source information

        Returns:
            Created source ID as string
        """
        now = datetime.utcnow()
        source_data.update({
            'status': source_data.get('status', 'active'),
            'error_count': 0,
            'created_at': now,
            'updated_at': now
        })

        result = self.db.sources.insert_one(source_data)
        logger.info(f"Created source with ID: {result.inserted_id}")
        return str(result.inserted_id)

    def get_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get source by ID."""
        try:
            source = self.db.sources.find_one({'_id': ObjectId(source_id)})
            if source:
                source['_id'] = str(source['_id'])
            return source
        except Exception as e:
            logger.error(f"Error getting source {source_id}: {e}")
            return None

    def get_source_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get source by name."""
        source = self.db.sources.find_one({'name': name})
        if source:
            source['_id'] = str(source['_id'])
        return source

    def list_sources(
        self,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List sources with optional filtering."""
        query = {}
        if status:
            query['status'] = status

        cursor = self.db.sources.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        sources = []
        for source in cursor:
            source['_id'] = str(source['_id'])
            sources.append(source)
        return sources

    def update_source(self, source_id: str, update_data: Dict[str, Any]) -> bool:
        """Update a source."""
        update_data['updated_at'] = datetime.utcnow()
        result = self.db.sources.update_one(
            {'_id': ObjectId(source_id)},
            {'$set': update_data}
        )
        return result.modified_count > 0

    def update_source_status(
        self,
        source_id: str,
        status: str,
        last_run: Optional[datetime] = None,
        last_success: Optional[datetime] = None,
        increment_error: bool = False
    ) -> bool:
        """Update source execution status."""
        update = {
            '$set': {
                'status': status,
                'updated_at': datetime.utcnow()
            }
        }

        if last_run:
            update['$set']['last_run'] = last_run
        if last_success:
            update['$set']['last_success'] = last_success
        if increment_error:
            update['$inc'] = {'error_count': 1}
        elif status == 'active':
            update['$set']['error_count'] = 0

        result = self.db.sources.update_one(
            {'_id': ObjectId(source_id)},
            update
        )
        return result.modified_count > 0

    def delete_source(self, source_id: str) -> bool:
        """Delete a source and related data."""
        oid = ObjectId(source_id)

        # Delete related crawlers, results, history, and errors
        self.db.crawlers.delete_many({'source_id': oid})
        self.db.crawl_results.delete_many({'source_id': oid})
        self.db.crawler_history.delete_many({'source_id': oid})
        self.db.error_logs.delete_many({'source_id': oid})

        result = self.db.sources.delete_one({'_id': oid})
        return result.deleted_count > 0

    # ==================== Crawlers Collection ====================

    def create_crawler(self, crawler_data: Dict[str, Any]) -> str:
        """Create a new crawler."""
        now = datetime.utcnow()

        # Convert source_id to ObjectId if string
        if isinstance(crawler_data.get('source_id'), str):
            crawler_data['source_id'] = ObjectId(crawler_data['source_id'])

        crawler_data.update({
            'version': crawler_data.get('version', 1),
            'status': crawler_data.get('status', 'testing'),
            'created_at': now
        })

        result = self.db.crawlers.insert_one(crawler_data)
        logger.info(f"Created crawler with ID: {result.inserted_id}")
        return str(result.inserted_id)

    def get_crawler(self, crawler_id: str) -> Optional[Dict[str, Any]]:
        """Get crawler by ID."""
        try:
            crawler = self.db.crawlers.find_one({'_id': ObjectId(crawler_id)})
            if crawler:
                crawler['_id'] = str(crawler['_id'])
                crawler['source_id'] = str(crawler['source_id'])
            return crawler
        except Exception as e:
            logger.error(f"Error getting crawler {crawler_id}: {e}")
            return None

    def get_active_crawler_for_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get the active crawler for a source."""
        crawler = self.db.crawlers.find_one({
            'source_id': ObjectId(source_id),
            'status': 'active'
        })
        if crawler:
            crawler['_id'] = str(crawler['_id'])
            crawler['source_id'] = str(crawler['source_id'])
        return crawler

    def get_crawler_by_dag_id(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """Get crawler by DAG ID."""
        crawler = self.db.crawlers.find_one({'dag_id': dag_id})
        if crawler:
            crawler['_id'] = str(crawler['_id'])
            crawler['source_id'] = str(crawler['source_id'])
        return crawler

    def list_crawlers(
        self,
        source_id: Optional[str] = None,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List crawlers with optional filtering."""
        query = {}
        if source_id:
            query['source_id'] = ObjectId(source_id)
        if status:
            query['status'] = status

        cursor = self.db.crawlers.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        crawlers = []
        for crawler in cursor:
            crawler['_id'] = str(crawler['_id'])
            crawler['source_id'] = str(crawler['source_id'])
            crawlers.append(crawler)
        return crawlers

    def update_crawler(self, crawler_id: str, update_data: Dict[str, Any]) -> bool:
        """Update a crawler."""
        result = self.db.crawlers.update_one(
            {'_id': ObjectId(crawler_id)},
            {'$set': update_data}
        )
        return result.modified_count > 0

    def update_crawler_code(
        self,
        crawler_id: str,
        new_code: str,
        created_by: str = 'gpt',
        gpt_prompt: Optional[str] = None
    ) -> bool:
        """Update crawler code and increment version."""
        crawler = self.get_crawler(crawler_id)
        if not crawler:
            return False

        new_version = crawler['version'] + 1

        update_data = {
            'code': new_code,
            'version': new_version,
            'created_by': created_by
        }
        if gpt_prompt:
            update_data['gpt_prompt'] = gpt_prompt

        return self.update_crawler(crawler_id, update_data)

    def activate_crawler(self, crawler_id: str) -> bool:
        """Activate a crawler and deactivate others for the same source."""
        crawler = self.get_crawler(crawler_id)
        if not crawler:
            return False

        # Deactivate other crawlers for this source
        self.db.crawlers.update_many(
            {
                'source_id': ObjectId(crawler['source_id']),
                '_id': {'$ne': ObjectId(crawler_id)}
            },
            {'$set': {'status': 'deprecated'}}
        )

        # Activate this crawler
        return self.update_crawler(crawler_id, {'status': 'active'})

    # ==================== Crawl Results Collection ====================

    def save_crawl_result(self, result_data: Dict[str, Any]) -> str:
        """Save crawl result."""
        # Convert IDs to ObjectId
        if isinstance(result_data.get('source_id'), str):
            result_data['source_id'] = ObjectId(result_data['source_id'])
        if isinstance(result_data.get('crawler_id'), str):
            result_data['crawler_id'] = ObjectId(result_data['crawler_id'])

        result_data['executed_at'] = result_data.get('executed_at', datetime.utcnow())

        result = self.db.crawl_results.insert_one(result_data)
        return str(result.inserted_id)

    def get_crawl_results(
        self,
        source_id: Optional[str] = None,
        crawler_id: Optional[str] = None,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get crawl results with filtering."""
        query = {}
        if source_id:
            query['source_id'] = ObjectId(source_id)
        if crawler_id:
            query['crawler_id'] = ObjectId(crawler_id)
        if status:
            query['status'] = status

        cursor = self.db.crawl_results.find(query).sort('executed_at', DESCENDING).skip(skip).limit(limit)
        results = []
        for result in cursor:
            result['_id'] = str(result['_id'])
            result['source_id'] = str(result['source_id'])
            result['crawler_id'] = str(result['crawler_id'])
            results.append(result)
        return results

    def get_latest_result(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get the latest crawl result for a source."""
        result = self.db.crawl_results.find_one(
            {'source_id': ObjectId(source_id)},
            sort=[('executed_at', DESCENDING)]
        )
        if result:
            result['_id'] = str(result['_id'])
            result['source_id'] = str(result['source_id'])
            result['crawler_id'] = str(result['crawler_id'])
        return result

    # ==================== Crawler History Collection ====================

    def save_crawler_history(
        self,
        crawler_id: str,
        version: int,
        code: str,
        change_reason: str,
        change_detail: str = '',
        changed_by: str = 'gpt'
    ) -> str:
        """Save crawler code history."""
        history_data = {
            'crawler_id': ObjectId(crawler_id),
            'version': version,
            'code': code,
            'change_reason': change_reason,
            'change_detail': change_detail,
            'changed_at': datetime.utcnow(),
            'changed_by': changed_by
        }

        result = self.db.crawler_history.insert_one(history_data)
        logger.info(f"Saved crawler history: version {version}")
        return str(result.inserted_id)

    def get_crawler_history(
        self,
        crawler_id: str,
        skip: int = 0,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get crawler code history."""
        cursor = self.db.crawler_history.find(
            {'crawler_id': ObjectId(crawler_id)}
        ).sort('version', DESCENDING).skip(skip).limit(limit)

        history = []
        for h in cursor:
            h['_id'] = str(h['_id'])
            h['crawler_id'] = str(h['crawler_id'])
            history.append(h)
        return history

    def get_crawler_version(self, crawler_id: str, version: int) -> Optional[Dict[str, Any]]:
        """Get specific version of crawler code."""
        h = self.db.crawler_history.find_one({
            'crawler_id': ObjectId(crawler_id),
            'version': version
        })
        if h:
            h['_id'] = str(h['_id'])
            h['crawler_id'] = str(h['crawler_id'])
        return h

    # ==================== Error Logs Collection ====================

    def log_error(self, error_data: Dict[str, Any]) -> str:
        """Log an error."""
        # Convert IDs to ObjectId
        if isinstance(error_data.get('source_id'), str):
            error_data['source_id'] = ObjectId(error_data['source_id'])
        if isinstance(error_data.get('crawler_id'), str):
            error_data['crawler_id'] = ObjectId(error_data['crawler_id'])

        error_data.update({
            'resolved': False,
            'created_at': datetime.utcnow()
        })

        result = self.db.error_logs.insert_one(error_data)
        logger.info(f"Logged error: {error_data.get('error_code')} - {error_data.get('message')}")
        return str(result.inserted_id)

    def get_error(self, error_id: str) -> Optional[Dict[str, Any]]:
        """Get error by ID."""
        error = self.db.error_logs.find_one({'_id': ObjectId(error_id)})
        if error:
            error['_id'] = str(error['_id'])
            error['source_id'] = str(error['source_id'])
            if error.get('crawler_id'):
                error['crawler_id'] = str(error['crawler_id'])
        return error

    def list_errors(
        self,
        source_id: Optional[str] = None,
        resolved: Optional[bool] = None,
        error_code: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List errors with filtering."""
        query = {}
        if source_id:
            query['source_id'] = ObjectId(source_id)
        if resolved is not None:
            query['resolved'] = resolved
        if error_code:
            query['error_code'] = error_code

        cursor = self.db.error_logs.find(query).sort('created_at', DESCENDING).skip(skip).limit(limit)
        errors = []
        for error in cursor:
            error['_id'] = str(error['_id'])
            error['source_id'] = str(error['source_id'])
            if error.get('crawler_id'):
                error['crawler_id'] = str(error['crawler_id'])
            errors.append(error)
        return errors

    def resolve_error(
        self,
        error_id: str,
        resolution_method: str,
        resolution_detail: str = ''
    ) -> bool:
        """Mark an error as resolved."""
        result = self.db.error_logs.update_one(
            {'_id': ObjectId(error_id)},
            {
                '$set': {
                    'resolved': True,
                    'resolved_at': datetime.utcnow(),
                    'resolution_method': resolution_method,
                    'resolution_detail': resolution_detail
                }
            }
        )
        return result.modified_count > 0

    # ==================== Statistics ====================

    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get dashboard statistics."""
        now = datetime.utcnow()

        # Source stats
        total_sources = self.db.sources.count_documents({})
        active_sources = self.db.sources.count_documents({'status': 'active'})
        error_sources = self.db.sources.count_documents({'status': 'error'})

        # Crawler stats
        total_crawlers = self.db.crawlers.count_documents({})
        active_crawlers = self.db.crawlers.count_documents({'status': 'active'})

        # Recent results
        recent_results = list(self.db.crawl_results.find(
            {},
            {'status': 1, 'executed_at': 1}
        ).sort('executed_at', DESCENDING).limit(100))

        success_count = sum(1 for r in recent_results if r['status'] == 'success')
        failed_count = sum(1 for r in recent_results if r['status'] == 'failed')

        # Unresolved errors
        unresolved_errors = self.db.error_logs.count_documents({'resolved': False})

        return {
            'sources': {
                'total': total_sources,
                'active': active_sources,
                'error': error_sources
            },
            'crawlers': {
                'total': total_crawlers,
                'active': active_crawlers
            },
            'recent_executions': {
                'total': len(recent_results),
                'success': success_count,
                'failed': failed_count,
                'success_rate': round(success_count / len(recent_results) * 100, 2) if recent_results else 0
            },
            'unresolved_errors': unresolved_errors,
            'timestamp': now.isoformat()
        }
