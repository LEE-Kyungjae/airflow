"""
Data Promotion Service for staging → production data movement.

This module handles the promotion of verified data from staging
collections to production collections after review approval.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from bson import ObjectId
from pymongo.database import Database

logger = logging.getLogger(__name__)


# Mapping of data types to their staging/production collections
COLLECTION_MAPPING = {
    'news': ('staging_news', 'news_articles'),
    'financial': ('staging_financial', 'financial_data'),
    'stock': ('staging_financial', 'stock_prices'),
    'exchange': ('staging_financial', 'exchange_rates'),
    'market': ('staging_financial', 'market_indices'),
    'announcement': ('staging_data', 'announcements'),
    'generic': ('staging_data', 'crawl_data'),
}


class DataPromotionService:
    """
    Service for promoting data from staging to production.

    Workflow:
    1. Crawler extracts data → saved to staging_*
    2. Review created with reference to staging record
    3. Reviewer approves → this service moves to production
    4. Lineage record created for audit trail
    """

    def __init__(self, db: Database):
        """
        Initialize promotion service.

        Args:
            db: MongoDB database instance
        """
        self.db = db

    def determine_collection_type(self, source_id: ObjectId) -> str:
        """
        Determine the collection type based on source configuration.

        Args:
            source_id: Source ObjectId

        Returns:
            Collection type key (news, financial, generic, etc.)
        """
        source = self.db.sources.find_one({"_id": source_id})

        if not source:
            return 'generic'

        # Check source type or name for hints
        source_type = source.get('type', '')
        source_name = source.get('name', '').lower()
        source_url = source.get('url', '').lower()

        # Simple heuristics
        if any(kw in source_name or kw in source_url for kw in ['news', '뉴스', 'article', '기사']):
            return 'news'
        elif any(kw in source_name or kw in source_url for kw in ['stock', '주식', 'finance', '금융']):
            return 'financial'
        elif any(kw in source_name or kw in source_url for kw in ['공시', 'disclosure', 'announcement']):
            return 'announcement'

        return 'generic'

    def save_to_staging(
        self,
        data: Dict[str, Any],
        source_id: ObjectId,
        crawl_result_id: ObjectId,
        record_index: int = 0,
        collection_type: Optional[str] = None
    ) -> ObjectId:
        """
        Save crawled data to staging collection.

        Args:
            data: The extracted data
            source_id: Source ObjectId
            crawl_result_id: Crawl result ObjectId
            record_index: Index within the crawl result
            collection_type: Override collection type

        Returns:
            Inserted staging document ObjectId
        """
        if collection_type is None:
            collection_type = self.determine_collection_type(source_id)

        staging_collection, _ = COLLECTION_MAPPING.get(collection_type, COLLECTION_MAPPING['generic'])

        # Add metadata
        staging_doc = {
            **data,
            '_source_id': source_id,
            '_crawl_result_id': crawl_result_id,
            '_record_index': record_index,
            '_review_status': 'pending',
            '_collection_type': collection_type,
            '_crawled_at': datetime.utcnow(),
            '_staged_at': datetime.utcnow()
        }

        result = self.db[staging_collection].insert_one(staging_doc)
        logger.info(f"Saved to staging: {staging_collection}/{result.inserted_id}")

        return result.inserted_id

    def promote_to_production(
        self,
        staging_id: ObjectId,
        reviewer_id: str,
        corrections: Optional[List[Dict[str, Any]]] = None,
        collection_type: Optional[str] = None
    ) -> Tuple[bool, Optional[ObjectId], str]:
        """
        Promote a staging record to production.

        Args:
            staging_id: Staging document ObjectId
            reviewer_id: ID of the reviewer who approved
            corrections: Optional list of field corrections
            collection_type: Override collection type

        Returns:
            Tuple of (success, production_id, message)
        """
        # Find staging record
        staging_doc = None
        staging_collection = None

        # Try to find in staging collections
        for ctype, (stg, _) in COLLECTION_MAPPING.items():
            doc = self.db[stg].find_one({"_id": staging_id})
            if doc:
                staging_doc = doc
                staging_collection = stg
                collection_type = collection_type or doc.get('_collection_type', ctype)
                break

        if not staging_doc:
            return False, None, "Staging record not found"

        _, production_collection = COLLECTION_MAPPING.get(collection_type, COLLECTION_MAPPING['generic'])

        # Prepare production document
        production_doc = {k: v for k, v in staging_doc.items() if not k.startswith('_') or k in ['_source_id', '_data_date']}

        # Apply corrections if any
        if corrections:
            for correction in corrections:
                field = correction.get('field')
                if field and field in production_doc:
                    production_doc[field] = correction.get('corrected_value')

        # Add production metadata
        production_doc.update({
            '_source_id': staging_doc['_source_id'],
            '_staging_id': staging_id,
            '_verified': True,
            '_verified_at': datetime.utcnow(),
            '_verified_by': reviewer_id,
            '_has_corrections': bool(corrections),
            '_promoted_at': datetime.utcnow(),
            '_crawled_at': staging_doc.get('_crawled_at'),
            '_data_date': staging_doc.get('_data_date', datetime.utcnow().date().isoformat())
        })

        try:
            # Insert to production
            result = self.db[production_collection].insert_one(production_doc)
            production_id = result.inserted_id

            # Update staging record status
            self.db[staging_collection].update_one(
                {"_id": staging_id},
                {
                    "$set": {
                        "_review_status": "promoted",
                        "_promoted_to": production_id,
                        "_promoted_at": datetime.utcnow()
                    }
                }
            )

            # Create lineage record
            self.db.data_lineage.insert_one({
                "staging_id": staging_id,
                "staging_collection": staging_collection,
                "production_id": production_id,
                "production_collection": production_collection,
                "source_id": staging_doc['_source_id'],
                "crawl_result_id": staging_doc.get('_crawl_result_id'),
                "reviewer_id": reviewer_id,
                "has_corrections": bool(corrections),
                "corrections": corrections or [],
                "moved_at": datetime.utcnow()
            })

            logger.info(f"Promoted {staging_collection}/{staging_id} -> {production_collection}/{production_id}")
            return True, production_id, "Successfully promoted to production"

        except Exception as e:
            logger.error(f"Failed to promote: {e}")
            return False, None, str(e)

    def rollback_promotion(
        self,
        production_id: ObjectId,
        reason: str,
        operator_id: str
    ) -> Tuple[bool, str]:
        """
        Rollback a production record (soft delete, keep staging).

        Args:
            production_id: Production document ObjectId
            reason: Reason for rollback
            operator_id: ID of operator performing rollback

        Returns:
            Tuple of (success, message)
        """
        # Find lineage record
        lineage = self.db.data_lineage.find_one({"production_id": production_id})

        if not lineage:
            return False, "Lineage record not found"

        production_collection = lineage['production_collection']
        staging_collection = lineage['staging_collection']
        staging_id = lineage['staging_id']

        try:
            # Remove from production
            self.db[production_collection].delete_one({"_id": production_id})

            # Update staging status back to pending/rolled_back
            self.db[staging_collection].update_one(
                {"_id": staging_id},
                {
                    "$set": {
                        "_review_status": "rolled_back",
                        "_rollback_reason": reason,
                        "_rolled_back_at": datetime.utcnow(),
                        "_rolled_back_by": operator_id
                    },
                    "$unset": {
                        "_promoted_to": "",
                        "_promoted_at": ""
                    }
                }
            )

            # Update lineage
            self.db.data_lineage.update_one(
                {"_id": lineage['_id']},
                {
                    "$set": {
                        "rolled_back": True,
                        "rollback_reason": reason,
                        "rolled_back_at": datetime.utcnow(),
                        "rolled_back_by": operator_id
                    }
                }
            )

            logger.info(f"Rolled back {production_collection}/{production_id}")
            return True, "Successfully rolled back"

        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            return False, str(e)

    def get_staging_stats(self, source_id: Optional[ObjectId] = None) -> Dict[str, Any]:
        """
        Get statistics for staging data.

        Args:
            source_id: Optional filter by source

        Returns:
            Statistics dictionary
        """
        match_stage = {}
        if source_id:
            match_stage = {"_source_id": source_id}

        stats = {
            "total_pending": 0,
            "total_promoted": 0,
            "total_rolled_back": 0,
            "by_collection": {}
        }

        for ctype, (staging_col, _) in COLLECTION_MAPPING.items():
            pipeline = [
                {"$match": match_stage} if match_stage else {"$match": {}},
                {"$group": {
                    "_id": "$_review_status",
                    "count": {"$sum": 1}
                }}
            ]

            results = list(self.db[staging_col].aggregate(pipeline))
            col_stats = {"pending": 0, "promoted": 0, "rolled_back": 0}

            for r in results:
                status = r['_id']
                count = r['count']
                if status == 'pending':
                    col_stats['pending'] = count
                    stats['total_pending'] += count
                elif status == 'promoted':
                    col_stats['promoted'] = count
                    stats['total_promoted'] += count
                elif status == 'rolled_back':
                    col_stats['rolled_back'] = count
                    stats['total_rolled_back'] += count

            if any(col_stats.values()):
                stats['by_collection'][staging_col] = col_stats

        return stats

    def batch_promote(
        self,
        staging_ids: List[ObjectId],
        reviewer_id: str
    ) -> Dict[str, Any]:
        """
        Batch promote multiple staging records.

        Args:
            staging_ids: List of staging ObjectIds
            reviewer_id: Reviewer ID

        Returns:
            Results summary
        """
        results = {
            "total": len(staging_ids),
            "success": 0,
            "failed": 0,
            "errors": []
        }

        for staging_id in staging_ids:
            success, prod_id, msg = self.promote_to_production(staging_id, reviewer_id)
            if success:
                results['success'] += 1
            else:
                results['failed'] += 1
                results['errors'].append({"staging_id": str(staging_id), "error": msg})

        return results

    def cleanup_old_staging(self, days: int = 30) -> int:
        """
        Clean up old promoted staging records.

        Args:
            days: Delete promoted records older than this

        Returns:
            Number of deleted records
        """
        from datetime import timedelta

        cutoff = datetime.utcnow() - timedelta(days=days)
        total_deleted = 0

        for _, (staging_col, _) in COLLECTION_MAPPING.items():
            result = self.db[staging_col].delete_many({
                "_review_status": "promoted",
                "_promoted_at": {"$lt": cutoff}
            })
            total_deleted += result.deleted_count

        logger.info(f"Cleaned up {total_deleted} old staging records")
        return total_deleted
