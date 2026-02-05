"""
Review Service for bulk review operations.

This module provides services for bulk approval, rejection, and filter-based
operations on data review records with transaction support.
"""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorClientSession

from ..models.schemas import (
    BulkApproveRequest,
    BulkRejectRequest,
    BulkFilterRequest,
    BulkOperationResult,
    BulkJobStatus
)
from .data_promotion import DataPromotionService, COLLECTION_MAPPING

logger = logging.getLogger(__name__)

# In-memory job storage (replace with Redis/MongoDB for production)
_bulk_jobs: Dict[str, BulkJobStatus] = {}


def validate_object_id(value: str, context: str = "") -> Optional[ObjectId]:
    """
    Validate and convert string to ObjectId.

    Args:
        value: String ID to validate
        context: Context for error logging

    Returns:
        ObjectId if valid, None otherwise
    """
    try:
        return ObjectId(value)
    except (InvalidId, TypeError):
        logger.warning(f"Invalid ObjectId: {value} (context: {context})")
        return None


class ReviewService:
    """
    Service for bulk review operations with transaction support.

    Provides methods for:
    - Bulk approval of review records
    - Bulk rejection of review records
    - Filter-based bulk operations
    - Async job tracking for large operations
    """

    # Batch size for processing
    BATCH_SIZE = 100

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initialize review service.

        Args:
            db: Async MongoDB database instance
        """
        self.db = db

    async def bulk_approve(
        self,
        request: BulkApproveRequest,
        reviewer_id: str,
        use_transaction: bool = True
    ) -> BulkOperationResult:
        """
        Bulk approve multiple review records.

        Approves specified review IDs and promotes data from staging to production.

        Args:
            request: Bulk approve request with review IDs
            reviewer_id: ID of the reviewer performing the operation
            use_transaction: Whether to use MongoDB transaction (requires replica set)

        Returns:
            BulkOperationResult with success/failure counts
        """
        result = BulkOperationResult(
            total=len(request.review_ids),
            success=0,
            failed=0,
            failed_ids=[],
            errors=[]
        )

        # Validate all ObjectIds first
        valid_ids = []
        for rid in request.review_ids:
            oid = validate_object_id(rid, "bulk_approve")
            if oid:
                valid_ids.append((rid, oid))
            else:
                result.failed += 1
                result.failed_ids.append(rid)
                result.errors.append(f"Invalid ObjectId: {rid}")

        if not valid_ids:
            return result

        # Process in batches
        for i in range(0, len(valid_ids), self.BATCH_SIZE):
            batch = valid_ids[i:i + self.BATCH_SIZE]
            batch_result = await self._process_approve_batch(
                batch, reviewer_id, request.comment, use_transaction
            )

            result.success += batch_result["success"]
            result.failed += batch_result["failed"]
            result.failed_ids.extend(batch_result["failed_ids"])
            result.errors.extend(batch_result["errors"])

        logger.info(
            f"Bulk approve completed: {result.success}/{result.total} success, "
            f"{result.failed} failed"
        )

        return result

    async def _process_approve_batch(
        self,
        batch: List[Tuple[str, ObjectId]],
        reviewer_id: str,
        comment: Optional[str],
        use_transaction: bool
    ) -> Dict[str, Any]:
        """
        Process a batch of approve operations.

        Args:
            batch: List of (string_id, ObjectId) tuples
            reviewer_id: Reviewer ID
            comment: Optional comment
            use_transaction: Whether to use transaction

        Returns:
            Batch result dictionary
        """
        batch_result = {
            "success": 0,
            "failed": 0,
            "failed_ids": [],
            "errors": []
        }

        now = datetime.utcnow()
        oid_list = [oid for _, oid in batch]

        # Fetch all reviews in batch
        reviews = await self.db.data_reviews.find(
            {"_id": {"$in": oid_list}, "review_status": "pending"}
        ).to_list(None)

        found_ids = {str(r["_id"]) for r in reviews}

        # Mark not found as failed
        for str_id, oid in batch:
            if str_id not in found_ids:
                batch_result["failed"] += 1
                batch_result["failed_ids"].append(str_id)
                batch_result["errors"].append(f"Review not found or not pending: {str_id}")

        if not reviews:
            return batch_result

        # Update all reviews to approved
        update_data = {
            "review_status": "approved",
            "reviewer_id": reviewer_id,
            "reviewed_at": now,
            "updated_at": now
        }
        if comment:
            update_data["notes"] = comment

        try:
            # Bulk update reviews
            update_result = await self.db.data_reviews.update_many(
                {"_id": {"$in": [r["_id"] for r in reviews]}},
                {"$set": update_data}
            )

            # Promote each to production
            for review in reviews:
                staging_id = review.get("staging_id")
                if staging_id:
                    success, prod_id, msg = await self._promote_to_production_async(
                        staging_id=ObjectId(staging_id) if isinstance(staging_id, str) else staging_id,
                        reviewer_id=reviewer_id
                    )

                    if success:
                        # Update review with production reference
                        await self.db.data_reviews.update_one(
                            {"_id": review["_id"]},
                            {"$set": {
                                "production_id": prod_id,
                                "promoted_at": now
                            }}
                        )
                        batch_result["success"] += 1
                    else:
                        batch_result["failed"] += 1
                        batch_result["failed_ids"].append(str(review["_id"]))
                        batch_result["errors"].append(f"Promotion failed: {msg}")
                else:
                    # No staging_id, just count as success (legacy data)
                    batch_result["success"] += 1

        except Exception as e:
            logger.exception(f"Batch approve error: {e}")
            for review in reviews:
                batch_result["failed"] += 1
                batch_result["failed_ids"].append(str(review["_id"]))
                batch_result["errors"].append(f"Database error: {str(e)}")

        return batch_result

    async def _promote_to_production_async(
        self,
        staging_id: ObjectId,
        reviewer_id: str,
        corrections: Optional[List[Dict[str, Any]]] = None
    ) -> Tuple[bool, Optional[ObjectId], str]:
        """
        Async version of promote_to_production.

        Args:
            staging_id: Staging document ObjectId
            reviewer_id: Reviewer ID
            corrections: Optional corrections

        Returns:
            Tuple of (success, production_id, message)
        """
        # Find staging record across collections
        staging_doc = None
        staging_collection = None
        collection_type = None

        for ctype, (stg, _) in COLLECTION_MAPPING.items():
            doc = await self.db[stg].find_one({"_id": staging_id})
            if doc:
                staging_doc = doc
                staging_collection = stg
                collection_type = doc.get('_collection_type', ctype)
                break

        if not staging_doc:
            return False, None, "Staging record not found"

        _, production_collection = COLLECTION_MAPPING.get(
            collection_type, COLLECTION_MAPPING['generic']
        )

        # Prepare production document
        production_doc = {
            k: v for k, v in staging_doc.items()
            if not k.startswith('_') or k in ['_source_id', '_data_date']
        }

        # Apply corrections if any
        if corrections:
            for correction in corrections:
                field = correction.get('field')
                if field and field in production_doc:
                    production_doc[field] = correction.get('corrected_value')

        # Add production metadata
        now = datetime.utcnow()
        production_doc.update({
            '_source_id': staging_doc['_source_id'],
            '_staging_id': staging_id,
            '_verified': True,
            '_verified_at': now,
            '_verified_by': reviewer_id,
            '_has_corrections': bool(corrections),
            '_promoted_at': now,
            '_crawled_at': staging_doc.get('_crawled_at'),
            '_data_date': staging_doc.get('_data_date', now.date().isoformat())
        })

        try:
            # Insert to production
            result = await self.db[production_collection].insert_one(production_doc)
            production_id = result.inserted_id

            # Update staging record status
            await self.db[staging_collection].update_one(
                {"_id": staging_id},
                {
                    "$set": {
                        "_review_status": "promoted",
                        "_promoted_to": production_id,
                        "_promoted_at": now
                    }
                }
            )

            # Create lineage record
            await self.db.data_lineage.insert_one({
                "staging_id": staging_id,
                "staging_collection": staging_collection,
                "production_id": production_id,
                "production_collection": production_collection,
                "source_id": staging_doc['_source_id'],
                "crawl_result_id": staging_doc.get('_crawl_result_id'),
                "reviewer_id": reviewer_id,
                "has_corrections": bool(corrections),
                "corrections": corrections or [],
                "moved_at": now
            })

            return True, production_id, "Successfully promoted"

        except Exception as e:
            logger.error(f"Promotion failed: {e}")
            return False, None, str(e)

    async def bulk_reject(
        self,
        request: BulkRejectRequest,
        reviewer_id: str
    ) -> BulkOperationResult:
        """
        Bulk reject multiple review records.

        Marks specified review IDs as rejected with the provided reason.

        Args:
            request: Bulk reject request with review IDs and reason
            reviewer_id: ID of the reviewer performing the operation

        Returns:
            BulkOperationResult with success/failure counts
        """
        result = BulkOperationResult(
            total=len(request.review_ids),
            success=0,
            failed=0,
            failed_ids=[],
            errors=[]
        )

        # Validate all ObjectIds first
        valid_ids = []
        for rid in request.review_ids:
            oid = validate_object_id(rid, "bulk_reject")
            if oid:
                valid_ids.append((rid, oid))
            else:
                result.failed += 1
                result.failed_ids.append(rid)
                result.errors.append(f"Invalid ObjectId: {rid}")

        if not valid_ids:
            return result

        now = datetime.utcnow()
        oid_list = [oid for _, oid in valid_ids]

        # Fetch all pending reviews
        reviews = await self.db.data_reviews.find(
            {"_id": {"$in": oid_list}, "review_status": "pending"}
        ).to_list(None)

        found_ids = {str(r["_id"]) for r in reviews}

        # Mark not found as failed
        for str_id, oid in valid_ids:
            if str_id not in found_ids:
                result.failed += 1
                result.failed_ids.append(str_id)
                result.errors.append(f"Review not found or not pending: {str_id}")

        if not reviews:
            return result

        # Update all reviews to rejected
        update_data = {
            "review_status": "rejected",
            "reviewer_id": reviewer_id,
            "reviewed_at": now,
            "updated_at": now,
            "rejection_reason": request.reason
        }
        if request.comment:
            update_data["notes"] = request.comment

        try:
            update_result = await self.db.data_reviews.update_many(
                {"_id": {"$in": [r["_id"] for r in reviews]}},
                {"$set": update_data}
            )

            result.success = update_result.modified_count

            # Update staging records if present
            for review in reviews:
                staging_id = review.get("staging_id")
                if staging_id:
                    # Mark staging as rejected across collections
                    for _, (stg, _) in COLLECTION_MAPPING.items():
                        await self.db[stg].update_one(
                            {"_id": ObjectId(staging_id) if isinstance(staging_id, str) else staging_id},
                            {"$set": {
                                "_review_status": "rejected",
                                "_rejection_reason": request.reason,
                                "_rejected_at": now,
                                "_rejected_by": reviewer_id
                            }}
                        )

        except Exception as e:
            logger.exception(f"Bulk reject error: {e}")
            result.failed = len(reviews)
            result.errors.append(f"Database error: {str(e)}")

        logger.info(
            f"Bulk reject completed: {result.success}/{result.total} success, "
            f"{result.failed} failed"
        )

        return result

    async def bulk_approve_by_filter(
        self,
        request: BulkFilterRequest,
        reviewer_id: str
    ) -> BulkOperationResult:
        """
        Bulk approve records matching filter criteria.

        Finds and approves all pending reviews matching the specified filters.

        Args:
            request: Filter criteria for selecting records
            reviewer_id: ID of the reviewer performing the operation

        Returns:
            BulkOperationResult with success/failure counts
        """
        # Build filter query
        query: Dict[str, Any] = {"review_status": "pending"}

        if request.source_id:
            source_oid = validate_object_id(request.source_id, "filter_source_id")
            if source_oid:
                query["source_id"] = source_oid
            else:
                return BulkOperationResult(
                    total=0, success=0, failed=0,
                    failed_ids=[], errors=["Invalid source_id"]
                )

        if request.confidence_min is not None:
            query["confidence_score"] = {"$gte": request.confidence_min}

        if request.date_from or request.date_to:
            date_query = {}
            if request.date_from:
                date_query["$gte"] = request.date_from
            if request.date_to:
                date_query["$lte"] = request.date_to
            query["created_at"] = date_query

        # Find matching reviews
        cursor = self.db.data_reviews.find(query).limit(request.limit)
        reviews = await cursor.to_list(request.limit)

        if not reviews:
            return BulkOperationResult(
                total=0, success=0, failed=0,
                failed_ids=[], errors=[]
            )

        # Convert to BulkApproveRequest format
        review_ids = [str(r["_id"]) for r in reviews]
        approve_request = BulkApproveRequest(
            review_ids=review_ids[:100],  # Process in chunks due to max limit
            comment=request.comment
        )

        # Process in multiple batches if needed
        result = BulkOperationResult(
            total=len(review_ids),
            success=0,
            failed=0,
            failed_ids=[],
            errors=[]
        )

        for i in range(0, len(review_ids), 100):
            batch_ids = review_ids[i:i + 100]
            batch_request = BulkApproveRequest(
                review_ids=batch_ids,
                comment=request.comment
            )

            batch_result = await self.bulk_approve(batch_request, reviewer_id)
            result.success += batch_result.success
            result.failed += batch_result.failed
            result.failed_ids.extend(batch_result.failed_ids)
            result.errors.extend(batch_result.errors)

        logger.info(
            f"Bulk approve by filter completed: {result.success}/{result.total} success"
        )

        return result

    async def create_bulk_job(
        self,
        operation: str,
        total: int,
        reviewer_id: str
    ) -> str:
        """
        Create a bulk job record for async tracking.

        Args:
            operation: Type of operation (approve, reject, filter_approve)
            total: Total records to process
            reviewer_id: Reviewer ID

        Returns:
            Job ID string
        """
        job_id = f"bulk_{operation}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        job_status = BulkJobStatus(
            job_id=job_id,
            status="pending",
            operation=operation,
            total=total,
            processed=0,
            success=0,
            failed=0,
            started_at=datetime.utcnow(),
            completed_at=None,
            error_message=None,
            result=None
        )

        # Store in memory (replace with persistent storage for production)
        _bulk_jobs[job_id] = job_status

        # Also store in MongoDB for persistence
        await self.db.bulk_jobs.insert_one({
            "job_id": job_id,
            "status": "pending",
            "operation": operation,
            "total": total,
            "processed": 0,
            "success": 0,
            "failed": 0,
            "reviewer_id": reviewer_id,
            "started_at": job_status.started_at,
            "completed_at": None,
            "error_message": None,
            "result": None
        })

        return job_id

    async def get_bulk_job_status(self, job_id: str) -> Optional[BulkJobStatus]:
        """
        Get status of a bulk job.

        Args:
            job_id: Job ID to look up

        Returns:
            BulkJobStatus if found, None otherwise
        """
        # Check memory first
        if job_id in _bulk_jobs:
            return _bulk_jobs[job_id]

        # Check MongoDB
        job_doc = await self.db.bulk_jobs.find_one({"job_id": job_id})
        if job_doc:
            result_data = job_doc.get("result")
            result = BulkOperationResult(**result_data) if result_data else None

            return BulkJobStatus(
                job_id=job_doc["job_id"],
                status=job_doc["status"],
                operation=job_doc["operation"],
                total=job_doc["total"],
                processed=job_doc["processed"],
                success=job_doc["success"],
                failed=job_doc["failed"],
                started_at=job_doc["started_at"],
                completed_at=job_doc.get("completed_at"),
                error_message=job_doc.get("error_message"),
                result=result
            )

        return None

    async def update_bulk_job(
        self,
        job_id: str,
        processed: int,
        success: int,
        failed: int,
        status: str = "processing",
        error_message: Optional[str] = None,
        result: Optional[BulkOperationResult] = None
    ) -> None:
        """
        Update bulk job progress.

        Args:
            job_id: Job ID to update
            processed: Number of records processed
            success: Number of successes
            failed: Number of failures
            status: Current job status
            error_message: Error message if failed
            result: Final result if completed
        """
        update_data = {
            "processed": processed,
            "success": success,
            "failed": failed,
            "status": status
        }

        if status in ["completed", "failed"]:
            update_data["completed_at"] = datetime.utcnow()

        if error_message:
            update_data["error_message"] = error_message

        if result:
            update_data["result"] = result.model_dump()

        # Update memory
        if job_id in _bulk_jobs:
            job = _bulk_jobs[job_id]
            job.processed = processed
            job.success = success
            job.failed = failed
            job.status = status
            if status in ["completed", "failed"]:
                job.completed_at = datetime.utcnow()
            if error_message:
                job.error_message = error_message
            if result:
                job.result = result

        # Update MongoDB
        await self.db.bulk_jobs.update_one(
            {"job_id": job_id},
            {"$set": update_data}
        )

    async def get_review_count_by_filter(
        self,
        request: BulkFilterRequest
    ) -> int:
        """
        Get count of reviews matching filter criteria.

        Args:
            request: Filter criteria

        Returns:
            Count of matching reviews
        """
        query: Dict[str, Any] = {"review_status": "pending"}

        if request.source_id:
            source_oid = validate_object_id(request.source_id, "filter_source_id")
            if source_oid:
                query["source_id"] = source_oid

        if request.confidence_min is not None:
            query["confidence_score"] = {"$gte": request.confidence_min}

        if request.date_from or request.date_to:
            date_query = {}
            if request.date_from:
                date_query["$gte"] = request.date_from
            if request.date_to:
                date_query["$lte"] = request.date_to
            query["created_at"] = date_query

        return await self.db.data_reviews.count_documents(query)
