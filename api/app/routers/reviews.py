"""
Review API endpoints for data verification.

This module provides endpoints for the data review/verification workflow,
allowing reviewers to approve, hold, or correct crawled data.
"""

import logging
from datetime import datetime
from typing import Optional, List
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, Depends

from ..models.schemas import (
    DataReviewResponse,
    ReviewStatusUpdate,
    ReviewQueueItem,
    ReviewSessionStats,
    ReviewDashboardResponse,
    PaginatedResponse
)
from ..services.mongo_service import get_db, MongoService
from ..services.data_promotion import DataPromotionService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/reviews", tags=["Reviews"])


def get_promotion_service(db) -> DataPromotionService:
    """Get data promotion service instance."""
    # For sync operations, we need the sync db
    mongo = MongoService()
    return DataPromotionService(mongo.db)


def serialize_doc(doc: dict) -> dict:
    """Convert MongoDB document for JSON serialization."""
    if doc is None:
        return None
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    if "crawl_result_id" in doc:
        doc["crawl_result_id"] = str(doc["crawl_result_id"])
    if "source_id" in doc:
        doc["source_id"] = str(doc["source_id"])
    return doc


@router.get("/dashboard", response_model=ReviewDashboardResponse)
async def get_review_dashboard(db=Depends(get_db)):
    """Get review dashboard statistics."""
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    # Pending count
    pending_count = await db.data_reviews.count_documents({"review_status": "pending"})

    # Today reviewed
    today_reviewed = await db.data_reviews.count_documents({
        "reviewed_at": {"$gte": today_start},
        "review_status": {"$ne": "pending"}
    })

    # Approval rate (last 7 days)
    total_reviewed = await db.data_reviews.count_documents({
        "review_status": {"$ne": "pending"}
    })
    approved = await db.data_reviews.count_documents({
        "review_status": "approved"
    })
    approval_rate = (approved / total_reviewed * 100) if total_reviewed > 0 else 0

    # Average confidence
    pipeline = [
        {"$match": {"confidence_score": {"$exists": True}}},
        {"$group": {"_id": None, "avg": {"$avg": "$confidence_score"}}}
    ]
    result = await db.data_reviews.aggregate(pipeline).to_list(1)
    avg_confidence = result[0]["avg"] if result else 0

    # Needs number review
    needs_number_review = await db.data_reviews.count_documents({
        "needs_number_review": True,
        "review_status": "pending"
    })

    # By source (top 5)
    pipeline = [
        {"$match": {"review_status": "pending"}},
        {"$group": {"_id": "$source_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    by_source_raw = await db.data_reviews.aggregate(pipeline).to_list(5)

    by_source = []
    for item in by_source_raw:
        source = await db.sources.find_one({"_id": ObjectId(item["_id"])})
        by_source.append({
            "source_id": str(item["_id"]),
            "source_name": source["name"] if source else "Unknown",
            "pending_count": item["count"]
        })

    # Recent reviews
    recent = await db.data_reviews.find(
        {"review_status": {"$ne": "pending"}}
    ).sort("reviewed_at", -1).limit(10).to_list(10)

    return ReviewDashboardResponse(
        pending_count=pending_count,
        today_reviewed=today_reviewed,
        approval_rate=approval_rate,
        avg_confidence=avg_confidence,
        needs_number_review_count=needs_number_review,
        by_source=by_source,
        recent_reviews=[serialize_doc(r) for r in recent]
    )


@router.get("/queue", response_model=List[ReviewQueueItem])
async def get_review_queue(
    source_id: Optional[str] = None,
    status: str = Query("pending", pattern="^(pending|on_hold|needs_correction)$"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    priority_numbers: bool = Query(False, description="Prioritize items needing number review"),
    db=Depends(get_db)
):
    """
    Get review queue items.

    Items are ordered by:
    1. needs_number_review (if priority_numbers is True)
    2. confidence_score (lower first)
    3. created_at (older first)
    """
    query = {"review_status": status}

    if source_id:
        query["source_id"] = ObjectId(source_id)

    # Build sort order
    sort_order = []
    if priority_numbers:
        sort_order.append(("needs_number_review", -1))
    sort_order.extend([("confidence_score", 1), ("created_at", 1)])

    total = await db.data_reviews.count_documents(query)
    reviews = await db.data_reviews.find(query).sort(sort_order).skip(offset).limit(limit).to_list(limit)

    result = []
    for i, review in enumerate(reviews):
        source = await db.sources.find_one({"_id": ObjectId(review["source_id"])})
        result.append(ReviewQueueItem(
            review=serialize_doc(review),
            source_name=source["name"] if source else "Unknown",
            source_type=source["type"] if source else "unknown",
            source_url=source["url"] if source else "",
            total_in_queue=total,
            current_position=offset + i + 1
        ))

    return result


@router.get("/next")
async def get_next_review(
    source_id: Optional[str] = None,
    current_id: Optional[str] = None,
    direction: str = Query("forward", pattern="^(forward|backward)$"),
    db=Depends(get_db)
):
    """
    Get next/previous review item for continuous review workflow.

    Used for the 'next' and 'previous' buttons in the UI.
    """
    query = {"review_status": "pending"}

    if source_id:
        query["source_id"] = ObjectId(source_id)

    if current_id:
        current = await db.data_reviews.find_one({"_id": ObjectId(current_id)})
        if current:
            if direction == "forward":
                query["created_at"] = {"$gt": current["created_at"]}
                sort_dir = 1
            else:
                query["created_at"] = {"$lt": current["created_at"]}
                sort_dir = -1

            review = await db.data_reviews.find(query).sort("created_at", sort_dir).limit(1).to_list(1)

            if not review and direction == "backward":
                # Going back to a completed one
                back_query = {
                    "created_at": {"$lt": current["created_at"]},
                    "review_status": {"$ne": "pending"}
                }
                if source_id:
                    back_query["source_id"] = ObjectId(source_id)
                review = await db.data_reviews.find(back_query).sort("created_at", -1).limit(1).to_list(1)
        else:
            review = await db.data_reviews.find(query).sort("created_at", 1).limit(1).to_list(1)
    else:
        review = await db.data_reviews.find(query).sort("created_at", 1).limit(1).to_list(1)

    if not review:
        return {"has_next": False, "review": None}

    review = review[0]
    source = await db.sources.find_one({"_id": ObjectId(review["source_id"])})

    # Get position info
    total_pending = await db.data_reviews.count_documents({"review_status": "pending"})
    position_query = {"review_status": "pending", "created_at": {"$lte": review["created_at"]}}
    if source_id:
        position_query["source_id"] = ObjectId(source_id)
    position = await db.data_reviews.count_documents(position_query)

    return {
        "has_next": True,
        "review": serialize_doc(review),
        "source": {
            "name": source["name"] if source else "Unknown",
            "type": source["type"] if source else "unknown",
            "url": source["url"] if source else ""
        },
        "position": position,
        "total_pending": total_pending
    }


@router.get("/{review_id}", response_model=DataReviewResponse)
async def get_review(review_id: str, db=Depends(get_db)):
    """Get a specific review by ID."""
    review = await db.data_reviews.find_one({"_id": ObjectId(review_id)})

    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    return serialize_doc(review)


@router.get("/{review_id}/source-content")
async def get_review_source_content(review_id: str, db=Depends(get_db)):
    """
    Get source content for review (HTML, PDF, JSON, etc.)

    Returns the original source data with highlight information.
    """
    review = await db.data_reviews.find_one({"_id": ObjectId(review_id)})
    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    source = await db.sources.find_one({"_id": ObjectId(review["source_id"])})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    crawl_result = await db.crawl_results.find_one({"_id": ObjectId(review["crawl_result_id"])})

    return {
        "source_type": source["type"],
        "source_url": source["url"],
        "source_name": source["name"],
        "fields": source.get("fields", []),
        "highlights": review.get("source_highlights", []),
        "html_snapshot": crawl_result.get("html_snapshot") if crawl_result else None,
        "raw_data": crawl_result.get("data") if crawl_result else None
    }


@router.put("/{review_id}", response_model=DataReviewResponse)
async def update_review(
    review_id: str,
    update: ReviewStatusUpdate,
    reviewer_id: str = Query(..., description="Reviewer identifier"),
    db=Depends(get_db)
):
    """
    Update review status.

    Actions:
    - approved: Data is correct → promote staging to production
    - on_hold: Needs more investigation, skip for now
    - needs_correction: Data has errors, awaiting correction
    - corrected: Corrections have been made → promote with corrections
    """
    review = await db.data_reviews.find_one({"_id": ObjectId(review_id)})
    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    update_data = {
        "review_status": update.status,
        "reviewer_id": reviewer_id,
        "reviewed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }

    if update.notes:
        update_data["notes"] = update.notes

    if update.review_duration_ms:
        update_data["review_duration_ms"] = update.review_duration_ms

    corrections_list = None

    # Handle corrections
    if update.status in ["needs_correction", "corrected"] and update.corrections:
        update_data["corrections"] = [c.model_dump() for c in update.corrections]
        corrections_list = update_data["corrections"]

        # If corrected, create corrected_data from original + corrections
        if update.status == "corrected":
            corrected_data = review.get("original_data", {}).copy()
            for correction in update.corrections:
                if correction.field in corrected_data:
                    corrected_data[correction.field] = correction.corrected_value
            update_data["corrected_data"] = corrected_data

    await db.data_reviews.update_one(
        {"_id": ObjectId(review_id)},
        {"$set": update_data}
    )

    # Promote to production on approval or correction
    if update.status in ["approved", "corrected"]:
        staging_id = review.get("staging_id")

        if staging_id:
            # Use promotion service to move staging → production
            promotion_service = get_promotion_service(db)
            success, production_id, message = promotion_service.promote_to_production(
                staging_id=ObjectId(staging_id),
                reviewer_id=reviewer_id,
                corrections=corrections_list
            )

            if success:
                # Update review with production reference
                await db.data_reviews.update_one(
                    {"_id": ObjectId(review_id)},
                    {"$set": {
                        "production_id": production_id,
                        "promoted_at": datetime.utcnow()
                    }}
                )
                logger.info(f"Promoted staging/{staging_id} to production/{production_id}")
            else:
                logger.warning(f"Promotion failed for review {review_id}: {message}")
        else:
            # Legacy path: direct update to crawl_data (for existing data without staging)
            if update.status == "corrected" and "corrected_data" in update_data:
                await db.crawl_data.update_one(
                    {
                        "_crawl_result_id": ObjectId(review["crawl_result_id"]),
                        "_record_index": review.get("data_record_index", 0)
                    },
                    {
                        "$set": {
                            **update_data["corrected_data"],
                            "_verified": True,
                            "_verified_at": datetime.utcnow(),
                            "_verified_by": reviewer_id
                        }
                    }
                )

    updated = await db.data_reviews.find_one({"_id": ObjectId(review_id)})
    return serialize_doc(updated)


@router.post("/batch-approve")
async def batch_approve(
    review_ids: List[str],
    reviewer_id: str = Query(..., description="Reviewer identifier"),
    db=Depends(get_db)
):
    """Batch approve multiple reviews."""
    object_ids = [ObjectId(rid) for rid in review_ids]

    result = await db.data_reviews.update_many(
        {"_id": {"$in": object_ids}},
        {
            "$set": {
                "review_status": "approved",
                "reviewer_id": reviewer_id,
                "reviewed_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
        }
    )

    return {
        "success": True,
        "modified_count": result.modified_count
    }


@router.get("/stats/by-source/{source_id}")
async def get_source_review_stats(source_id: str, db=Depends(get_db)):
    """Get review statistics for a specific source."""
    pipeline = [
        {"$match": {"source_id": ObjectId(source_id)}},
        {"$group": {
            "_id": "$review_status",
            "count": {"$sum": 1}
        }}
    ]

    results = await db.data_reviews.aggregate(pipeline).to_list(None)

    stats = {
        "pending": 0,
        "approved": 0,
        "on_hold": 0,
        "needs_correction": 0,
        "corrected": 0,
        "total": 0
    }

    for r in results:
        stats[r["_id"]] = r["count"]
        stats["total"] += r["count"]

    return stats


@router.post("/create-from-crawl-result/{crawl_result_id}")
async def create_reviews_from_crawl_result(
    crawl_result_id: str,
    db=Depends(get_db)
):
    """
    Create review records from a crawl result.

    This should be called after a crawl completes to queue
    the data for review.
    """
    crawl_result = await db.crawl_results.find_one({"_id": ObjectId(crawl_result_id)})
    if not crawl_result:
        raise HTTPException(status_code=404, detail="Crawl result not found")

    data = crawl_result.get("data", [])
    if isinstance(data, dict):
        data = [data]

    created_count = 0
    for i, record in enumerate(data):
        # Check if review already exists
        existing = await db.data_reviews.find_one({
            "crawl_result_id": ObjectId(crawl_result_id),
            "data_record_index": i
        })

        if existing:
            continue

        # Extract confidence info if available
        confidence = record.get("confidence", record.get("_confidence"))
        ocr_conf = record.get("ocr_confidence", record.get("_ocr_confidence"))
        ai_conf = record.get("ai_confidence", record.get("_ai_confidence"))
        needs_review = record.get("needs_number_review", False)
        uncertain = record.get("uncertain_numbers", [])

        # Create review record
        review_doc = {
            "crawl_result_id": ObjectId(crawl_result_id),
            "source_id": crawl_result["source_id"],
            "data_record_index": i,
            "review_status": "pending",
            "original_data": record,
            "confidence_score": confidence,
            "ocr_confidence": ocr_conf,
            "ai_confidence": ai_conf,
            "needs_number_review": needs_review,
            "uncertain_numbers": uncertain,
            "source_highlights": record.get("_highlights", []),
            "created_at": datetime.utcnow()
        }

        await db.data_reviews.insert_one(review_doc)
        created_count += 1

    return {
        "success": True,
        "created_count": created_count,
        "total_records": len(data)
    }
