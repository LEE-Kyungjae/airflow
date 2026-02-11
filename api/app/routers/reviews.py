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
    PaginatedResponse,
    BulkApproveRequest,
    BulkRejectRequest,
    BulkFilterRequest,
    BulkOperationResult,
    BulkJobStatus
)
from ..services.mongo_service import get_db, MongoService
from ..services.data_promotion import DataPromotionService
from ..services.review_service import ReviewService
from app.auth.dependencies import require_auth, require_scope, require_admin, AuthContext

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/reviews", tags=["Reviews"])


def get_promotion_service(db) -> DataPromotionService:
    """Get data promotion service instance."""
    # For sync operations, we need the sync db
    mongo = MongoService()
    return DataPromotionService(mongo.db)


def get_review_service(db) -> ReviewService:
    """Get review service instance for bulk operations."""
    return ReviewService(db)


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
async def get_review_dashboard(db=Depends(get_db), auth: AuthContext = Depends(require_auth)):
    """
    Get review dashboard statistics.

    N+1 최적화:
    1. $facet을 사용하여 여러 count 쿼리를 단일 aggregation으로 병합
    2. by_source 조회 시 $lookup으로 소스 이름을 조인 (개별 find_one 제거)
    3. recent_reviews 조회 시 $lookup으로 소스 정보 포함

    - 기존: 6개 count 쿼리 + N개 소스 조회 (N+1 패턴)
    - 최적화 후: 2개 aggregation 쿼리
    """
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    # 최적화: $facet으로 여러 통계를 단일 쿼리로 계산
    stats_pipeline = [
        {"$facet": {
            "pending": [
                {"$match": {"review_status": "pending"}},
                {"$count": "count"}
            ],
            "today_reviewed": [
                {"$match": {
                    "reviewed_at": {"$gte": today_start},
                    "review_status": {"$ne": "pending"}
                }},
                {"$count": "count"}
            ],
            "total_reviewed": [
                {"$match": {"review_status": {"$ne": "pending"}}},
                {"$count": "count"}
            ],
            "approved": [
                {"$match": {"review_status": "approved"}},
                {"$count": "count"}
            ],
            "needs_number_review": [
                {"$match": {
                    "needs_number_review": True,
                    "review_status": "pending"
                }},
                {"$count": "count"}
            ],
            "avg_confidence": [
                {"$match": {"confidence_score": {"$exists": True}}},
                {"$group": {"_id": None, "avg": {"$avg": "$confidence_score"}}}
            ],
            # N+1 최적화: by_source에 $lookup으로 소스 이름 조인
            "by_source": [
                {"$match": {"review_status": "pending"}},
                {"$group": {"_id": "$source_id", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5},
                {"$lookup": {
                    "from": "sources",
                    "localField": "_id",
                    "foreignField": "_id",
                    "pipeline": [{"$project": {"name": 1}}],
                    "as": "source"
                }},
                {"$addFields": {
                    "source_name": {"$ifNull": [{"$arrayElemAt": ["$source.name", 0]}, "Unknown"]}
                }},
                {"$project": {"source": 0}}
            ]
        }}
    ]

    stats_result = await db.data_reviews.aggregate(stats_pipeline).to_list(1)
    stats = stats_result[0] if stats_result else {}

    # 결과 파싱
    pending_count = stats.get("pending", [{}])[0].get("count", 0) if stats.get("pending") else 0
    today_reviewed = stats.get("today_reviewed", [{}])[0].get("count", 0) if stats.get("today_reviewed") else 0
    total_reviewed = stats.get("total_reviewed", [{}])[0].get("count", 0) if stats.get("total_reviewed") else 0
    approved = stats.get("approved", [{}])[0].get("count", 0) if stats.get("approved") else 0
    needs_number_review = stats.get("needs_number_review", [{}])[0].get("count", 0) if stats.get("needs_number_review") else 0
    avg_confidence_result = stats.get("avg_confidence", [])
    avg_confidence = avg_confidence_result[0]["avg"] if avg_confidence_result else 0

    approval_rate = (approved / total_reviewed * 100) if total_reviewed > 0 else 0

    # by_source 결과 변환
    by_source = [
        {
            "source_id": str(item["_id"]),
            "source_name": item["source_name"],
            "pending_count": item["count"]
        }
        for item in stats.get("by_source", [])
    ]

    # Recent reviews (별도 쿼리 - $facet 내 중첩 제한)
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
    db=Depends(get_db),
    auth: AuthContext = Depends(require_auth)
):
    """
    Get review queue items.

    Items are ordered by:
    1. needs_number_review (if priority_numbers is True)
    2. confidence_score (lower first)
    3. created_at (older first)

    N+1 최적화: 리뷰 목록 조회 후 각 리뷰에 대해 개별 소스 조회하는 대신
    $lookup을 사용하여 단일 aggregation으로 소스 정보를 조인

    - 기존: 1개 find + N개 소스 조회 (N+1 패턴)
    - 최적화 후: 1개 aggregation (소스 정보 $lookup 포함)
    """
    match_stage = {"review_status": status}
    if source_id:
        match_stage["source_id"] = ObjectId(source_id)

    # Build sort order for aggregation
    sort_stage = {}
    if priority_numbers:
        sort_stage["needs_number_review"] = -1
    sort_stage["confidence_score"] = 1
    sort_stage["created_at"] = 1

    # 최적화: $lookup으로 소스 정보 조인
    pipeline = [
        {"$match": match_stage},
        {"$sort": sort_stage},
        {"$skip": offset},
        {"$limit": limit},
        # N+1 최적화: 소스 정보를 $lookup으로 조인
        {"$lookup": {
            "from": "sources",
            "localField": "source_id",
            "foreignField": "_id",
            "pipeline": [
                {"$project": {"name": 1, "type": 1, "url": 1}}
            ],
            "as": "source_info"
        }},
        {"$addFields": {
            "source_info": {"$arrayElemAt": ["$source_info", 0]}
        }}
    ]

    # 총 개수 (별도 쿼리 - aggregation에서는 $count 사용 시 pagination 불가)
    total = await db.data_reviews.count_documents(match_stage)
    reviews = await db.data_reviews.aggregate(pipeline).to_list(limit)

    result = []
    for i, review in enumerate(reviews):
        source_info = review.pop("source_info", None) or {}
        result.append(ReviewQueueItem(
            review=serialize_doc(review),
            source_name=source_info.get("name", "Unknown"),
            source_type=source_info.get("type", "unknown"),
            source_url=source_info.get("url", ""),
            total_in_queue=total,
            current_position=offset + i + 1
        ))

    return result


@router.get("/next")
async def get_next_review(
    source_id: Optional[str] = None,
    current_id: Optional[str] = None,
    direction: str = Query("forward", pattern="^(forward|backward)$"),
    db=Depends(get_db),
    auth: AuthContext = Depends(require_auth)
):
    """
    Get next/previous review item for continuous review workflow.

    Used for the 'next' and 'previous' buttons in the UI.

    N+1 최적화: 리뷰 조회 후 소스 정보를 별도 조회하는 대신
    $lookup을 사용하여 단일 aggregation으로 처리

    - 기존: 1개 find + 1개 소스 조회 + 2개 count 쿼리
    - 최적화 후: 1개 aggregation ($lookup + $facet으로 position 계산)
    """
    # Auto-resume: if no current_id, look up reviewer's last bookmark
    if not current_id:
        reviewer_id = auth.user_id or "anonymous"
        bookmark = await db.reviewer_bookmarks.find_one({"reviewer_id": reviewer_id})
        if bookmark:
            current_id = bookmark.get("last_review_id")

    match_stage = {"review_status": "pending"}

    if source_id:
        match_stage["source_id"] = ObjectId(source_id)

    # 기본 pipeline 구성
    if current_id:
        current = await db.data_reviews.find_one({"_id": ObjectId(current_id)})
        if current:
            if direction == "forward":
                match_stage["created_at"] = {"$gt": current["created_at"]}
                sort_dir = 1
            else:
                match_stage["created_at"] = {"$lt": current["created_at"]}
                sort_dir = -1

            # 최적화: 리뷰 + 소스 정보를 단일 aggregation으로 조회
            pipeline = [
                {"$match": match_stage},
                {"$sort": {"created_at": sort_dir}},
                {"$limit": 1},
                {"$lookup": {
                    "from": "sources",
                    "localField": "source_id",
                    "foreignField": "_id",
                    "pipeline": [{"$project": {"name": 1, "type": 1, "url": 1}}],
                    "as": "source_info"
                }},
                {"$addFields": {
                    "source_info": {"$arrayElemAt": ["$source_info", 0]}
                }}
            ]

            review = await db.data_reviews.aggregate(pipeline).to_list(1)

            if not review and direction == "backward":
                # Going back to a completed one
                back_match = {
                    "created_at": {"$lt": current["created_at"]},
                    "review_status": {"$ne": "pending"}
                }
                if source_id:
                    back_match["source_id"] = ObjectId(source_id)

                back_pipeline = [
                    {"$match": back_match},
                    {"$sort": {"created_at": -1}},
                    {"$limit": 1},
                    {"$lookup": {
                        "from": "sources",
                        "localField": "source_id",
                        "foreignField": "_id",
                        "pipeline": [{"$project": {"name": 1, "type": 1, "url": 1}}],
                        "as": "source_info"
                    }},
                    {"$addFields": {
                        "source_info": {"$arrayElemAt": ["$source_info", 0]}
                    }}
                ]
                review = await db.data_reviews.aggregate(back_pipeline).to_list(1)
        else:
            pipeline = [
                {"$match": match_stage},
                {"$sort": {"created_at": 1}},
                {"$limit": 1},
                {"$lookup": {
                    "from": "sources",
                    "localField": "source_id",
                    "foreignField": "_id",
                    "pipeline": [{"$project": {"name": 1, "type": 1, "url": 1}}],
                    "as": "source_info"
                }},
                {"$addFields": {
                    "source_info": {"$arrayElemAt": ["$source_info", 0]}
                }}
            ]
            review = await db.data_reviews.aggregate(pipeline).to_list(1)
    else:
        pipeline = [
            {"$match": match_stage},
            {"$sort": {"created_at": 1}},
            {"$limit": 1},
            {"$lookup": {
                "from": "sources",
                "localField": "source_id",
                "foreignField": "_id",
                "pipeline": [{"$project": {"name": 1, "type": 1, "url": 1}}],
                "as": "source_info"
            }},
            {"$addFields": {
                "source_info": {"$arrayElemAt": ["$source_info", 0]}
            }}
        ]
        review = await db.data_reviews.aggregate(pipeline).to_list(1)

    if not review:
        return {"has_next": False, "review": None}

    review = review[0]
    source_info = review.pop("source_info", None) or {}

    # Get position info (count 쿼리는 유지 - 간단한 쿼리)
    total_pending = await db.data_reviews.count_documents({"review_status": "pending"})
    position_query = {"review_status": "pending", "created_at": {"$lte": review["created_at"]}}
    if source_id:
        position_query["source_id"] = ObjectId(source_id)
    position = await db.data_reviews.count_documents(position_query)

    return {
        "has_next": True,
        "review": serialize_doc(review),
        "source": {
            "name": source_info.get("name", "Unknown"),
            "type": source_info.get("type", "unknown"),
            "url": source_info.get("url", "")
        },
        "position": position,
        "total_pending": total_pending
    }


@router.get("/resume")
async def get_resume_info(
    db=Depends(get_db),
    auth: AuthContext = Depends(require_auth)
):
    """
    Get reviewer's last review position for session resume.

    Returns the bookmark info so the UI can offer "Continue from where you left off".
    """
    reviewer_id = auth.user_id or "anonymous"
    bookmark = await db.reviewer_bookmarks.find_one({"reviewer_id": reviewer_id})

    if not bookmark:
        # No bookmark - count total pending
        total_pending = await db.data_reviews.count_documents({"review_status": "pending"})
        return {
            "has_bookmark": False,
            "total_pending": total_pending,
            "message": "No previous session found. Start from the beginning."
        }

    last_review = await db.data_reviews.find_one({"_id": ObjectId(bookmark["last_review_id"])})

    # Count remaining pending after the bookmarked review
    remaining_query = {"review_status": "pending"}
    if last_review:
        remaining_query["created_at"] = {"$gt": last_review["created_at"]}
    remaining = await db.data_reviews.count_documents(remaining_query)
    total_pending = await db.data_reviews.count_documents({"review_status": "pending"})

    return {
        "has_bookmark": True,
        "last_review_id": bookmark["last_review_id"],
        "last_reviewed_at": bookmark["last_reviewed_at"].isoformat() if bookmark.get("last_reviewed_at") else None,
        "remaining_after_bookmark": remaining,
        "total_pending": total_pending
    }


@router.get("/{review_id}", response_model=DataReviewResponse)
async def get_review(review_id: str, db=Depends(get_db), auth: AuthContext = Depends(require_auth)):
    """Get a specific review by ID."""
    review = await db.data_reviews.find_one({"_id": ObjectId(review_id)})

    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    return serialize_doc(review)


@router.get("/{review_id}/source-content")
async def get_review_source_content(review_id: str, db=Depends(get_db), auth: AuthContext = Depends(require_auth)):
    """
    Get source content for review (HTML, PDF, JSON, etc.)

    Returns the original source data with highlight information.

    N+1 최적화: 3개의 순차적 find_one 쿼리를 단일 aggregation으로 병합
    $lookup을 사용하여 review -> source -> crawl_result 조인

    - 기존: 3개 순차 쿼리 (review, source, crawl_result)
    - 최적화 후: 1개 aggregation ($lookup 2회)
    """
    # 최적화: 단일 aggregation으로 모든 관련 데이터 조회
    pipeline = [
        {"$match": {"_id": ObjectId(review_id)}},
        # 소스 정보 조인
        {"$lookup": {
            "from": "sources",
            "localField": "source_id",
            "foreignField": "_id",
            "pipeline": [
                {"$project": {"name": 1, "type": 1, "url": 1, "fields": 1}}
            ],
            "as": "source"
        }},
        # 크롤 결과 조인
        {"$lookup": {
            "from": "crawl_results",
            "localField": "crawl_result_id",
            "foreignField": "_id",
            "pipeline": [
                {"$project": {"html_snapshot": 1, "data": 1}}
            ],
            "as": "crawl_result"
        }},
        {"$addFields": {
            "source": {"$arrayElemAt": ["$source", 0]},
            "crawl_result": {"$arrayElemAt": ["$crawl_result", 0]}
        }}
    ]

    result = await db.data_reviews.aggregate(pipeline).to_list(1)

    if not result:
        raise HTTPException(status_code=404, detail="Review not found")

    review = result[0]
    source = review.get("source")
    crawl_result = review.get("crawl_result")

    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

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
    db=Depends(get_db),
    auth: AuthContext = Depends(require_scope("write"))
):
    """
    Update review status.

    Actions:
    - approved: Data is correct → promote staging to production
    - on_hold: Needs more investigation, skip for now
    - needs_correction: Data has errors, awaiting correction
    - corrected: Corrections have been made → promote with corrections
    """
    reviewer_id = auth.user_id or "anonymous"
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

    # Handle rejection
    if update.status == "rejected":
        if update.rejection_reason:
            update_data["rejection_reason"] = update.rejection_reason
        if update.rejection_notes:
            update_data["rejection_notes"] = update.rejection_notes

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

    # Handle rejection post-processing
    if update.status == "rejected" and update.rejection_reason:
        from app.services.review_actions import handle_rejection
        try:
            await handle_rejection(
                db=db,
                review=review,
                reason=update.rejection_reason,
                notes=update.rejection_notes
            )
        except Exception as e:
            logger.warning(f"Rejection post-processing failed for review {review_id}: {e}")

    # Save reviewer bookmark for resume functionality
    await db.reviewer_bookmarks.update_one(
        {"reviewer_id": reviewer_id},
        {"$set": {
            "reviewer_id": reviewer_id,
            "last_review_id": review_id,
            "last_source_id": str(review.get("source_id", "")),
            "last_reviewed_at": datetime.utcnow()
        }},
        upsert=True
    )

    updated = await db.data_reviews.find_one({"_id": ObjectId(review_id)})
    return serialize_doc(updated)


@router.put("/{review_id}/revert")
async def revert_review(
    review_id: str,
    db=Depends(get_db),
    auth: AuthContext = Depends(require_scope("write"))
):
    """
    Revert a review back to pending status.

    If the review was approved and data was promoted to production,
    this will also rollback the promotion.
    """
    reviewer_id = auth.user_id or "anonymous"
    review = await db.data_reviews.find_one({"_id": ObjectId(review_id)})

    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    if review["review_status"] == "pending":
        raise HTTPException(status_code=400, detail="Review is already pending")

    previous_status = review["review_status"]

    # If it was approved/corrected and promoted, rollback production
    if previous_status in ["approved", "corrected"] and review.get("production_id"):
        try:
            promotion_service = get_promotion_service(db)
            promotion_service.rollback_promotion(
                production_id=ObjectId(review["production_id"]),
                reason=f"Review reverted by {reviewer_id}"
            )
            logger.info(f"Rolled back production/{review['production_id']} for review {review_id}")
        except Exception as e:
            logger.warning(f"Rollback failed for review {review_id}: {e}")

    # Revert to pending
    await db.data_reviews.update_one(
        {"_id": ObjectId(review_id)},
        {"$set": {
            "review_status": "pending",
            "reviewer_id": None,
            "reviewed_at": None,
            "production_id": None,
            "promoted_at": None,
            "updated_at": datetime.utcnow()
        }, "$push": {
            "revert_history": {
                "previous_status": previous_status,
                "reverted_by": reviewer_id,
                "reverted_at": datetime.utcnow()
            }
        }}
    )

    # Record in audit log
    await db.data_lineage.insert_one({
        "action": "review_reverted",
        "review_id": review_id,
        "previous_status": previous_status,
        "reverted_by": reviewer_id,
        "timestamp": datetime.utcnow()
    })

    updated = await db.data_reviews.find_one({"_id": ObjectId(review_id)})
    return serialize_doc(updated)


@router.post("/batch-approve")
async def batch_approve(
    review_ids: List[str],
    db=Depends(get_db),
    auth: AuthContext = Depends(require_scope("write"))
):
    """Batch approve multiple reviews."""
    reviewer_id = auth.user_id or "anonymous"
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


@router.get("/stats/trends")
async def get_review_trends(
    days: int = Query(7, ge=1, le=90, description="Number of days to show"),
    db=Depends(get_db),
    auth: AuthContext = Depends(require_auth)
):
    """
    Get review trends over time for charting.

    Returns daily counts of approved, corrected, rejected, and on_hold reviews.
    """
    from datetime import timedelta

    start_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days - 1)

    pipeline = [
        {"$match": {
            "reviewed_at": {"$gte": start_date},
            "review_status": {"$ne": "pending"}
        }},
        {"$group": {
            "_id": {
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$reviewed_at"}},
                "status": "$review_status"
            },
            "count": {"$sum": 1}
        }},
        {"$group": {
            "_id": "$_id.date",
            "statuses": {
                "$push": {"status": "$_id.status", "count": "$count"}
            },
            "total": {"$sum": "$count"}
        }},
        {"$sort": {"_id": 1}}
    ]

    results = await db.data_reviews.aggregate(pipeline).to_list(None)

    # Build complete date range with defaults
    trend_data = []
    for i in range(days):
        date = start_date + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")

        # Find matching result
        day_data = next((r for r in results if r["_id"] == date_str), None)

        entry = {
            "date": date_str,
            "approved": 0,
            "corrected": 0,
            "rejected": 0,
            "on_hold": 0,
            "total": 0,
        }

        if day_data:
            entry["total"] = day_data["total"]
            for s in day_data["statuses"]:
                if s["status"] in entry:
                    entry[s["status"]] = s["count"]

        trend_data.append(entry)

    return trend_data


@router.get("/stats/by-source/{source_id}")
async def get_source_review_stats(source_id: str, db=Depends(get_db), auth: AuthContext = Depends(require_auth)):
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
    db=Depends(get_db),
    auth: AuthContext = Depends(require_scope("write"))
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


# ============================================================
# Bulk Review Operations
# ============================================================

@router.post("/bulk-approve", response_model=BulkOperationResult)
async def bulk_approve_reviews(
    request: BulkApproveRequest,
    db=Depends(get_db),
    auth: AuthContext = Depends(require_scope("write"))
):
    """
    Bulk approve multiple review records.

    Approves up to 100 review records at once and promotes their
    data from staging to production. Each record must be in 'pending'
    status to be approved.

    Returns:
        BulkOperationResult with success/failure counts and error details

    Raises:
        HTTPException: If no valid review IDs provided
    """
    reviewer_id = auth.user_id or "anonymous"
    if not request.review_ids:
        raise HTTPException(
            status_code=400,
            detail="At least one review ID is required"
        )

    review_service = get_review_service(db)
    result = await review_service.bulk_approve(request, reviewer_id)

    logger.info(
        f"Bulk approve by {reviewer_id}: "
        f"{result.success}/{result.total} approved"
    )

    return result


@router.post("/bulk-reject", response_model=BulkOperationResult)
async def bulk_reject_reviews(
    request: BulkRejectRequest,
    db=Depends(get_db),
    auth: AuthContext = Depends(require_scope("write"))
):
    """
    Bulk reject multiple review records.

    Rejects up to 100 review records at once with a required reason.
    Each record must be in 'pending' status to be rejected.

    Returns:
        BulkOperationResult with success/failure counts and error details

    Raises:
        HTTPException: If no valid review IDs provided
    """
    reviewer_id = auth.user_id or "anonymous"
    if not request.review_ids:
        raise HTTPException(
            status_code=400,
            detail="At least one review ID is required"
        )

    review_service = get_review_service(db)
    result = await review_service.bulk_reject(request, reviewer_id)

    logger.info(
        f"Bulk reject by {reviewer_id}: "
        f"{result.success}/{result.total} rejected, reason: {request.reason}"
    )

    return result


@router.post("/bulk-approve-by-filter", response_model=BulkOperationResult)
async def bulk_approve_by_filter(
    request: BulkFilterRequest,
    db=Depends(get_db),
    auth: AuthContext = Depends(require_scope("write"))
):
    """
    Bulk approve reviews matching filter criteria.

    Finds all pending reviews matching the specified filters (source_id,
    confidence threshold, date range) and approves them in batches.
    Maximum 1000 records can be processed in a single request.

    Returns:
        BulkOperationResult with success/failure counts and error details

    Example filters:
        - source_id: Filter by specific source
        - confidence_min: Only approve records with confidence >= threshold
        - date_from/date_to: Filter by creation date range
        - limit: Maximum number of records to process (default 100, max 1000)
    """
    reviewer_id = auth.user_id or "anonymous"
    review_service = get_review_service(db)

    # Get count preview first
    count = await review_service.get_review_count_by_filter(request)

    if count == 0:
        return BulkOperationResult(
            total=0,
            success=0,
            failed=0,
            failed_ids=[],
            errors=["No pending reviews match the specified filters"]
        )

    result = await review_service.bulk_approve_by_filter(request, reviewer_id)

    logger.info(
        f"Bulk approve by filter by {reviewer_id}: "
        f"{result.success}/{result.total} approved"
    )

    return result


@router.get("/bulk-approve-by-filter/preview")
async def preview_bulk_approve_by_filter(
    source_id: Optional[str] = Query(None, description="Filter by source ID"),
    confidence_min: Optional[float] = Query(None, ge=0, le=1, description="Minimum confidence score"),
    date_from: Optional[datetime] = Query(None, description="Start date filter"),
    date_to: Optional[datetime] = Query(None, description="End date filter"),
    db=Depends(get_db),
    auth: AuthContext = Depends(require_auth)
):
    """
    Preview count of reviews that would be approved by filter.

    Use this endpoint to check how many records would be affected
    before executing the bulk approve operation.

    Args:
        source_id: Optional source ID filter
        confidence_min: Optional minimum confidence threshold
        date_from: Optional start date
        date_to: Optional end date

    Returns:
        Dictionary with count and sample records
    """
    review_service = get_review_service(db)

    request = BulkFilterRequest(
        source_id=source_id,
        confidence_min=confidence_min,
        date_from=date_from,
        date_to=date_to,
        limit=1000
    )

    count = await review_service.get_review_count_by_filter(request)

    # Get sample records
    query = {"review_status": "pending"}
    if source_id:
        query["source_id"] = ObjectId(source_id)
    if confidence_min is not None:
        query["confidence_score"] = {"$gte": confidence_min}
    if date_from or date_to:
        date_query = {}
        if date_from:
            date_query["$gte"] = date_from
        if date_to:
            date_query["$lte"] = date_to
        query["created_at"] = date_query

    samples = await db.data_reviews.find(query).limit(5).to_list(5)

    return {
        "matching_count": count,
        "sample_records": [serialize_doc(s) for s in samples]
    }


@router.get("/bulk-jobs/{job_id}", response_model=BulkJobStatus)
async def get_bulk_job_status(
    job_id: str,
    db=Depends(get_db),
    auth: AuthContext = Depends(require_auth)
):
    """
    Get status of an async bulk operation job.

    For large bulk operations that run asynchronously, this endpoint
    allows tracking progress and retrieving final results.

    Args:
        job_id: The job ID returned when starting the bulk operation

    Returns:
        BulkJobStatus with current progress and results

    Raises:
        HTTPException: If job not found
    """
    review_service = get_review_service(db)
    job_status = await review_service.get_bulk_job_status(job_id)

    if not job_status:
        raise HTTPException(
            status_code=404,
            detail=f"Bulk job not found: {job_id}"
        )

    return job_status


@router.get("/bulk-jobs")
async def list_bulk_jobs(
    status: Optional[str] = Query(
        None,
        pattern="^(pending|processing|completed|failed)$",
        description="Filter by job status"
    ),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of jobs to return"),
    db=Depends(get_db),
    auth: AuthContext = Depends(require_auth)
):
    """
    List recent bulk operation jobs.

    Returns a list of bulk jobs with their status, useful for
    monitoring ongoing operations and reviewing past results.

    Args:
        status: Optional filter by job status
        limit: Maximum number of jobs to return (default 20, max 100)

    Returns:
        List of bulk job records
    """
    query = {}
    if status:
        query["status"] = status

    jobs = await db.bulk_jobs.find(query).sort(
        "started_at", -1
    ).limit(limit).to_list(limit)

    return [serialize_doc(j) for j in jobs]
