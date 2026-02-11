"""
Review rejection post-processing actions.

Handles automated responses to different rejection reasons:
- data_error: trigger re-crawl
- source_changed: snapshot version + flag for crawler update
- source_not_updated: keep current schedule (no action needed)
"""

import logging
from datetime import datetime
from bson import ObjectId

logger = logging.getLogger(__name__)


async def handle_rejection(db, review: dict, reason: str, notes: str = None):
    """
    Process rejection based on reason category.

    Args:
        db: MongoDB database instance
        review: The review document being rejected
        reason: Rejection reason category
        notes: Additional rejection notes
    """
    source_id = review.get("source_id")
    if not source_id:
        logger.warning(f"No source_id in review {review.get('_id')}")
        return

    if reason == "data_error":
        await _trigger_recrawl(db, source_id)
    elif reason == "source_changed":
        await _handle_source_changed(db, source_id, notes)
    elif reason == "source_not_updated":
        await _handle_source_not_updated(db, source_id)
    # 'other' requires no automated action


async def _trigger_recrawl(db, source_id):
    """Trigger re-crawl for data errors."""
    try:
        source = await db.sources.find_one({"_id": ObjectId(source_id) if isinstance(source_id, str) else source_id})
        if not source:
            logger.warning(f"Source {source_id} not found for re-crawl")
            return

        # Record re-crawl request
        await db.recrawl_requests.insert_one({
            "source_id": source["_id"],
            "reason": "data_error_rejection",
            "status": "pending",
            "requested_at": datetime.utcnow()
        })

        logger.info(f"Re-crawl requested for source {source_id} due to data error")
    except Exception as e:
        logger.error(f"Failed to trigger re-crawl for source {source_id}: {e}")


async def _handle_source_changed(db, source_id, notes: str = None):
    """Handle source structure change: snapshot + flag for update."""
    try:
        sid = ObjectId(source_id) if isinstance(source_id, str) else source_id

        # Create version snapshot of current data before any changes
        await db.version_snapshots.insert_one({
            "source_id": sid,
            "snapshot_type": "pre_structure_change",
            "reason": "source_changed_rejection",
            "notes": notes,
            "created_at": datetime.utcnow()
        })

        # Update source status to flag it needs crawler update
        await db.sources.update_one(
            {"_id": sid},
            {"$set": {
                "status": "error",
                "needs_crawler_update": True,
                "structure_change_detected_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }}
        )

        # Create error log entry
        await db.error_logs.insert_one({
            "source_id": sid,
            "error_code": "SOURCE_STRUCTURE_CHANGED",
            "error_type": "structure_change",
            "message": f"Source structure change detected during review. {notes or ''}".strip(),
            "auto_recoverable": False,
            "resolved": False,
            "created_at": datetime.utcnow()
        })

        logger.info(f"Source {source_id} flagged for crawler update due to structure change")
    except Exception as e:
        logger.error(f"Failed to handle source change for {source_id}: {e}")


async def _handle_source_not_updated(db, source_id):
    """Handle source not updated: log and wait for next scheduled crawl."""
    try:
        sid = ObjectId(source_id) if isinstance(source_id, str) else source_id

        await db.recrawl_requests.insert_one({
            "source_id": sid,
            "reason": "source_not_updated",
            "status": "scheduled",
            "notes": "Source content not updated. Will retry on next scheduled crawl.",
            "requested_at": datetime.utcnow()
        })

        logger.info(f"Source {source_id} marked for next scheduled re-crawl (not updated)")
    except Exception as e:
        logger.error(f"Failed to handle source_not_updated for {source_id}: {e}")
