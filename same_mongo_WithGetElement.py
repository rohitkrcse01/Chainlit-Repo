import datetime
import uuid
from typing import Any, Dict, List, Optional, Union

import logging
from dataclasses import asdict

import chainlit as cl
from chainlit.data.base import BaseDataLayer

import motor.motor_asyncio as motor
from bson.objectid import ObjectId

# Configure logging
logger = logging.getLogger("app")

# Expose 'id' on cl.User using the Mongo _id if present
setattr(cl.User, "id", property(lambda self: self.metadata.get("_id", self.identifier)))


# ----------------------------
# Helpers
# ----------------------------

def _now() -> datetime.datetime:
    # Return current time in Indian time zone (UTC+5:30)
    # NOTE: Keep as per your existing structure
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


def _to_objectid(value: Any) -> Optional[ObjectId]:
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return value
    if isinstance(value, str) and ObjectId.is_valid(value):
        return ObjectId(value)
    return None


def _encode_value(v: Any) -> Any:
    if isinstance(v, datetime.datetime):
        return v.isoformat()
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, dict):
        return {k: _encode_value(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_encode_value(i) for i in v]
    return v


def _encode_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return _encode_value(doc) if doc else None


def _safe_lower(s: Optional[str]) -> Optional[str]:
    return s.lower() if isinstance(s, str) else s


def _get_chainlit_user_identifier() -> Optional[str]:
    """
    Best-effort: resolve user identifier from Chainlit context.
    Works when called inside Chainlit runtime.
    """
    try:
        if hasattr(cl, "context") and cl.context:
            u = cl.user_session.get("user")
            if u is None:
                return None
            return getattr(u, "identifier", u) if u else None
    except Exception as e:
        logger.debug(f"Could not get user from Chainlit session: {e}")
    return None


def _normalize_thread_fields(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize thread_id -> threadId in-place.
    """
    tid = d.get("threadId") or d.get("thread_id")
    if tid:
        d["threadId"] = tid
    if "thread_id" in d:
        del d["thread_id"]
    return d


class CLPaginatedResponse:
    def __init__(self, data: List[Dict[str, Any]], total: int, page: int, size: int):
        self.data = data
        self.total = total
        self.page_info = {"page": page, "size": size, "total": total}

    def to_dict(self) -> Dict[str, Any]:
        return {"data": self.data, "total": self.total, "pageInfo": self.page_info}


# ----------------------------
# Mongo Data Layer
# ----------------------------

class MongoDataLayer(BaseDataLayer):
    """
    MongoDB DataLayer for Chainlit backend + external React frontend.

    Collections:
      - users
      - threads
      - steps
      - elements
      - feedback
      - sessions (kept since present in your base code)
    """

    def __init__(self, uri: str, db_name: str):
        self.client = motor.AsyncIOMotorClient(uri)
        self.db = self.client[db_name]
        self.col_users = self.db["users"]
        self.col_threads = self.db["threads"]
        self.col_steps = self.db["steps"]
        self.col_elements = self.db["elements"]
        self.col_feedback = self.db["feedback"]
        self.col_sessions = self.db["sessions"]
        logger.info(f"MongoDB data layer initialized - database={db_name}")

    async def close(self):
        """Close the MongoDB connection"""
        if getattr(self, "client", None):
            self.client.close()
            logger.info("MongoDB connection closed")

    def build_debug_url(self, thread_id: str) -> str:
        return f"mongodb://debug/thread/{thread_id}"

    # ---------------- Users CRUD ----------------

    async def get_user(self, identifier: str) -> Optional[cl.User]:
        identifier = _safe_lower(identifier)
        logger.info(f"Getting user - identifier={identifier}")
        if not identifier:
            return None

        doc = await self.col_users.find_one({"identifier": identifier})
        if not doc:
            logger.debug(f"User not found - identifier={identifier}")
            return None

        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id"))},
        )

    async def create_user(self, user: cl.User) -> cl.User:
        identifier = _safe_lower(user.identifier)
        logger.info(f"Creating user - identifier={identifier}")
        if not identifier:
            raise ValueError("User identifier is required")

        payload = {
            "identifier": identifier,
            "metadata": user.metadata or {},
            "created_at": _now(),
            "updated_at": _now(),
        }

        await self.col_users.update_one(
            {"identifier": identifier},
            {"$setOnInsert": payload, "$set": {"updated_at": _now()}},
            upsert=True,
        )

        doc = await self.col_users.find_one({"identifier": identifier})
        return cl.User(
            identifier=identifier,
            metadata={**(user.metadata or {}), "_id": str(doc.get("_id"))},
        )

    async def delete_user_session(self, id: str) -> bool:
        """
        Kept from your base code, but corrected to delete reliably.

        It will try:
          - delete by custom string field "id"
          - delete by Mongo _id if passed is an ObjectId string
          - delete by raw "_id": <string> (legacy)
        """
        logger.info(f"Deleting user session - id={id}")

        # 1) common pattern: sessions stored with string id field
        res = await self.col_sessions.delete_one({"id": id})
        if res.deleted_count == 1:
            logger.info(f"User session deleted - id={id} (by id field)")
            return True

        # 2) if actual Mongo ObjectId
        oid = _to_objectid(id)
        if oid:
            res2 = await self.col_sessions.delete_one({"_id": oid})
            if res2.deleted_count == 1:
                logger.info(f"User session deleted - id={id} (by _id ObjectId)")
                return True

        # 3) legacy raw string _id
        res3 = await self.col_sessions.delete_one({"_id": id})
        logger.info(f"User session deleted - id={id}, deleted={res3.deleted_count == 1}")
        return res3.deleted_count == 1

    # ---------------- Feedback CRUD ----------------

    async def upsert_feedback(self, feedback) -> str:
        fid = getattr(feedback, "id", None) or str(uuid.uuid4())
        doc = asdict(feedback)
        doc["id"] = fid
        doc["updated_at"] = _now()

        # Normalize common fields if present
        if doc.get("threadId") and isinstance(doc["threadId"], str):
            doc["threadId"] = doc["threadId"]
        if doc.get("forId") and isinstance(doc["forId"], str):
            doc["forId"] = doc["forId"]

        await self.col_feedback.update_one({"id": fid}, {"$set": doc}, upsert=True)
        return fid

    async def delete_feedback(self, feedback_id: str) -> bool:
        res = await self.col_feedback.delete_one({"id": feedback_id})
        return res.deleted_count == 1

    # ---------------- Elements CRUD ----------------

    async def create_element(self, element_dict: Dict[str, Any]) -> str:
        """
        Create an element (attachments/UI elements) in Mongo.
        """
        element = dict(element_dict)

        element.setdefault("id", str(uuid.uuid4()))
        element.setdefault("created_at", _now())
        element.setdefault("updated_at", _now())

        # Normalize thread field naming
        _normalize_thread_fields(element)

        # Upsert to be idempotent (production safe)
        await self.col_elements.update_one(
            {"id": element["id"]},
            {"$set": element},
            upsert=True,
        )
        logger.info(f"Element upserted - id={element['id']}")
        return element["id"]

    async def get_element(self, element_id: str) -> Optional[Dict[str, Any]]:
        """
        Proper get_element implementation (single element by id).
        This matches how Chainlit typically fetches an element for display/download.
        """
        if not element_id:
            return None
        doc = await self.col_elements.find_one({"id": element_id})
        return _encode_doc(doc)

    async def update_element(self, element_dict: Dict[str, Any]) -> bool:
        """
        Update an existing element by id.
        """
        element = dict(element_dict)
        eid = element.get("id")
        if not eid:
            return False

        element["updated_at"] = _now()
        _normalize_thread_fields(element)

        res = await self.col_elements.update_one({"id": eid}, {"$set": element}, upsert=False)
        return res.matched_count == 1

    async def delete_element(self, element_id: str) -> bool:
        res = await self.col_elements.delete_one({"id": element_id})
        return res.deleted_count == 1

    # ---------------- Steps CRUD (messages) ----------------

    async def create_step(self, step_dict: Dict[str, Any]) -> str:
        """
        IMPORTANT FLOW:
        - Always persist the step.
        - Create a thread ONLY when the first USER message arrives.
        - Keep your existing structure and naming.
        """
        step = dict(step_dict)

        # Normalize userIdentifier to lowercase if present
        if step.get("userIdentifier"):
            step["userIdentifier"] = _safe_lower(step["userIdentifier"])

        # Ensure IDs/timestamps
        step.setdefault("id", str(uuid.uuid4()))
        step.setdefault("created_at", _now())
        step.setdefault("updated_at", _now())

        # Normalize threadId BEFORE persisting to avoid mixed schema
        tid = step.get("threadId") or step.get("thread_id")
        if tid:
            step["threadId"] = tid
        if "thread_id" in step:
            del step["thread_id"]

        # Upsert step to avoid duplicates on retries
        await self.col_steps.update_one(
            {"id": step["id"]},
            {"$setOnInsert": step},
            upsert=True,
        )

        logger.info(f"Step upserted - id={step['id']}, type={step.get('type')}")

        # Create threads only on USER messages
        step_type = step.get("type", "")
        is_user_message = step_type in ["user_message", "message"]

        if not is_user_message:
            logger.info(f"Thread creation skipped for non-user message - type={step_type}")
            return step["id"]

        if not tid:
            return step["id"]

        patch: Dict[str, Any] = {
            "$setOnInsert": {"id": tid, "created_at": _now(), "name": "Untitled"},
            "$set": {"updated_at": _now()},
        }

        # Persist legacy user_id if provided
        if step.get("user_id"):
            patch["$set"]["user_id"] = step["user_id"]

        # Persist userIdentifier consistently
        user_identifier = step.get("userIdentifier") or _get_chainlit_user_identifier()
        if user_identifier:
            patch["$set"]["userIdentifier"] = _safe_lower(str(user_identifier))

        # Persist chat_profile if present (optional)
        if step.get("chat_profile"):
            patch["$set"]["chat_profile"] = step["chat_profile"]

        await self.col_threads.update_one({"id": tid}, patch, upsert=True)
        logger.info(f"Thread created/updated for user message - tid={tid}")

        return step["id"]

    async def update_step(self, step_dict: Dict[str, Any]) -> bool:
        step = dict(step_dict)
        sid = step.get("id")
        if not sid:
            return False

        if step.get("userIdentifier"):
            step["userIdentifier"] = _safe_lower(step["userIdentifier"])

        step["updated_at"] = _now()

        _normalize_thread_fields(step)

        res = await self.col_steps.update_one({"id": sid}, {"$set": step}, upsert=False)
        return res.matched_count == 1

    async def delete_step(self, step_id: str) -> bool:
        res = await self.col_steps.delete_one({"id": step_id})
        return res.deleted_count == 1

    # ---------------- Threads CRUD ----------------

    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        author = t.get("userIdentifier") if t else None
        return _safe_lower(author)

    async def delete_thread(self, thread_id: str) -> bool:
        """
        Cascade delete for React UI delete:
          - thread
          - steps (threadId + legacy thread_id)
          - elements (threadId + legacy thread_id)
          - feedback (threadId and forId linked to step ids)
          - sessions (if any) tied to threadId (best-effort)
        """
        logger.info(f"Deleting thread (cascade) - thread_id={thread_id}")

        # Collect step ids for feedback cleanup by forId
        step_ids: List[str] = []
        cursor = self.col_steps.find({"threadId": thread_id}, {"id": 1})
        async for st in cursor:
            if st.get("id"):
                step_ids.append(st["id"])

        cursor_legacy = self.col_steps.find({"thread_id": thread_id}, {"id": 1})
        async for st in cursor_legacy:
            if st.get("id"):
                step_ids.append(st["id"])

        # Delete feedback by threadId
        fb_by_thread = await self.col_feedback.delete_many({"threadId": thread_id})

        # Delete feedback by forId (step ids)
        fb_by_forid = None
        if step_ids:
            fb_by_forid = await self.col_feedback.delete_many({"forId": {"$in": list(set(step_ids))}})

        # Delete elements (standard + legacy)
        el1 = await self.col_elements.delete_many({"threadId": thread_id})
        el2 = await self.col_elements.delete_many({"thread_id": thread_id})

        # Delete steps (standard + legacy)
        st1 = await self.col_steps.delete_many({"threadId": thread_id})
        st2 = await self.col_steps.delete_many({"thread_id": thread_id})

        # Delete sessions for this thread (best-effort, since your DB includes sessions)
        ss = await self.col_sessions.delete_many({"threadId": thread_id})

        # Delete the thread doc itself
        th = await self.col_threads.delete_one({"id": thread_id})

        logger.info(
            "delete_thread cascade summary: "
            f"thread={th.deleted_count}, steps(threadId)={st1.deleted_count}, steps(thread_id)={st2.deleted_count}, "
            f"elements(threadId)={el1.deleted_count}, elements(thread_id)={el2.deleted_count}, "
            f"feedback(threadId)={fb_by_thread.deleted_count}, feedback(forId)={(fb_by_forid.deleted_count if fb_by_forid else 0)}, "
            f"sessions={ss.deleted_count}"
        )

        return th.deleted_count == 1

    def _calculate_pagination(self, pagination: Any) -> Dict[str, int]:
        skip = 0
        limit = 20

        if pagination is None:
            return {"skip": skip, "limit": limit}

        offset = getattr(pagination, "offset", None)
        if offset is not None:
            skip = int(offset) or 0

        first = getattr(pagination, "first", None)
        if first is not None:
            limit = int(first) or limit

        page = getattr(pagination, "page", None)
        size = getattr(pagination, "size", None)
        if page and size:
            page_num = int(page) or 1
            size_num = int(size) or limit
            skip = (page_num - 1) * size_num
            limit = size_num

        limit_attr = getattr(pagination, "limit", None)
        if limit_attr is not None:
            limit = int(limit_attr) or limit

        return {"skip": skip, "limit": limit}

    def _prepare_thread_item(self, it: Dict[str, Any]) -> Dict[str, Any]:
        it = dict(it)

        if "id" not in it and it.get("_id"):
            it["id"] = str(it["_id"])

        it.setdefault("name", "Untitled")
        it.setdefault("created_at", _now())
        it.setdefault("updated_at", _now())

        it["createdAt"] = _encode_value(it["created_at"])
        it["updatedAt"] = _encode_value(it["updated_at"])

        return _encode_doc(it)

    async def list_threads(self, pagination: Any, filters: Any) -> CLPaginatedResponse:
        logger.info(f"Listing threads - pagination={pagination}, filters={filters}")

        # Support object-like filters (Chainlit) and dict-like (custom)
        user_id = None
        chat_profile = None
        try:
            if isinstance(filters, dict):
                user_id = filters.get("userId")
                chat_profile = filters.get("chat_profile")
            else:
                user_id = getattr(filters, "userId", None)
                chat_profile = getattr(filters, "chat_profile", None)
        except Exception:
            user_id = None
            chat_profile = None

        if not user_id:
            logger.warning("list_threads: missing userId in filters")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        user_doc = None
        oid = _to_objectid(user_id)
        if oid:
            user_doc = await self.col_users.find_one({"_id": oid})
        else:
            user_doc = await self.col_users.find_one({"identifier": _safe_lower(str(user_id))})

        if not user_doc:
            logger.warning(f"User not found for userId={user_id}")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        query: Dict[str, Any] = {"userIdentifier": user_doc.get("identifier")}
        if chat_profile:
            query["chat_profile"] = chat_profile

        pag = self._calculate_pagination(pagination)
        skip, limit = pag["skip"], pag["limit"]

        total = await self.col_threads.count_documents(query)
        cursor = (
            self.col_threads.find(query)
            .sort("updated_at", -1)
            .skip(skip)
            .limit(limit)
        )

        raw_items = await cursor.to_list(length=limit)
        items = [self._prepare_thread_item(it) for it in raw_items]

        page_number = (skip // limit + 1) if limit else 1
        return CLPaginatedResponse(data=items, total=total, page=page_number, size=limit)

    async def get_thread(self, thread_id: str, user_identifier: Optional[str] = None) -> Optional[Dict[str, Any]]:
        logger.info(f"Getting thread - thread_id={thread_id}, user_identifier={user_identifier}")

        thread_query: Dict[str, Any] = {"id": thread_id}
        if user_identifier:
            thread_query["userIdentifier"] = _safe_lower(user_identifier)

        t = await self.col_threads.find_one(thread_query)
        if not t:
            return None

        steps_query: Dict[str, Any] = {"threadId": thread_id}
        if user_identifier:
            steps_query["userIdentifier"] = _safe_lower(user_identifier)

        steps_cursor = self.col_steps.find(steps_query).sort("created_at", 1)
        steps = [_encode_doc(s) async for s in steps_cursor]

        t.setdefault("created_at", _now())
        t.setdefault("updated_at", _now())
        t["createdAt"] = _encode_value(t["created_at"])
        t["updatedAt"] = _encode_value(t["updated_at"])

        if "id" not in t and t.get("_id"):
            t["id"] = str(t["_id"])

        t["steps"] = steps
        return _encode_doc(t)

    async def update_thread(
        self,
        thread_id: str,
        name: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        user_identifier: Optional[str] = None,
        chat_profile: Optional[str] = None,
    ) -> bool:
        try:
            patch: Dict[str, Any] = {"updated_at": _now()}

            if name is not None:
                patch["name"] = name
            if user_id is not None:
                patch["user_id"] = user_id
            if metadata is not None:
                patch["metadata"] = metadata
            if tags is not None:
                patch["tags"] = tags
            if chat_profile is not None:
                patch["chat_profile"] = chat_profile

            if user_identifier is not None:
                patch["userIdentifier"] = _safe_lower(user_identifier)
            else:
                resolved = _get_chainlit_user_identifier()
                if resolved:
                    patch["userIdentifier"] = _safe_lower(str(resolved))

            res = await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=True)
            return res.acknowledged is True
        except Exception as e:
            logger.error(f"Error updating thread - thread_id={thread_id}: {e}", exc_info=True)
            return False
