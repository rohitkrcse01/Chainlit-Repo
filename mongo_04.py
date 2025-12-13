import datetime
import uuid
from typing import Any, Dict, List, Optional, Tuple

import logging
from dataclasses import asdict

import chainlit as cl
from chainlit.data.base import BaseDataLayer

import motor.motor_asyncio as motor
from bson.objectid import ObjectId

logger = logging.getLogger("app")

# ----------------------------
# Time (UTC) - define ONCE
# ----------------------------
UTC = datetime.timezone.utc


def _now() -> datetime.datetime:
    """Return current time in UTC (timezone-aware)."""
    return datetime.datetime.now(UTC)


# ----------------------------
# Helpers
# ----------------------------

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


def _get_filter_value(filters: Any, key: str) -> Any:
    """
    Support both object-like filters (Chainlit) and dict-like filters (custom API).
    """
    if filters is None:
        return None
    if isinstance(filters, dict):
        return filters.get(key)
    return getattr(filters, key, None)


# Expose 'id' on cl.User using the Mongo _id if present
setattr(cl.User, "id", property(lambda self: self.metadata.get("_id", self.identifier)))


class CLPaginatedResponse:
    def __init__(self, data: List[Dict[str, Any]], total: int, page: int, size: int):
        self.data = data
        self.total = total
        self.page_info = {"page": page, "size": size, "total": total}

    def to_dict(self) -> Dict[str, Any]:
        return {"data": self.data, "total": self.total, "pageInfo": self.page_info}


class MongoDataLayer(BaseDataLayer):
    """
    MongoDB DataLayer for Chainlit + external React UI

    Collections:
      - users
      - threads
      - steps
      - elements
      - feedback

    Behavior:
      - Thread is created ONLY when the first USER message arrives (create_step).
      - Deleting a thread cascades deletion of related steps/elements/feedback.
    """

    def __init__(self, uri: str, db_name: str):
        self.client = motor.AsyncIOMotorClient(uri)
        self.db = self.client[db_name]

        self.col_users = self.db["users"]
        self.col_threads = self.db["threads"]
        self.col_steps = self.db["steps"]
        self.col_elements = self.db["elements"]
        self.col_feedback = self.db["feedback"]

        logger.info(f"MongoDB data layer initialized - database={db_name}")

    async def close(self):
        if getattr(self, "client", None):
            self.client.close()
            logger.info("MongoDB connection closed")

    def build_debug_url(self, thread_id: str) -> str:
        return f"mongodb://debug/thread/{thread_id}"

    # ---------------- Users ----------------

    async def get_user(self, identifier: str) -> Optional[cl.User]:
        identifier = _safe_lower(identifier)
        if not identifier:
            return None

        doc = await self.col_users.find_one({"identifier": identifier})
        if not doc:
            return None

        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id"))},
        )

    async def create_user(self, user: cl.User) -> cl.User:
        identifier = _safe_lower(user.identifier)
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

    # ---------------- Feedback ----------------

    async def upsert_feedback(self, feedback) -> str:
        fid = getattr(feedback, "id", None) or str(uuid.uuid4())
        doc = asdict(feedback)
        doc["id"] = fid
        doc["updated_at"] = _now()
        await self.col_feedback.update_one({"id": fid}, {"$set": doc}, upsert=True)
        return fid

    async def delete_feedback(self, feedback_id: str) -> bool:
        res = await self.col_feedback.delete_one({"id": feedback_id})
        return res.deleted_count == 1

    # ---------------- Elements ----------------

    async def create_element(self, element_dict: Dict[str, Any]) -> str:
        """
        Persist attachments / UI elements linked to a thread and/or step.
        """
        element = dict(element_dict)
        element.setdefault("id", str(uuid.uuid4()))
        element.setdefault("created_at", _now())
        element.setdefault("updated_at", _now())

        # Normalize thread field name BEFORE saving
        if "threadId" not in element and "thread_id" in element:
            element["threadId"] = element["thread_id"]
            del element["thread_id"]

        await self.col_elements.update_one(
            {"id": element["id"]},
            {"$set": element},
            upsert=True,
        )
        return element["id"]

    async def get_element(self, element_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single element by its id.
        """
        doc = await self.col_elements.find_one({"id": element_id})
        return _encode_doc(doc)

    async def delete_element(self, element_id: str) -> bool:
        res = await self.col_elements.delete_one({"id": element_id})
        return res.deleted_count == 1

    # ---------------- Steps (messages) ----------------

    async def create_step(self, step_dict: Dict[str, Any]) -> str:
        """
        Screenshot-aligned behavior (but production-safe):
        - Normalize thread_id -> threadId BEFORE saving (prevents mixed schema)
        - Upsert step by 'id' (prevents duplicates)
        - Thread is created/updated ONLY when step is a user message
        """
        logger.info(f"Creating step - step_dict={step_dict}")

        step = dict(step_dict)

        # Ensure step id / timestamps
        step.setdefault("id", str(uuid.uuid4()))
        step.setdefault("created_at", _now())
        step.setdefault("updated_at", _now())

        # Normalize userIdentifier to lowercase if present
        if step.get("userIdentifier"):
            step["userIdentifier"] = step["userIdentifier"].lower()

        # Normalize threadId BEFORE saving (fix mixed schema)
        tid = step.get("threadId") or step.get("thread_id")
        if tid:
            step["threadId"] = tid
        if "thread_id" in step:
            del step["thread_id"]

        # Persist step (UPSERT to avoid duplicates / retries)
        await self.col_steps.update_one(
            {"id": step["id"]},
            {"$setOnInsert": step},
            upsert=True,
        )
        logger.info(f"Step upserted - id={step['id']}, type={step.get('type')}")

        # Only create thread when user sends first message
        step_type = (step.get("type") or "")
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

        # Persist user_id if provided (legacy)
        if step.get("user_id"):
            patch["$set"]["user_id"] = step["user_id"]

        # Persist userIdentifier consistently (try Chainlit session if missing)
        user_identifier = step.get("userIdentifier")
        if not user_identifier:
            try:
                if hasattr(cl, "context") and cl.context:
                    u = cl.user_session.get("user")
                    user_identifier = getattr(u, "identifier", u) if u else None
            except Exception as e:
                logger.debug(f"Could not get user from Chainlit session: {e}")
                user_identifier = None

        if user_identifier:
            patch["$set"]["userIdentifier"] = str(user_identifier).lower()

        # Optional chat_profile
        if step.get("chat_profile"):
            patch["$set"]["chat_profile"] = step["chat_profile"]

        await self.col_threads.update_one({"id": tid}, patch, upsert=True)
        logger.info(f"Thread created/updated for user message - tid={tid}")

        return step["id"]

    async def update_step(self, step_dict: Dict[str, Any]) -> bool:
        step = dict(step_dict)
        if not step.get("id"):
            return False

        step["updated_at"] = _now()

        if step.get("userIdentifier"):
            step["userIdentifier"] = step["userIdentifier"].lower()

        if "thread_id" in step and "threadId" not in step:
            step["threadId"] = step["thread_id"]
            del step["thread_id"]

        res = await self.col_steps.update_one({"id": step["id"]}, {"$set": step})
        return res.matched_count == 1

    async def delete_step(self, step_id: str) -> bool:
        res = await self.col_steps.delete_one({"id": step_id})
        return res.deleted_count == 1

    # ---------------- Threads ----------------

    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        author = t.get("userIdentifier") if t else None
        return author.lower() if isinstance(author, str) else None

    async def delete_thread(self, thread_id: str) -> bool:
        """
        Cascade delete everything related to a thread:
          - steps (threadId + legacy thread_id)
          - elements (threadId + legacy thread_id)
          - feedback (threadId + forId)
          - thread
        """
        # Gather step ids (for feedback deletion by forId)
        step_ids: List[str] = []
        cursor = self.col_steps.find({"threadId": thread_id}, {"id": 1})
        async for s in cursor:
            if s.get("id"):
                step_ids.append(s["id"])

        # Delete feedback by threadId
        fb_by_thread = await self.col_feedback.delete_many({"threadId": thread_id})

        # Delete feedback by forId
        fb_by_forid = None
        if step_ids:
            fb_by_forid = await self.col_feedback.delete_many({"forId": {"$in": step_ids}})

        # Delete elements (support legacy thread_id too)
        el1 = await self.col_elements.delete_many({"threadId": thread_id})
        el2 = await self.col_elements.delete_many({"thread_id": thread_id})

        # Delete steps (support legacy thread_id too)
        st1 = await self.col_steps.delete_many({"threadId": thread_id})
        st2 = await self.col_steps.delete_many({"thread_id": thread_id})

        # Delete thread
        th = await self.col_threads.delete_one({"id": thread_id})

        logger.info(
            "delete_thread cascade: "
            f"thread={th.deleted_count}, steps(threadId)={st1.deleted_count}, steps(thread_id)={st2.deleted_count}, "
            f"elements(threadId)={el1.deleted_count}, elements(thread_id)={el2.deleted_count}, "
            f"feedback_by_thread={fb_by_thread.deleted_count}, feedback_by_forId={(fb_by_forid.deleted_count if fb_by_forid else 0)}"
        )

        return th.deleted_count == 1

    def _calculate_pagination(self, pagination: Any) -> Tuple[int, int]:
        skip = 0
        limit = 20

        if pagination is None:
            return skip, limit

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

        return skip, limit

    def _prepare_thread_item(self, it: Dict[str, Any]) -> Dict[str, Any]:
        it = dict(it)
        it.setdefault("name", "Untitled")
        it.setdefault("created_at", _now())
        it.setdefault("updated_at", _now())

        it["createdAt"] = _encode_value(it["created_at"])
        it["updatedAt"] = _encode_value(it["updated_at"])

        if "id" not in it and it.get("_id"):
            it["id"] = str(it["_id"])

        return _encode_doc(it)

    async def list_threads(self, pagination: Any, filters: Any) -> CLPaginatedResponse:
        user_id = _get_filter_value(filters, "userId")
        chat_profile = _get_filter_value(filters, "chat_profile")

        user_doc = None
        if isinstance(user_id, str) and ObjectId.is_valid(user_id):
            user_doc = await self.col_users.find_one({"_id": ObjectId(user_id)})
        elif isinstance(user_id, str) and user_id:
            user_doc = await self.col_users.find_one({"identifier": _safe_lower(user_id)})

        if not user_doc:
            logger.warning(f"list_threads: user not found for userId={user_id}")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        query: Dict[str, Any] = {"userIdentifier": user_doc.get("identifier")}
        if chat_profile:
            query["chat_profile"] = chat_profile

        skip, limit = self._calculate_pagination(pagination)

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
        query: Dict[str, Any] = {"id": thread_id}
        if user_identifier:
            query["userIdentifier"] = _safe_lower(user_identifier)

        t = await self.col_threads.find_one(query)
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
        patch: Dict[str, Any] = {"updated_at": _now()}

        if name is not None:
            patch["name"] = name
        if metadata is not None:
            patch["metadata"] = metadata
        if tags is not None:
            patch["tags"] = tags
        if chat_profile is not None:
            patch["chat_profile"] = chat_profile

        if user_identifier is not None:
            patch["userIdentifier"] = _safe_lower(user_identifier)

        if user_id is not None:
            patch["user_id"] = user_id

        res = await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=False)
        return res.matched_count == 1
