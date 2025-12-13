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


def _derive_thread_name_from_step(step: Dict[str, Any], max_len: int = 80) -> str:
    """
    Thread 'name' should be the first user message.
    Priority:
      1) explicit threadName/thread_title/name provided by UI
      2) message text: input/content/text/message.content/output
      3) fallback: "New Chat"
    """
    explicit = step.get("threadName") or step.get("thread_title") or step.get("name")
    if isinstance(explicit, str) and explicit.strip():
        title = " ".join(explicit.strip().split())
        return title[:max_len]

    candidates: List[Any] = [
        step.get("input"),
        step.get("content"),
        step.get("text"),
        step.get("output"),
    ]

    msg = step.get("message")
    if isinstance(msg, dict):
        candidates.insert(0, msg.get("content"))

    for c in candidates:
        if isinstance(c, str) and c.strip():
            clean = " ".join(c.strip().split())
            return clean[:max_len]

    return "New Chat"


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
      - Thread created ONLY when the first USER message arrives.
      - Thread name = first user message.
      - session_id is UUID.
      - Delete thread cascades to steps/elements/feedback.
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
        element = dict(element_dict)
        element.setdefault("id", str(uuid.uuid4()))
        element.setdefault("created_at", _now())
        element.setdefault("updated_at", _now())

        # Normalize thread field name
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
        doc = await self.col_elements.find_one({"id": element_id})
        return _encode_doc(doc)

    async def delete_element(self, element_id: str) -> bool:
        res = await self.col_elements.delete_one({"id": element_id})
        return res.deleted_count == 1

    # ---------------- Internal: userIdentifier resolution ----------------

    async def _resolve_user_identifier_for_step(self, step: Dict[str, Any]) -> Optional[str]:
        """
        For React->backend calls, Chainlit context may be missing.
        We must still set userIdentifier so list_threads works.
        """
        # 1) direct
        if step.get("userIdentifier"):
            return _safe_lower(step.get("userIdentifier"))

        # 2) lookup using userId/user_id (Mongo _id)
        user_id = step.get("userId") or step.get("user_id")
        if isinstance(user_id, str) and ObjectId.is_valid(user_id):
            udoc = await self.col_users.find_one({"_id": ObjectId(user_id)}, {"identifier": 1})
            if udoc and udoc.get("identifier"):
                return _safe_lower(udoc["identifier"])

        # 3) chainlit session (only if present)
        try:
            if hasattr(cl, "context") and cl.context:
                u = cl.user_session.get("user")
                if u:
                    ident = getattr(u, "identifier", u)
                    if isinstance(ident, str) and ident.strip():
                        return _safe_lower(ident)
        except Exception:
            pass

        return None

    # ---------------- Steps (messages) ----------------

    async def create_step(self, step_dict: Dict[str, Any]) -> str:
        """
        Production-safe:
        - Upsert step (handles retries)
        - Create/update thread only when user message
        - Thread name from first user message
        - Ensure userIdentifier is stored (so React list works)
        - Ensure session_id is UUID
        """
        step = dict(step_dict)
        logger.info(f"Creating step - keys={list(step.keys())}")

        # Ensure IDs / timestamps
        step.setdefault("id", str(uuid.uuid4()))
        step.setdefault("created_at", _now())
        step.setdefault("updated_at", _now())

        # Normalize threadId
        tid = step.get("threadId") or step.get("thread_id")
        if tid:
            step["threadId"] = tid
        if "thread_id" in step:
            del step["thread_id"]

        # session_id must be UUID (never user_id)
        session_id = step.get("session_id") or step.get("sessionId")
        if not session_id:
            session_id = str(uuid.uuid4())
        step["session_id"] = str(session_id)
        if "sessionId" in step:
            del step["sessionId"]

        # Resolve userIdentifier reliably
        user_identifier = await self._resolve_user_identifier_for_step(step)
        if user_identifier:
            step["userIdentifier"] = user_identifier

        # Upsert step (safer than insert_one)
        await self.col_steps.update_one({"id": step["id"]}, {"$set": step}, upsert=True)
        logger.info(f"Step stored - id={step['id']}, type={step.get('type')}, threadId={tid}")

        # Only create thread when user sends message
        step_type = (step.get("type") or "").strip()
        is_user_message = step_type in ["user_message", "message"]

        if not is_user_message or not tid:
            return step["id"]

        # Thread name = user message
        thread_name = _derive_thread_name_from_step(step)

        # Upsert thread
        patch: Dict[str, Any] = {
            "$setOnInsert": {
                "id": tid,
                "created_at": _now(),
                "name": thread_name,
                "session_id": step["session_id"],
                # IMPORTANT: store userIdentifier on insert if we have it
                **({"userIdentifier": user_identifier} if user_identifier else {}),
            },
            "$set": {
                "updated_at": _now(),
                "session_id": step["session_id"],
            },
        }

        # keep userIdentifier updated if present
        if user_identifier:
            patch["$set"]["userIdentifier"] = user_identifier

        # optional chat_profile
        if step.get("chat_profile"):
            patch["$set"]["chat_profile"] = step["chat_profile"]

        # If thread already exists with empty/Untitled, set name once (doesn't change behavior, fixes bad data)
        existing = await self.col_threads.find_one({"id": tid}, {"name": 1})
        if existing and (not existing.get("name") or existing.get("name") == "Untitled"):
            patch["$set"]["name"] = thread_name

        await self.col_threads.update_one({"id": tid}, patch, upsert=True)
        logger.info(f"Thread created/updated - id={tid}, userIdentifier={user_identifier}, name={thread_name}")

        return step["id"]

    async def update_step(self, step_dict: Dict[str, Any]) -> bool:
        step = dict(step_dict)
        if not step.get("id"):
            return False

        step["updated_at"] = _now()

        if "thread_id" in step and "threadId" not in step:
            step["threadId"] = step["thread_id"]
            del step["thread_id"]

        # Keep session_id consistent if provided
        if step.get("sessionId") and not step.get("session_id"):
            step["session_id"] = str(step["sessionId"])
            del step["sessionId"]

        if step.get("userIdentifier"):
            step["userIdentifier"] = _safe_lower(step["userIdentifier"])

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
        # Gather step ids (for feedback deletion by forId)
        step_ids: List[str] = []
        cursor = self.col_steps.find({"threadId": thread_id}, {"id": 1})
        async for s in cursor:
            if s.get("id"):
                step_ids.append(s["id"])

        fb_by_thread = await self.col_feedback.delete_many({"threadId": thread_id})

        fb_by_forid = None
        if step_ids:
            fb_by_forid = await self.col_feedback.delete_many({"forId": {"$in": step_ids}})

        el = await self.col_elements.delete_many({"threadId": thread_id})

        st1 = await self.col_steps.delete_many({"threadId": thread_id})
        st2 = await self.col_steps.delete_many({"thread_id": thread_id})

        th = await self.col_threads.delete_one({"id": thread_id})

        logger.info(
            "delete_thread cascade: "
            f"thread={th.deleted_count}, steps(threadId)={st1.deleted_count}, steps(thread_id)={st2.deleted_count}, "
            f"elements={el.deleted_count}, feedback_by_thread={fb_by_thread.deleted_count}, "
            f"feedback_by_forId={(fb_by_forid.deleted_count if fb_by_forid else 0)}"
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
        it.setdefault("name", "New Chat")
        it.setdefault("created_at", _now())
        it.setdefault("updated_at", _now())

        it["createdAt"] = _encode_value(it["created_at"])
        it["updatedAt"] = _encode_value(it["updated_at"])

        if "id" not in it and it.get("_id"):
            it["id"] = str(it["_id"])

        return _encode_doc(it)

    async def list_threads(self, pagination: Any, filters: Any) -> CLPaginatedResponse:
        user_id = getattr(filters, "userId", None)
        chat_profile = getattr(filters, "chat_profile", None)

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
        user_id: Optional[str] = None,  # kept for signature compatibility
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

        res = await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=False)
        return res.matched_count == 1
