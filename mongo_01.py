import datetime
import logging
import uuid
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

import chainlit as cl
import motor.motor_asyncio as motor
from bson.objectid import ObjectId
from chainlit.data.base import BaseDataLayer

logger = logging.getLogger("app")

# Expose 'id' on cl.User using the Mongo _id if present
setattr(cl.User, "id", property(lambda self: self.metadata.get("_id", self.identifier)))


# --------------------------
# Helpers
# --------------------------
def _now() -> datetime.datetime:
    # IST (UTC+5:30) – keeping your behavior
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


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


def _norm_identifier(x: Optional[str]) -> Optional[str]:
    return x.lower() if isinstance(x, str) else x


def _safe_objectid(value: Any) -> Optional[ObjectId]:
    if isinstance(value, str) and ObjectId.is_valid(value):
        return ObjectId(value)
    return None


def _get_chainlit_user_identifier() -> Optional[str]:
    """
    Best-effort resolve current Chainlit user identifier.
    Safe for FastAPI/non-Chainlit contexts too.
    """
    try:
        if hasattr(cl, "context") and cl.context:
            u = cl.user_session.get("user")
            if not u:
                return None
            ident = getattr(u, "identifier", None)
            return _norm_identifier(ident)
    except Exception:
        return None
    return None


def _normalize_thread_id(d: Dict[str, Any]) -> Optional[str]:
    """
    Normalize thread id field names to always store `threadId`.
    Supports legacy `thread_id`.
    """
    tid = d.get("threadId") or d.get("thread_id")
    if tid:
        d["threadId"] = tid
        if "thread_id" in d:
            del d["thread_id"]
    return tid


def _pagination_skip_limit(pagination) -> Tuple[int, int]:
    skip = 0
    limit = 20

    if pagination is None:
        return skip, limit

    offset = getattr(pagination, "offset", None)
    first = getattr(pagination, "first", None)
    page = getattr(pagination, "page", None)
    size = getattr(pagination, "size", None)
    limit_attr = getattr(pagination, "limit", None)

    if offset is not None:
        skip = int(offset) or 0
    if first is not None:
        limit = int(first) or limit

    if page and size:
        page_num = int(page) or 1
        size_num = int(size) or limit
        skip = (page_num - 1) * size_num
        limit = size_num

    if limit_attr is not None:
        limit = int(limit_attr) or limit

    return skip, limit


def _prepare_thread_item(it: Dict[str, Any]) -> Dict[str, Any]:
    if "id" not in it and it.get("_id"):
        it["id"] = str(it["_id"])

    it.setdefault("name", "Untitled")
    it.setdefault("created_at", _now())
    it.setdefault("updated_at", _now())

    it["createdAt"] = _encode_value(it["created_at"])
    it["updatedAt"] = _encode_value(it["updated_at"])

    return _encode_doc(it)  # type: ignore


def _is_user_step(step_dict: Dict[str, Any]) -> bool:
    """
    Thread should be created ONLY when user starts chatting.
    Accepts user steps by:
    - role == 'user'
    - type == 'user_message'
    - type == 'message' and role == 'user'
    """
    t = (step_dict.get("type") or "").lower()
    role = (step_dict.get("role") or "").lower()

    if role == "user":
        return True
    if t in ("user_message", "user"):
        return True
    if t == "message" and role == "user":
        return True

    return False


# --------------------------
# Chainlit expects .to_dict() for pagination
# --------------------------
class CLPaginatedResponse:
    def __init__(self, data: List[Dict[str, Any]], total: int, page: int, size: int):
        self.data = data
        self.total = total
        self.page_info = {"page": page, "size": size, "total": total}

    def to_dict(self) -> Dict[str, Any]:
        return {"data": self.data, "total": self.total, "pageInfo": self.page_info}


# --------------------------
# Data Layer
# --------------------------
class MongoDataLayer(BaseDataLayer):
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
        if getattr(self, "client", None):
            self.client.close()
            logger.info("MongoDB connection closed")

    def build_debug_url(self, thread_id: str) -> str:
        return f"mongodb://debug/thread/{thread_id}"

    # ---------------- Users ----------------
    async def get_user(self, identifier: str):
        identifier = _norm_identifier(identifier)
        if not identifier:
            return None

        doc = await self.col_users.find_one({"identifier": identifier})
        if not doc:
            return None

        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id"))},
        )

    async def create_user(self, user: cl.User):
        identifier = _norm_identifier(user.identifier)
        if not identifier:
            return None

        payload = {
            "identifier": identifier,
            "metadata": user.metadata or {},
            "created_at": _now(),
        }

        await self.col_users.update_one(
            {"identifier": identifier},
            {"$setOnInsert": payload},
            upsert=True,
        )

        doc = await self.col_users.find_one({"identifier": identifier})
        return cl.User(
            identifier=identifier,
            metadata={**(user.metadata or {}), "_id": str(doc.get("_id"))},
        )

    # ---------------- Sessions ----------------
    async def delete_user_session(self, id: str) -> bool:
        oid = _safe_objectid(id)
        res = await self.col_sessions.delete_one({"_id": oid if oid else id})
        return res.deleted_count == 1

    # ---------------- Feedback ----------------
    async def upsert_feedback(self, feedback):
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
    async def create_element(self, element_dict: Dict[str, Any]):
        if "id" not in element_dict:
            element_dict["id"] = str(uuid.uuid4())
        element_dict.setdefault("created_at", _now())

        _normalize_thread_id(element_dict)

        await self.col_elements.update_one(
            {"id": element_dict["id"]},
            {"$set": element_dict},
            upsert=True,
        )
        return element_dict["id"]

    async def get_element(self, thread_id: str, element_id: str):
        doc = await self.col_elements.find_one({"threadId": thread_id, "id": element_id})
        if not doc:
            doc = await self.col_elements.find_one({"thread_id": thread_id, "id": element_id})
        return _encode_doc(doc)

    async def delete_element(self, element_id: str) -> bool:
        res = await self.col_elements.delete_one({"id": element_id})
        return res.deleted_count == 1

    # ---------------- Steps ----------------
    async def create_step(self, step_dict: Dict[str, Any]):
        # Normalize userIdentifier
        if step_dict.get("userIdentifier"):
            step_dict["userIdentifier"] = _norm_identifier(step_dict["userIdentifier"])

        # Normalize threadId
        tid = _normalize_thread_id(step_dict)

        # Ensure id + created_at
        if "id" not in step_dict:
            step_dict["id"] = str(uuid.uuid4())
        step_dict.setdefault("created_at", _now())

        # Persist step always
        await self.col_steps.update_one(
            {"id": step_dict["id"]},
            {"$set": step_dict},
            upsert=True,
        )

        # ✅ Create thread ONLY when user starts chatting
        if tid and _is_user_step(step_dict):
            user_identifier = step_dict.get("userIdentifier") or _get_chainlit_user_identifier()
            user_identifier = _norm_identifier(user_identifier)

            patch: Dict[str, Any] = {
                "$setOnInsert": {"id": tid, "created_at": _now()},
                "$set": {"updated_at": _now()},
            }

            if user_identifier:
                patch["$set"]["userIdentifier"] = user_identifier

            if step_dict.get("chat_profile"):
                patch["$set"]["chat_profile"] = step_dict["chat_profile"]

            if step_dict.get("user_id"):
                patch["$set"]["user_id"] = step_dict["user_id"]

            await self.col_threads.update_one({"id": tid}, patch, upsert=True)

        return step_dict["id"]

    async def update_step(self, step_dict: Dict[str, Any]):
        if step_dict.get("userIdentifier"):
            step_dict["userIdentifier"] = _norm_identifier(step_dict["userIdentifier"])
        _normalize_thread_id(step_dict)
        await self.col_steps.update_one({"id": step_dict["id"]}, {"$set": step_dict})
        return True

    async def delete_step(self, step_id: str) -> bool:
        res = await self.col_steps.delete_one({"id": step_id})
        return res.deleted_count == 1

    # ---------------- Threads ----------------
    async def get_thread_author(self, thread_id: str):
        t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        author = t.get("userIdentifier") if t else None
        return _norm_identifier(author)

    async def list_threads(self, pagination, filters):
        """
        IMPORTANT: Must return an object with .to_dict().
        """
        user_id = getattr(filters, "userId", None)
        chat_profile = getattr(filters, "chat_profile", None)

        if not user_id:
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        # Resolve user doc
        user_doc = None
        oid = _safe_objectid(str(user_id))
        if oid:
            user_doc = await self.col_users.find_one({"_id": oid})
        if not user_doc:
            user_doc = await self.col_users.find_one({"identifier": _norm_identifier(str(user_id))})

        if not user_doc:
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        query: Dict[str, Any] = {"userIdentifier": user_doc.get("identifier")}
        if chat_profile:
            query["chat_profile"] = chat_profile

        skip, limit = _pagination_skip_limit(pagination)
        total = await self.col_threads.count_documents(query)

        cursor = (
            self.col_threads.find(query)
            .sort("updated_at", -1)
            .skip(skip)
            .limit(limit)
        )
        raw_items = await cursor.to_list(length=limit)
        items = [_prepare_thread_item(it) for it in raw_items]

        page_number = (skip // limit + 1) if limit else 1
        return CLPaginatedResponse(data=items, total=total, page=page_number, size=limit or len(items))

    async def get_thread(self, thread_id: str):
        t = await self.col_threads.find_one({"id": thread_id})
        if not t:
            oid = _safe_objectid(thread_id)
            if oid:
                t = await self.col_threads.find_one({"_id": oid})
                if t and "id" not in t:
                    t["id"] = str(t["_id"])

        if not t:
            return None

        # Steps ordered
        steps_cursor = self.col_steps.find({"threadId": thread_id}).sort("created_at", 1)
        steps = [_encode_doc(s) async for s in steps_cursor]

        if not steps:
            legacy_cursor = self.col_steps.find({"thread_id": thread_id}).sort("created_at", 1)
            steps = [_encode_doc(s) async for s in legacy_cursor]

        t["steps"] = steps

        t.setdefault("created_at", _now())
        t.setdefault("updated_at", _now())
        t["createdAt"] = _encode_value(t["created_at"])
        t["updatedAt"] = _encode_value(t["updated_at"])

        if "id" not in t and t.get("_id"):
            t["id"] = str(t["_id"])

        return _encode_doc(t)

    async def update_thread(
        self,
        thread_id: str,
        name: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
    ):
        patch: Dict[str, Any] = {"updated_at": _now()}

        if name is not None:
            patch["name"] = name
        if user_id is not None:
            patch["user_id"] = user_id
        if metadata is not None:
            patch["metadata"] = metadata
        if tags is not None:
            patch["tags"] = tags

        ui = _get_chainlit_user_identifier()
        if ui:
            patch.setdefault("userIdentifier", ui)

        await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=True)
        return True

    async def delete_thread(self, thread_id: str):
        # Collect step ids before deleting steps (for feedback cleanup by forId)
        step_ids: List[str] = []

        cur = self.col_steps.find({"threadId": thread_id}, {"id": 1})
        async for s in cur:
            if s.get("id"):
                step_ids.append(s["id"])

        legacy_cur = self.col_steps.find({"thread_id": thread_id}, {"id": 1})
        async for s in legacy_cur:
            if s.get("id"):
                step_ids.append(s["id"])

        # Delete thread
        await self.col_threads.delete_one({"id": thread_id})

        # Delete steps
        await self.col_steps.delete_many({"threadId": thread_id})
        await self.col_steps.delete_many({"thread_id": thread_id})

        # Delete elements
        await self.col_elements.delete_many({"threadId": thread_id})
        await self.col_elements.delete_many({"thread_id": thread_id})

        # Delete feedback
        await self.col_feedback.delete_many({"threadId": thread_id})
        if step_ids:
            await self.col_feedback.delete_many({"forId": {"$in": step_ids}})

        return True
