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
    # IST (UTC+5:30) like your code (kept as-is)
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


def _is_objectid(s: Any) -> bool:
    return isinstance(s, str) and ObjectId.is_valid(s)


def _to_objectid_if_possible(s: Any) -> Any:
    if _is_objectid(s):
        return ObjectId(s)
    return s


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


def _normalize_identifier(identifier: Optional[str]) -> Optional[str]:
    return identifier.lower() if isinstance(identifier, str) else identifier


def _normalize_thread_id_in_step(step_dict: Dict[str, Any]) -> Optional[str]:
    """
    Chainlit step dicts may contain thread_id or threadId depending on version/caller.
    We normalize to threadId and remove thread_id to keep schema consistent.
    """
    tid = step_dict.get("threadId") or step_dict.get("thread_id")
    if tid:
        step_dict["threadId"] = tid
        if "thread_id" in step_dict:
            del step_dict["thread_id"]
    return tid


def _pagination_skip_limit(pagination) -> Tuple[int, int]:
    """
    Accepts Chainlit Pagination object (may have offset/first or page/size).
    """
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


def _paginated_response(data: List[Dict[str, Any]], total: int, page: int, size: int) -> Dict[str, Any]:
    return {
        "data": data,
        "total": total,
        "pageInfo": {"page": page, "size": size, "total": total},
    }


def _prepare_thread_item(it: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare thread document for Chainlit UI.
    """
    if "id" not in it and it.get("_id"):
        it["id"] = str(it["_id"])

    it.setdefault("name", "Untitled")
    it.setdefault("created_at", _now())
    it.setdefault("updated_at", _now())

    it["createdAt"] = _encode_value(it["created_at"])
    it["updatedAt"] = _encode_value(it["updated_at"])

    return _encode_doc(it)  # type: ignore


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
        identifier = _normalize_identifier(identifier)
        if not identifier:
            return None

        logger.info(f"Getting user - identifier={identifier}")
        doc = await self.col_users.find_one({"identifier": identifier})
        if not doc:
            logger.debug(f"User not found - identifier={identifier}")
            return None

        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id"))},
        )

    async def create_user(self, user: cl.User):
        identifier = _normalize_identifier(user.identifier)
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

    async def delete_user_session(self, id: str) -> bool:
        # id may be ObjectId string or a custom string, handle both.
        query = {"_id": _to_objectid_if_possible(id)}
        res = await self.col_sessions.delete_one(query)
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
        # Chainlit expects you to persist elements (files/images/etc) for chat history
        if "id" not in element_dict:
            element_dict["id"] = str(uuid.uuid4())

        # Normalize thread id field
        tid = element_dict.get("threadId") or element_dict.get("thread_id")
        if tid:
            element_dict["threadId"] = tid
            if "thread_id" in element_dict:
                del element_dict["thread_id"]

        element_dict.setdefault("created_at", _now())
        await self.col_elements.update_one(
            {"id": element_dict["id"]},
            {"$set": element_dict},
            upsert=True,
        )
        return element_dict["id"]

    async def get_element(self, thread_id: str, element_id: str):
        # We scope by threadId + id
        doc = await self.col_elements.find_one({"threadId": thread_id, "id": element_id})
        return _encode_doc(doc)

    async def delete_element(self, element_id: str):
        res = await self.col_elements.delete_one({"id": element_id})
        return res.deleted_count == 1

    # ---------------- Steps (messages) ----------------
    async def create_step(self, step_dict: Dict[str, Any]):
        # Normalize userIdentifier
        if step_dict.get("userIdentifier"):
            step_dict["userIdentifier"] = _normalize_identifier(step_dict["userIdentifier"])

        if "id" not in step_dict:
            step_dict["id"] = str(uuid.uuid4())
        step_dict.setdefault("created_at", _now())

        # Normalize thread id field early
        tid = _normalize_thread_id_in_step(step_dict)

        await self.col_steps.update_one(
            {"id": step_dict["id"]},
            {"$set": step_dict},
            upsert=True,
        )

        # Create/update the thread ONLY on first user message (your original behavior)
        step_type = step_dict.get("type", "")
        is_user_message = step_type in ("user_message", "message")

        if not is_user_message or not tid:
            return step_dict["id"]

        # Determine userIdentifier (prefer explicit, else attempt Chainlit session)
        user_identifier = step_dict.get("userIdentifier")
        if not user_identifier:
            try:
                if getattr(cl, "context", None) and cl.context:
                    u = cl.user_session.get("user")
                    user_identifier = getattr(u, "identifier", u) if u else None
                    user_identifier = _normalize_identifier(user_identifier)
            except Exception as e:
                logger.debug(f"Could not get user from Chainlit session (normal for FastAPI calls): {e}")

        patch: Dict[str, Any] = {
            "$setOnInsert": {"id": tid, "created_at": _now()},
            "$set": {"updated_at": _now()},
        }

        if step_dict.get("user_id"):
            patch["$set"]["user_id"] = step_dict["user_id"]

        if user_identifier:
            patch["$set"]["userIdentifier"] = user_identifier

        if step_dict.get("chat_profile"):
            patch["$set"]["chat_profile"] = step_dict["chat_profile"]

        await self.col_threads.update_one({"id": tid}, patch, upsert=True)
        return step_dict["id"]

    async def update_step(self, step_dict: Dict[str, Any]):
        if step_dict.get("userIdentifier"):
            step_dict["userIdentifier"] = _normalize_identifier(step_dict["userIdentifier"])
        _normalize_thread_id_in_step(step_dict)

        await self.col_steps.update_one({"id": step_dict["id"]}, {"$set": step_dict})

    async def delete_step(self, step_id: str):
        res = await self.col_steps.delete_one({"id": step_id})
        return res.deleted_count == 1

    # ---------------- Threads ----------------
    async def get_thread_author(self, thread_id: str):
        t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        author = t.get("userIdentifier") if t else None
        return _normalize_identifier(author)

    async def list_threads(self, pagination, filters):
        """
        Chainlit passes a ThreadFilter that typically includes userId.
        We'll support:
        - filters.userId as Mongo ObjectId string (preferred)
        - or as identifier (fallback)
        """
        user_id = getattr(filters, "userId", None)
        chat_profile = getattr(filters, "chat_profile", None)

        if not user_id:
            return _paginated_response([], 0, 1, 0)

        # Resolve user doc
        user_doc = None
        if _is_objectid(user_id):
            user_doc = await self.col_users.find_one({"_id": ObjectId(user_id)})
        if not user_doc:
            # fallback: treat as identifier
            user_doc = await self.col_users.find_one({"identifier": _normalize_identifier(user_id)})

        if not user_doc:
            logger.warning(f"User not found for userId={user_id}")
            return _paginated_response([], 0, 1, 0)

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
        return _paginated_response(items, total, page_number, limit or len(items))

    async def get_thread(self, thread_id: str):
        # Chainlit signature is get_thread(self, thread_id: str) :contentReference[oaicite:4]{index=4}
        t = await self.col_threads.find_one({"id": thread_id})
        if not t:
            # fallback: allow reading by Mongo _id if caller passes it
            if _is_objectid(thread_id):
                t = await self.col_threads.find_one({"_id": ObjectId(thread_id)})
                if t and "id" not in t:
                    t["id"] = str(t["_id"])
            if not t:
                return None

        # Load steps: primary schema uses threadId, but keep legacy support
        steps_cursor = self.col_steps.find({"threadId": thread_id})
        steps = [_encode_doc(s) async for s in steps_cursor]

        if not steps:
            legacy_cursor = self.col_steps.find({"thread_id": thread_id})
            legacy_steps = [_encode_doc(s) async for s in legacy_cursor]
            steps = legacy_steps

        t["steps"] = steps

        # Ensure timestamps exist
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
        # Match Chainlit’s documented signature :contentReference[oaicite:5]{index=5}
        patch: Dict[str, Any] = {"updated_at": _now()}

        if name is not None:
            patch["name"] = name
        if user_id is not None:
            patch["user_id"] = user_id
        if metadata is not None:
            patch["metadata"] = metadata
        if tags is not None:
            patch["tags"] = tags

        # Best-effort: persist userIdentifier from session if available (optional)
        try:
            if getattr(cl, "context", None) and cl.context:
                current_user = cl.user_session.get("user")
                if current_user and hasattr(current_user, "identifier"):
                    patch.setdefault("userIdentifier", _normalize_identifier(current_user.identifier))
        except Exception as e:
            logger.debug(f"update_thread: no Chainlit context (normal for FastAPI calls): {e}")

        await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=True)
        return True

    async def delete_thread(self, thread_id: str):
        # Delete thread document
        await self.col_threads.delete_one({"id": thread_id})

        # Delete steps (new + legacy)
        await self.col_steps.delete_many({"threadId": thread_id})
        await self.col_steps.delete_many({"thread_id": thread_id})

        # Delete elements (new + legacy)
        await self.col_elements.delete_many({"threadId": thread_id})
        await self.col_elements.delete_many({"thread_id": thread_id})

        # Collect step ids for feedback cleanup
        step_ids: List[str] = []

        cursor = self.col_steps.find({"threadId": thread_id}, {"id": 1})
        async for s in cursor:
            if s.get("id"):
                step_ids.append(s["id"])

        legacy_cursor = self.col_steps.find({"thread_id": thread_id}, {"id": 1})
        async for s in legacy_cursor:
            if s.get("id"):
                step_ids.append(s["id"])

        # Feedback cleanup:
        # 1) by threadId
        await self.col_feedback.delete_many({"threadId": thread_id})

        # 2) by forId (step ids)
        if step_ids:
            await self.col_feedback.delete_many({"forId": {"$in": step_ids}})

        return True
