
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

# ----------------------------
# Helpers
# ----------------------------

IST_OFFSET = datetime.timedelta(hours=5, minutes=30)


def _now() -> datetime.datetime:
    # Keep your existing behavior (UTC + 5:30)
    return datetime.datetime.utcnow() + IST_OFFSET


def _safe_lower(s: Optional[str]) -> Optional[str]:
    return s.lower() if isinstance(s, str) else s


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


def _resolve_chainlit_user_identifier() -> Optional[str]:
    try:
        u = cl.user_session.get("user")
        if u is None:
            return None
        if hasattr(u, "identifier"):
            return u.identifier
        if isinstance(u, str):
            return u
    except Exception:
        return None
    return None


def _normalize_thread_id(obj: Dict[str, Any]) -> Optional[str]:
    tid = obj.get("threadId") or obj.get("thread_id")
    if tid:
        obj["threadId"] = tid
    obj.pop("thread_id", None)
    return tid


def _normalize_user_identifier(obj: Dict[str, Any]) -> Optional[str]:
    if obj.get("userIdentifier"):
        obj["userIdentifier"] = _safe_lower(str(obj["userIdentifier"]))
        return obj["userIdentifier"]

    resolved = _resolve_chainlit_user_identifier()
    if resolved:
        obj["userIdentifier"] = _safe_lower(str(resolved))
        return obj["userIdentifier"]

    return None


def _get_message_content_from_step(step: Dict[str, Any]) -> str:
    """
    IMPORTANT: Match your threads_api.py logic exactly.
    Only use: output OR content OR text.
    """
    val = step.get("output") or step.get("content") or step.get("text") or ""
    if isinstance(val, str):
        val = " ".join(val.strip().split())
        return val
    return ""


def _sanitize_thread_name(name: str, max_len: int = 80) -> str:
    if not isinstance(name, str):
        return "Untitled"
    name = " ".join(name.strip().split())
    if not name:
        return "Untitled"
    return name[:max_len] if len(name) > max_len else name


class CLPaginatedResponse:
    def __init__(self, data: List[Dict[str, Any]], total: int, page: int, size: int):
        self.data = data
        self.total = total
        self.page_info = {"page": page, "size": size, "total": total}

    def to_dict(self) -> Dict[str, Any]:
        return {"data": self.data, "total": self.total, "pageInfo": self.page_info}


# ----------------------------
# Mongo Data Layer
# Collections: users, threads, steps, elements, feedback
# ----------------------------

class MongoDataLayer(BaseDataLayer):
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
        try:
            doc = await self.col_users.find_one({"identifier": identifier})
        except Exception as e:
            logger.error(f"Error getting user - identifier={identifier}: {e}", exc_info=True)
            return None
        if not doc:
            return None

        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id"))},
        )

    async def create_user(self, user: cl.User) -> Optional[cl.User]:
        identifier = _safe_lower(user.identifier)
        if not identifier:
            return None

        now = _now()
        payload = {
            "identifier": identifier,
            "metadata": user.metadata or {},
            "created_at": now,
            "updated_at": now,
        }

        try:
            await self.col_users.update_one(
                {"identifier": identifier},
                {"$setOnInsert": payload, "$set": {"updated_at": now, "metadata": user.metadata or {}}},
                upsert=True,
            )
            doc = await self.col_users.find_one({"identifier": identifier})
        except Exception as e:
            logger.error(f"Error creating user - identifier={identifier}: {e}", exc_info=True)
            return None

        return cl.User(
            identifier=identifier,
            metadata={**(user.metadata or {}), "_id": str(doc.get("_id")) if doc else None},
        )

    # ---------------- Feedback ----------------

    async def upsert_feedback(self, feedback) -> str:
        fid = getattr(feedback, "id", None) or str(uuid.uuid4())
        doc = asdict(feedback)
        now = _now()
        doc["id"] = fid
        doc.setdefault("created_at", now)
        doc["updated_at"] = now

        try:
            await self.col_feedback.update_one({"id": fid}, {"$set": doc}, upsert=True)
        except Exception as e:
            logger.error(f"Error upserting feedback - id={fid}: {e}", exc_info=True)

        return fid

    async def delete_feedback(self, feedback_id: str) -> bool:
        try:
            res = await self.col_feedback.delete_one({"id": feedback_id})
            return res.deleted_count == 1
        except Exception as e:
            logger.error(f"Error deleting feedback - feedback_id={feedback_id}: {e}", exc_info=True)
            return False

    # ---------------- Elements ----------------

    async def create_element(self, element_dict: Dict[str, Any]) -> str:
        element = dict(element_dict)
        element.setdefault("id", str(uuid.uuid4()))
        now = _now()
        element.setdefault("created_at", now)
        element.setdefault("updated_at", now)

        _normalize_thread_id(element)

        try:
            await self.col_elements.update_one(
                {"id": element["id"]},
                {"$set": element},
                upsert=True,
            )
        except Exception as e:
            logger.error(f"Error creating element - id={element.get('id')}: {e}", exc_info=True)

        return element["id"]

    async def get_element(self, element_id: str) -> Optional[Dict[str, Any]]:
        try:
            doc = await self.col_elements.find_one({"id": element_id})
            return _encode_doc(doc)
        except Exception as e:
            logger.error(f"Error getting element - element_id={element_id}: {e}", exc_info=True)
            return None

    async def delete_element(self, element_id: str) -> bool:
        try:
            res = await self.col_elements.delete_one({"id": element_id})
            return res.deleted_count == 1
        except Exception as e:
            logger.error(f"Error deleting element - element_id={element_id}: {e}", exc_info=True)
            return False

    # ---------------- Steps ----------------

    async def create_step(self, step_dict: Dict[str, Any]) -> str:
        """
        Store step.
        Create thread only when a USER message arrives.
        FIX: thread.name is always the first user message text (output/content/text),
             exactly like your threads_api.py.
        """
        step = dict(step_dict)

        _normalize_user_identifier(step)
        tid = _normalize_thread_id(step)

        step.setdefault("id", str(uuid.uuid4()))
        now = _now()
        step.setdefault("created_at", now)
        step.setdefault("updated_at", now)

        # Store step
        try:
            await self.col_steps.insert_one(step)
        except Exception as e:
            logger.warning(f"Step insert failed, falling back to upsert - id={step.get('id')}: {e}")
            try:
                await self.col_steps.update_one({"id": step["id"]}, {"$set": step}, upsert=True)
            except Exception as ee:
                logger.error(f"Error upserting step - id={step.get('id')}: {ee}", exc_info=True)

        # Thread creation only for user message (match your old code)
        step_type = step.get("type", "")
        is_user_message = step_type in ["user_message", "message"]

        if not is_user_message or not tid:
            return step["id"]

        # Create thread if missing
        try:
            existing = await self.col_threads.find_one({"id": tid}, {"id": 1})
        except Exception as e:
            logger.error(f"Error checking thread existence - id={tid}: {e}", exc_info=True)
            return step["id"]

        if not existing:
            # ✅ FIX: name from first user message (output/content/text only)
            first_msg = _get_message_content_from_step(step)
            thread_name = _sanitize_thread_name(first_msg)

            thread_doc: Dict[str, Any] = {
                "id": tid,
                "name": thread_name,
                "userIdentifier": step.get("userIdentifier"),
                "chat_profile": step.get("chat_profile"),
                "metadata": {},
                "tags": [],
                "created_at": now,
                "updated_at": now,
            }
            try:
                await self.col_threads.insert_one(thread_doc)
                logger.info(f"Thread created - id={tid}, name={thread_doc['name']}")
            except Exception as e:
                logger.error(f"Error creating thread - id={tid}: {e}", exc_info=True)
        else:
            # Update last activity only (do NOT touch name)
            try:
                patch: Dict[str, Any] = {"updated_at": _now()}
                if step.get("chat_profile"):
                    patch["chat_profile"] = step.get("chat_profile")
                await self.col_threads.update_one({"id": tid}, {"$set": patch}, upsert=False)
            except Exception as e:
                logger.error(f"Error updating thread activity - id={tid}: {e}", exc_info=True)

        return step["id"]

    async def update_step(self, step_dict: Dict[str, Any]) -> bool:
        step = dict(step_dict)
        sid = step.get("id")
        if not sid:
            return False

        step["updated_at"] = _now()
        _normalize_user_identifier(step)
        _normalize_thread_id(step)

        try:
            res = await self.col_steps.update_one({"id": sid}, {"$set": step}, upsert=False)
            return res.matched_count == 1
        except Exception as e:
            logger.error(f"Error updating step - id={sid}: {e}", exc_info=True)
            return False

    async def delete_step(self, step_id: str) -> bool:
        try:
            res = await self.col_steps.delete_one({"id": step_id})
            return res.deleted_count == 1
        except Exception as e:
            logger.error(f"Error deleting step - step_id={step_id}: {e}", exc_info=True)
            return False

    # ---------------- Threads ----------------

    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        try:
            t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        except Exception as e:
            logger.error(f"Error getting thread author - thread_id={thread_id}: {e}", exc_info=True)
            return None
        return _safe_lower(t.get("userIdentifier") if t else None)

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
        """
        Match your old behavior: filters.userId is Mongo ObjectId string.
        """
        user_id = getattr(filters, "userId", None)
        if not user_id:
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        try:
            user_doc = await self.col_users.find_one({"_id": ObjectId(user_id)})
        except Exception as e:
            logger.error(f"Error fetching user for list_threads - userId={user_id}: {e}", exc_info=True)
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        if not user_doc:
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        query: Dict[str, Any] = {"userIdentifier": user_doc.get("identifier")}

        if hasattr(filters, "chat_profile") and filters.chat_profile:
            query["chat_profile"] = filters.chat_profile

        skip, limit = self._calculate_pagination(pagination)

        try:
            total = await self.col_threads.count_documents(query)
            cursor = (
                self.col_threads.find(query)
                .sort("updated_at", -1)
                .skip(skip)
                .limit(limit)
            )
            raw_items = await cursor.to_list(length=limit)
        except Exception as e:
            logger.error(f"Error listing threads - query={query}: {e}", exc_info=True)
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        items = [self._prepare_thread_item(it) for it in raw_items]
        page_number = (skip // limit + 1) if limit else 1
        return CLPaginatedResponse(data=items, total=total, page=page_number, size=limit)

    async def get_thread(self, thread_id: str, user_identifier: Optional[str] = None) -> Optional[Dict[str, Any]]:
        query: Dict[str, Any] = {"id": thread_id}
        if user_identifier:
            query["userIdentifier"] = user_identifier

        try:
            t = await self.col_threads.find_one(query)
        except Exception as e:
            logger.error(f"Error getting thread - thread_id={thread_id}: {e}", exc_info=True)
            return None

        if not t:
            return None

        steps_query: Dict[str, Any] = {"threadId": thread_id}
        if user_identifier:
            steps_query["userIdentifier"] = user_identifier

        try:
            steps_cursor = self.col_steps.find(steps_query).sort("created_at", 1)
            steps = [_encode_doc(s) async for s in steps_cursor]
        except Exception as e:
            logger.error(f"Error getting steps for thread - thread_id={thread_id}: {e}", exc_info=True)
            steps = []

        t.setdefault("created_at", _now())
        t.setdefault("updated_at", _now())
        t["createdAt"] = _encode_value(t["created_at"])
        t["updatedAt"] = _encode_value(t["updated_at"])
        t["steps"] = steps

        if "id" not in t and t.get("_id"):
            t["id"] = str(t["_id"])

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
        """
        IMPORTANT FIX:
        Chainlit can call update_thread(name=<userIdentifier>) and overwrite your correct name.
        We prevent that by ignoring name if it equals the userIdentifier.
        """
        try:
            patch: Dict[str, Any] = {"updated_at": _now()}

            resolved_ui = user_identifier or _resolve_chainlit_user_identifier()
            resolved_ui_l = _safe_lower(str(resolved_ui)) if resolved_ui else None

            if name is not None:
                # ✅ Prevent overwriting thread name with userIdentifier
                if resolved_ui_l and _safe_lower(str(name)) == resolved_ui_l:
                    # ignore wrong name
                    pass
                else:
                    patch["name"] = name

            if user_id is not None:
                patch["user_id"] = user_id
            if metadata is not None:
                patch["metadata"] = metadata
            if tags is not None:
                patch["tags"] = tags
            if chat_profile is not None:
                patch["chat_profile"] = chat_profile

            if resolved_ui:
                patch["userIdentifier"] = resolved_ui_l

            await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=True)
            return True
        except Exception as e:
            logger.error(f"Error updating thread - thread_id={thread_id}: {e}", exc_info=True)
            return False

    async def delete_thread(self, thread_id: str) -> bool:
        """
        Keep your delete behavior (correct):
          - threads
          - steps
          - elements
          - feedback by threadId
          - feedback by forId (step ids) best-effort
        """
        try:
            step_ids: List[str] = []
            cursor = self.col_steps.find({"threadId": thread_id}, {"id": 1})
            async for s in cursor:
                if s.get("id"):
                    step_ids.append(s["id"])

            await self.col_feedback.delete_many({"threadId": thread_id})

            if step_ids:
                await self.col_feedback.delete_many({"forId": {"$in": step_ids}})

            await self.col_elements.delete_many({"threadId": thread_id})
            await self.col_steps.delete_many({"threadId": thread_id})

            # legacy cleanup (if any)
            await self.col_steps.delete_many({"thread_id": thread_id})

            th = await self.col_threads.delete_one({"id": thread_id})
            return th.deleted_count == 1
        except Exception as e:
            logger.error(f"Error deleting thread - thread_id={thread_id}: {e}", exc_info=True)
            return False
