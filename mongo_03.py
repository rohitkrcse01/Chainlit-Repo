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
# Helpers
# ----------------------------

def _utcnow() -> datetime.datetime:
    # Production best practice: store UTC in DB
    return datetime.datetime.now(datetime.timezone.utc)


def _as_str_objectid(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, str) and ObjectId.is_valid(value):
            return str(ObjectId(value))
    except Exception:
        return None
    return None


def _encode_value(v: Any) -> Any:
    if isinstance(v, datetime.datetime):
        # ensure isoformat with tz if available
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


def _resolve_chainlit_user_identifier() -> Optional[str]:
    """
    Resolve user identifier from Chainlit context, if available.
    Works when called inside Chainlit execution context.
    """
    try:
        if hasattr(cl, "context") and cl.context:
            u = cl.user_session.get("user")
            if u is None:
                return None
            # cl.User has `identifier`
            if hasattr(u, "identifier"):
                return u.identifier
            # fallback if stored raw string
            if isinstance(u, str):
                return u
    except Exception:
        return None
    return None


# Expose 'id' on cl.User using the Mongo _id if present
setattr(cl.User, "id", property(lambda self: self.metadata.get("_id", self.identifier)))


class CLPaginatedResponse:
    """
    Matches what your React API / Chainlit expects in responses.
    """
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
    Production MongoDB DataLayer for Chainlit + external React UI.

    Collections:
      - users
      - threads
      - steps
      - elements
      - feedback
      - sessions
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
        if getattr(self, "client", None):
            self.client.close()
            logger.info("MongoDB connection closed")

    def build_debug_url(self, thread_id: str) -> str:
        return f"mongodb://debug/thread/{thread_id}"

    async def ensure_indexes(self) -> None:
        """
        Call once at startup (recommended).
        """
        # Users
        await self.col_users.create_index("identifier", unique=True)

        # Threads: fast list threads by user + sort by updated_at
        await self.col_threads.create_index([("userIdentifier", 1), ("updated_at", -1)])
        await self.col_threads.create_index("chat_profile")

        # Steps: fetch by thread and order by created_at
        await self.col_steps.create_index([("threadId", 1), ("created_at", 1)])
        await self.col_steps.create_index("id", unique=True)

        # Elements / Feedback
        await self.col_elements.create_index("threadId")
        await self.col_feedback.create_index("threadId")
        await self.col_feedback.create_index("forId")

        # Sessions
        await self.col_sessions.create_index("id", unique=True)
        await self.col_sessions.create_index("threadId")
        await self.col_sessions.create_index([("userIdentifier", 1), ("updated_at", -1)])

        logger.info("MongoDB indexes ensured.")

    # ---------------- Users CRUD ----------------

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
            "created_at": _utcnow(),
            "updated_at": _utcnow(),
        }

        await self.col_users.update_one(
            {"identifier": identifier},
            {"$setOnInsert": payload, "$set": {"updated_at": _utcnow()}},
            upsert=True,
        )

        doc = await self.col_users.find_one({"identifier": identifier})
        return cl.User(
            identifier=identifier,
            metadata={**(user.metadata or {}), "_id": str(doc.get("_id"))},
        )

    # ---------------- Sessions CRUD ----------------
    # (Useful when your React UI tracks "current session" or when you want server-side session persistence.)

    async def create_session(self, session: Dict[str, Any]) -> str:
        """
        Create a session record.
        Expected useful fields:
          - id (str) (optional, generated if missing)
          - userIdentifier (str)
          - threadId (str)
          - metadata (dict)
        """
        sid = session.get("id") or str(uuid.uuid4())
        now = _utcnow()

        doc = {
            "id": sid,
            "userIdentifier": _safe_lower(session.get("userIdentifier")),
            "threadId": session.get("threadId"),
            "metadata": session.get("metadata") or {},
            "created_at": session.get("created_at") or now,
            "updated_at": now,
        }

        await self.col_sessions.update_one({"id": sid}, {"$set": doc}, upsert=True)
        return sid

    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        doc = await self.col_sessions.find_one({"id": session_id})
        return _encode_doc(doc)

    async def update_session(self, session_id: str, patch: Dict[str, Any]) -> bool:
        patch = dict(patch or {})
        patch["updated_at"] = _utcnow()
        res = await self.col_sessions.update_one({"id": session_id}, {"$set": patch})
        return res.matched_count == 1

    async def delete_user_session(self, session_id: str) -> bool:
        """
        Delete by our own string `id`.
        (This fixes the common bug of deleting by Mongo `_id` with a string.)
        """
        res = await self.col_sessions.delete_one({"id": session_id})
        return res.deleted_count == 1

    async def delete_sessions_for_thread(self, thread_id: str) -> int:
        res = await self.col_sessions.delete_many({"threadId": thread_id})
        return int(res.deleted_count or 0)

    # ---------------- Feedback CRUD ----------------

    async def upsert_feedback(self, feedback) -> str:
        fid = getattr(feedback, "id", None) or str(uuid.uuid4())
        doc = asdict(feedback)
        doc["id"] = fid
        doc["updated_at"] = _utcnow()

        # normalize
        if "threadId" in doc:
            doc["threadId"] = doc["threadId"]
        if "forId" in doc:
            doc["forId"] = doc["forId"]

        await self.col_feedback.update_one({"id": fid}, {"$set": doc}, upsert=True)
        return fid

    async def delete_feedback(self, feedback_id: str) -> bool:
        res = await self.col_feedback.delete_one({"id": feedback_id})
        return res.deleted_count == 1

    # ---------------- Elements CRUD ----------------

    async def create_element(self, element_dict: Dict[str, Any]) -> str:
        """
        Persist attachments / UI elements linked to a thread and/or step.
        """
        element_dict = dict(element_dict)
        element_dict.setdefault("id", str(uuid.uuid4()))
        element_dict.setdefault("created_at", _utcnow())
        element_dict.setdefault("updated_at", _utcnow())

        # normalize thread field name
        if "threadId" not in element_dict and "thread_id" in element_dict:
            element_dict["threadId"] = element_dict["thread_id"]
            del element_dict["thread_id"]

        await self.col_elements.update_one(
            {"id": element_dict["id"]},
            {"$set": element_dict},
            upsert=True,
        )
        return element_dict["id"]

    async def delete_element(self, element_id: str) -> bool:
        res = await self.col_elements.delete_one({"id": element_id})
        return res.deleted_count == 1

    # ---------------- Steps CRUD (messages) ----------------

    async def create_step(self, step_dict: Dict[str, Any]) -> str:
        """
        This is called by Chainlit when a message/step is created.

        Requirement: thread should only be created when user starts chat.
        We implement:
          - Only create a thread (upsert) when step type indicates a USER message
          - Always store steps
        """
        step = dict(step_dict)

        # normalize ids and timestamps
        step.setdefault("id", str(uuid.uuid4()))
        step.setdefault("created_at", _utcnow())
        step.setdefault("updated_at", _utcnow())

        # normalize userIdentifier
        if step.get("userIdentifier"):
            step["userIdentifier"] = _safe_lower(step["userIdentifier"])
        else:
            resolved = _resolve_chainlit_user_identifier()
            if resolved:
                step["userIdentifier"] = _safe_lower(resolved)

        # normalize threadId
        tid = step.get("threadId") or step.get("thread_id")
        if tid:
            step["threadId"] = tid
        if "thread_id" in step:
            del step["thread_id"]

        # persist step
        await self.col_steps.update_one({"id": step["id"]}, {"$set": step}, upsert=True)

        # Thread creation rule: only on first USER message
        step_type = (step.get("type") or "").strip()
        is_user_message = step_type in ["user_message", "message"]

        if not is_user_message:
            return step["id"]

        if not tid:
            # if for some reason threadId missing, we cannot create thread
            return step["id"]

        # Create thread ONLY if it doesn't exist yet
        existing = await self.col_threads.find_one({"id": tid}, {"id": 1})
        if not existing:
            thread_doc: Dict[str, Any] = {
                "id": tid,
                "name": step.get("threadName") or step.get("name") or "Untitled",
                "userIdentifier": step.get("userIdentifier"),
                "chat_profile": step.get("chat_profile"),
                "created_at": _utcnow(),
                "updated_at": _utcnow(),
                "metadata": {},
                "tags": [],
            }
            await self.col_threads.insert_one(thread_doc)
        else:
            # update last activity
            patch: Dict[str, Any] = {"updated_at": _utcnow()}
            if step.get("userIdentifier"):
                patch["userIdentifier"] = step.get("userIdentifier")
            if step.get("chat_profile"):
                patch["chat_profile"] = step.get("chat_profile")
            await self.col_threads.update_one({"id": tid}, {"$set": patch})

        return step["id"]

    async def update_step(self, step_dict: Dict[str, Any]) -> bool:
        step = dict(step_dict)
        if not step.get("id"):
            return False
        step["updated_at"] = _utcnow()

        # normalize
        if step.get("userIdentifier"):
            step["userIdentifier"] = _safe_lower(step["userIdentifier"])
        if "thread_id" in step and "threadId" not in step:
            step["threadId"] = step["thread_id"]
            del step["thread_id"]

        res = await self.col_steps.update_one({"id": step["id"]}, {"$set": step})
        return res.matched_count == 1

    async def delete_step(self, step_id: str) -> bool:
        res = await self.col_steps.delete_one({"id": step_id})
        return res.deleted_count == 1

    # ---------------- Threads CRUD ----------------

    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        author = t.get("userIdentifier") if t else None
        return _safe_lower(author)

    def _calculate_pagination(self, pagination: Any) -> Tuple[int, int]:
        """
        Supports common pagination objects:
          - offset/first
          - page/size
          - limit
        """
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
        it.setdefault("created_at", _utcnow())
        it.setdefault("updated_at", _utcnow())

        it["createdAt"] = _encode_value(it["created_at"])
        it["updatedAt"] = _encode_value(it["updated_at"])

        # ensure id exists
        if "id" not in it and it.get("_id"):
            it["id"] = str(it["_id"])

        return _encode_doc(it)

    async def list_threads(self, pagination: Any, filters: Any) -> CLPaginatedResponse:
        """
        List threads for a user (React UI sidebar).

        Accepts filters with:
          - userId (Mongo _id OR identifier string)
          - chat_profile (optional)
        """
        user_id = getattr(filters, "userId", None)
        chat_profile = getattr(filters, "chat_profile", None)

        user_doc = None
        if isinstance(user_id, str) and ObjectId.is_valid(user_id):
            user_doc = await self.col_users.find_one({"_id": ObjectId(user_id)})
        elif isinstance(user_id, str) and user_id:
            # treat as identifier if not an ObjectId
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
        """
        Get one thread + steps (React UI opens a chat).
        """
        query: Dict[str, Any] = {"id": thread_id}
        if user_identifier:
            query["userIdentifier"] = _safe_lower(user_identifier)

        t = await self.col_threads.find_one(query)
        if not t:
            return None

        # steps in chronological order
        steps_query: Dict[str, Any] = {"threadId": thread_id}
        if user_identifier:
            steps_query["userIdentifier"] = _safe_lower(user_identifier)

        steps_cursor = (
            self.col_steps.find(steps_query).sort("created_at", 1)
        )
        steps = [_encode_doc(s) async for s in steps_cursor]

        t.setdefault("created_at", _utcnow())
        t.setdefault("updated_at", _utcnow())
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
        """
        Update thread metadata (React UI rename, tag, etc.).
        """
        patch: Dict[str, Any] = {"updated_at": _utcnow()}

        if name is not None:
            patch["name"] = name
        if metadata is not None:
            patch["metadata"] = metadata
        if tags is not None:
            patch["tags"] = tags
        if chat_profile is not None:
            patch["chat_profile"] = chat_profile

        # Normalize userIdentifier
        resolved_user_identifier = user_identifier or _resolve_chainlit_user_identifier()
        if resolved_user_identifier:
            patch["userIdentifier"] = _safe_lower(resolved_user_identifier)

        # Optional legacy field user_id (if your React app uses it)
        if user_id is not None:
            patch["user_id"] = user_id

        res = await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=False)
        return res.matched_count == 1

    async def delete_thread(self, thread_id: str) -> bool:
        """
        Cascade delete:
          - threads
          - steps
          - elements
          - feedback
          - sessions for that thread

        Works whether the delete comes from React UI or inside Chainlit session.
        """
        # Gather step ids (for feedback deletion by forId)
        step_ids: List[str] = []
        cursor = self.col_steps.find({"threadId": thread_id}, {"id": 1})
        async for s in cursor:
            if s.get("id"):
                step_ids.append(s["id"])

        # Delete feedback by threadId
        fb1 = await self.col_feedback.delete_many({"threadId": thread_id})

        # Delete feedback by forId (if your feedback schema uses forId)
        fb2 = None
        if step_ids:
            fb2 = await self.col_feedback.delete_many({"forId": {"$in": step_ids}})

        # Delete elements
        el = await self.col_elements.delete_many({"threadId": thread_id})

        # Delete sessions tied to this thread
        ss = await self.col_sessions.delete_many({"threadId": thread_id})

        # Delete steps
        st = await self.col_steps.delete_many({"threadId": thread_id})

        # Delete thread document
        th = await self.col_threads.delete_one({"id": thread_id})

        logger.info(
            "delete_thread cascade: "
            f"thread={th.deleted_count}, steps={st.deleted_count}, elements={el.deleted_count}, "
            f"feedback_by_thread={fb1.deleted_count}, feedback_by_forId={(fb2.deleted_count if fb2 else 0)}, "
            f"sessions={ss.deleted_count}"
        )

        return th.deleted_count == 1
