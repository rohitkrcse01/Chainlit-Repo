
import datetime
import logging
import uuid
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

import chainlit as cl
import motor.motor_asyncio as motor
from bson.objectid import ObjectId
from chainlit.data.base import BaseDataLayer

# Configure logging
logger = logging.getLogger("app")

# Expose 'id' on cl.User using the Mongo _id if present
setattr(cl.User, "id", property(lambda self: self.metadata.get("_id", self.identifier)))


# ----------------------------
# Helpers
# ----------------------------

IST_OFFSET = datetime.timedelta(hours=5, minutes=30)


def _now() -> datetime.datetime:
    # Return current time in Indian time zone (UTC+5:30)
    return datetime.datetime.utcnow() + IST_OFFSET


def _safe_lower(s: Optional[str]) -> Optional[str]:
    return s.lower() if isinstance(s, str) else s


def _as_objectid(value: Any) -> Optional[ObjectId]:
    try:
        if isinstance(value, ObjectId):
            return value
        if isinstance(value, str) and ObjectId.is_valid(value):
            return ObjectId(value)
    except Exception:
        return None
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


def _resolve_chainlit_user_identifier() -> Optional[str]:
    try:
        if hasattr(cl, "context") and cl.context:
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

    # ---------------- Users ----------------

    async def get_user(self, identifier: str) -> Optional[cl.User]:
        identifier = _safe_lower(identifier)
        logger.info(f"Getting user - identifier={identifier}")
        if not identifier:
            return None
        try:
            doc = await self.col_users.find_one({"identifier": identifier})
        except Exception as e:
            logger.error(f"Error getting user - identifier={identifier}: {e}", exc_info=True)
            return None

        if not doc:
            logger.debug(f"User not found - identifier={identifier}")
            return None

        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id"))},
        )

    async def create_user(self, user: cl.User) -> Optional[cl.User]:
        identifier = _safe_lower(user.identifier)
        logger.info(f"Creating user - identifier={identifier}")
        if not identifier:
            return None

        payload = {
            "identifier": identifier,
            "metadata": user.metadata or {},
            "created_at": _now(),
            
        }

        try:
            await self.col_users.update_one(
                {"identifier": identifier},
                {"$setOnInsert": payload, "$set": {"updated_at": _now()}},
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

    async def delete_user_session(self, id: str) -> bool:
        """
        Chainlit calls this. Your project has a sessions collection.
        Keep behavior: delete by Mongo _id if valid, otherwise by string id if stored that way.
        """
        logger.info(f"Deleting user session - id={id}")
        try:
            oid = _as_objectid(id)
            if oid is not None:
                await self.col_sessions.delete_one({"_id": oid})
            else:
                # fallback: if you stored your own "id" field
                await self.col_sessions.delete_one({"id": id})
            return True
        except Exception as e:
            logger.error(f"Error deleting user session - id={id}: {e}", exc_info=True)
            return False

    # ---------------- Feedback ----------------

    async def upsert_feedback(self, feedback) -> str:
        logger.info(f"Upserting feedback")
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
        logger.info(f"Deleting feedback - feedback_id={feedback_id}")
        try:
            res = await self.col_feedback.delete_one({"id": feedback_id})
            return res.deleted_count == 1
        except Exception as e:
            logger.error(f"Error deleting feedback - feedback_id={feedback_id}: {e}", exc_info=True)
            return False

    # ---------------- Elements ----------------

    async def create_element(self, element_dict: Dict[str, Any]) -> str:
        logger.info("Creating element")
        element_dict = dict(element_dict)

        element_dict.setdefault("id", str(uuid.uuid4()))
        now = _now()
        element_dict.setdefault("created_at", now)
        element_dict.setdefault("updated_at", now)

        # Normalize thread field naming
        if element_dict.get("thread_id") and not element_dict.get("threadId"):
            element_dict["threadId"] = element_dict.pop("thread_id")

        try:
            await self.col_elements.update_one(
                {"id": element_dict["id"]},
                {"$set": element_dict},
                upsert=True,
            )
        except Exception as e:
            logger.error(f"Error creating element - id={element_dict.get('id')}: {e}", exc_info=True)

        return element_dict["id"]

    async def get_element(self, element_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single element by its id.
        Chainlit expects to retrieve stored attachments/elements using the element id.
        """
        logger.info(f"Getting element - element_id={element_id}")
        try:
            doc = await self.col_elements.find_one({"id": element_id})
            return _encode_doc(doc)
        except Exception as e:
            logger.error(f"Error getting element - element_id={element_id}: {e}", exc_info=True)
            return None

    async def delete_element(self, element_id: str) -> bool:
        logger.info(f"Deleting element - element_id={element_id}")
        try:
            res = await self.col_elements.delete_one({"id": element_id})
            return res.deleted_count == 1
        except Exception as e:
            logger.error(f"Error deleting element - element_id={element_id}: {e}", exc_info=True)
            return False

    # ---------------- Steps (messages) ----------------

    async def create_step(self, step_dict: Dict[str, Any]) -> str:
        """
        Store step always.
        Create thread ONLY on first user message AND ONLY if thread does not already exist.
        IMPORTANT: Do NOT upsert threads here; that prevents re-creating deleted threads.
        """
        step = dict(step_dict)
        logger.info(f"Creating step - type={step.get('type')}")

        # Normalize userIdentifier
        if step.get("userIdentifier"):
            step["userIdentifier"] = _safe_lower(step["userIdentifier"])
        else:
            resolved = _resolve_chainlit_user_identifier()
            if resolved:
                step["userIdentifier"] = _safe_lower(resolved)

        # Ensure ids/timestamps
        step.setdefault("id", str(uuid.uuid4()))
        now = _now()
        step.setdefault("created_at", now)
        step.setdefault("updated_at", now)

        # Normalize threadId
        tid = step.get("threadId") or step.get("thread_id")
        if tid:
            step["threadId"] = tid
        if "thread_id" in step:
            del step["thread_id"]

        # Persist step
        try:
            await self.col_steps.insert_one(step)
        except Exception as e:
            # If your system might retry same step id, fall back to upsert by id
            logger.warning(f"Step insert failed, falling back to upsert - id={step.get('id')}: {e}")
            try:
                await self.col_steps.update_one({"id": step["id"]}, {"$set": step}, upsert=True)
            except Exception as ee:
                logger.error(f"Error upserting step - id={step.get('id')}: {ee}", exc_info=True)

        # Thread creation rule: only on USER message
        step_type = (step.get("type") or "").strip().lower()
        is_user_message = step_type in {"user_message", "message", "user"}

        if not is_user_message:
            return step["id"]

        if not tid:
            logger.warning("User message missing threadId/thread_id; thread will not be created.")
            return step["id"]

        # IMPORTANT: Do NOT recreate deleted threads. Create only if missing.
        try:
            existing = await self.col_threads.find_one({"id": tid}, {"id": 1})
        except Exception as e:
            logger.error(f"Error checking existing thread - id={tid}: {e}", exc_info=True)
            return step["id"]

        if not existing:
            thread_doc: Dict[str, Any] = {
                "id": tid,
                "name": step.get("threadName") or step.get("name") or "Untitled",
                "userIdentifier": step.get("userIdentifier"),
                "chat_profile": step.get("chat_profile"),
                "created_at": now,
                "updated_at": now,
                "metadata": {},
                "tags": [],
            }
            try:
                await self.col_threads.insert_one(thread_doc)
                logger.info(f"Thread created on first user message - id={tid}")
            except Exception as e:
                logger.error(f"Error creating thread - id={tid}: {e}", exc_info=True)
        else:
            # Update last activity only (no upsert, no name override)
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

        if step.get("userIdentifier"):
            step["userIdentifier"] = _safe_lower(step["userIdentifier"])

        if "thread_id" in step and "threadId" not in step:
            step["threadId"] = step.pop("thread_id")

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
        logger.info(f"Getting thread author - thread_id={thread_id}")
        try:
            t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        except Exception as e:
            logger.error(f"Error getting thread author - thread_id={thread_id}: {e}", exc_info=True)
            return None
        author = t.get("userIdentifier") if t else None
        return _safe_lower(author)

    async def delete_thread(self, thread_id: str) -> bool:
        """
        Cascade delete:
          - threads
          - steps
          - elements
          - feedback by threadId
          - feedback by forId (step ids)
          - sessions by threadId (best-effort)
        """
        logger.info(f"Deleting thread - thread_id={thread_id}")

        try:
            # Gather step ids (for feedback deletion by forId)
            step_ids: List[str] = []
            cursor = self.col_steps.find({"threadId": thread_id}, {"id": 1})
            async for s in cursor:
                if s.get("id"):
                    step_ids.append(s["id"])

            # Feedback by threadId
            fb1 = await self.col_feedback.delete_many({"threadId": thread_id})

            # Feedback by forId (if your schema uses forId)
            fb2_count = 0
            if step_ids:
                fb2 = await self.col_feedback.delete_many({"forId": {"$in": step_ids}})
                fb2_count = int(fb2.deleted_count or 0)

            # Elements
            el = await self.col_elements.delete_many({"threadId": thread_id})

            # Sessions (best-effort cleanup if you store them)
            try:
                ss = await self.col_sessions.delete_many({"threadId": thread_id})
                ss_count = int(ss.deleted_count or 0)
            except Exception:
                ss_count = 0

            # Steps (standard + legacy field)
            st = await self.col_steps.delete_many({"threadId": thread_id})
            st_legacy = await self.col_steps.delete_many({"thread_id": thread_id})

            # Thread
            th = await self.col_threads.delete_one({"id": thread_id})

            logger.info(
                "delete_thread cascade: "
                f"thread={th.deleted_count}, steps={st.deleted_count}, legacy_steps={st_legacy.deleted_count}, "
                f"elements={el.deleted_count}, feedback_by_thread={fb1.deleted_count}, feedback_by_forId={fb2_count}, "
                f"sessions={ss_count}"
            )

            return th.deleted_count == 1
        except Exception as e:
            logger.error(f"Error deleting thread - thread_id={thread_id}: {e}", exc_info=True)
            return False

    # ---------------- Pagination + Thread read ----------------

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
        logger.info(f"Listing threads - filters={filters}")

        user_id = getattr(filters, "userId", None)

        if not user_id:
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        user_doc = None
        try:
            if isinstance(user_id, str) and ObjectId.is_valid(user_id):
                user_doc = await self.col_users.find_one({"_id": ObjectId(user_id)})
            elif isinstance(user_id, str) and user_id:
                user_doc = await self.col_users.find_one({"identifier": _safe_lower(user_id)})
        except Exception as e:
            logger.error(f"Error fetching user for list_threads - userId={user_id}: {e}", exc_info=True)
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        if not user_doc:
            logger.warning(f"list_threads: user not found for userId={user_id}")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        query: Dict[str, Any] = {"userIdentifier": user_doc.get("identifier")}

        chat_profile = getattr(filters, "chat_profile", None)
        if chat_profile:
            query["chat_profile"] = chat_profile

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
        logger.info(f"Getting thread - thread_id={thread_id}, user_identifier={user_identifier}")

        query: Dict[str, Any] = {"id": thread_id}
        if user_identifier:
            query["userIdentifier"] = _safe_lower(user_identifier)

        try:
            t = await self.col_threads.find_one(query)
        except Exception as e:
            logger.error(f"Error getting thread - thread_id={thread_id}: {e}", exc_info=True)
            return None

        if not t:
            return None

        steps_query: Dict[str, Any] = {"threadId": thread_id}
        if user_identifier:
            steps_query["userIdentifier"] = _safe_lower(user_identifier)

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
          - upsert is FALSE to prevent re-creating a deleted thread.
          - If thread doesn't exist, we do nothing and return False.
        """
        try:
            patch: Dict[str, Any] = {"updated_at": _now()}

            if name is not None:
                patch["name"] = name
            if metadata is not None:
                patch["metadata"] = metadata
            if tags is not None:
                patch["tags"] = tags
            if chat_profile is not None:
                patch["chat_profile"] = chat_profile

            if user_id is not None:
                patch["user_id"] = user_id

            resolved_user_identifier = user_identifier or _resolve_chainlit_user_identifier()
            if resolved_user_identifier:
                patch["userIdentifier"] = _safe_lower(str(resolved_user_identifier))

            logger.info(f"Updating thread (no upsert) - thread_id={thread_id}, patch={patch}")

            res = await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=False)

            if res.matched_count == 0:
                logger.warning(f"update_thread ignored: thread not found (maybe deleted) - thread_id={thread_id}")
                return False

            return True
        except Exception as e:
            logger.error(f"Error updating thread - thread_id={thread_id}: {e}", exc_info=True)
            return False
