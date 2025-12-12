import datetime
import uuid
from typing import Dict, List, Optional, Any

from bson.objectid import ObjectId
import chainlit as cl
from chainlit.data.base import BaseDataLayer
import motor.motor_asyncio as motor
from dataclasses import asdict
import logging

# Configure logging
logger = logging.getLogger("app")

# Expose 'id' on cl.User using the Mongo _id if present
setattr(cl.User, "id", property(lambda self: self.metadata.get("_id", self.identifier)))


def _now() -> datetime.datetime:
    # IST time (UTC + 5:30)
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


def _encode_value(v):
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


class CLPaginatedResponse:
    def __init__(self, data: List[Dict[str, Any]], total: int, page: int, size: int):
        self.data = data
        self.total = total
        self.page_info = {"page": page, "size": size, "total": total}

    def to_dict(self) -> Dict[str, Any]:
        return {"data": self.data, "total": self.total, "pageInfo": self.page_info}


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
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def build_debug_url(self, thread_id: str) -> str:
        return f"mongodb://debug/thread/{thread_id}"

    # ---------------- Users ----------------

    async def get_user(self, identifier: str):
        identifier = identifier.lower() if identifier else identifier
        doc = await self.col_users.find_one({"identifier": identifier})
        if not doc:
            return None
        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id"))},
        )

    async def create_user(self, user: cl.User):
        identifier = user.identifier.lower()
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
        await self.col_sessions.delete_one({"_id": id})
        return True

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

    # ---------------- Steps ----------------

    async def create_step(self, step_dict: dict):
        if step_dict.get("userIdentifier"):
            step_dict["userIdentifier"] = step_dict["userIdentifier"].lower()

        step_dict.setdefault("id", str(uuid.uuid4()))
        step_dict.setdefault("created_at", _now())

        await self.col_steps.insert_one(step_dict)

        step_type = step_dict.get("type", "")
        if step_type not in ["user_message", "message"]:
            return step_dict["id"]

        tid = step_dict.get("thread_id") or step_dict.get("threadId")
        if not tid:
            return step_dict["id"]

        step_dict["threadId"] = tid
        step_dict.pop("thread_id", None)

        await self.col_steps.update_one(
            {"id": step_dict["id"]}, {"$set": {"threadId": tid}}
        )

        patch = {
            "$setOnInsert": {"id": tid, "created_at": _now()},
            "$set": {"updated_at": _now()},
        }

        if step_dict.get("user_id"):
            patch["$set"]["user_id"] = step_dict["user_id"]

        user_identifier = step_dict.get("userIdentifier")
        if not user_identifier:
            try:
                if hasattr(cl, "context") and cl.context:
                    u = cl.user_session.get("user")
                    user_identifier = getattr(u, "identifier", None)
            except Exception:
                pass

        if user_identifier:
            patch["$set"]["userIdentifier"] = user_identifier.lower()

        if step_dict.get("chat_profile"):
            patch["$set"]["chat_profile"] = step_dict["chat_profile"]

        await self.col_threads.update_one({"id": tid}, patch, upsert=True)
        return step_dict["id"]

    async def update_step(self, step_dict: dict):
        await self.col_steps.update_one({"id": step_dict["id"]}, {"$set": step_dict})

    async def delete_step(self, step_id: str):
        await self.col_steps.delete_one({"id": step_id})

    # ---------------- Threads ----------------

    async def get_thread_author(self, thread_id: str):
        t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        return t.get("userIdentifier").lower() if t else None

    async def delete_thread(self, thread_id: str):
        await self.col_threads.delete_one({"id": thread_id})
        await self.col_steps.delete_many({"threadId": thread_id})
        await self.col_steps.delete_many({"thread_id": thread_id})
        await self.col_elements.delete_many({"threadId": thread_id})

        step_ids = []

        async for s in self.col_steps.find({"threadId": thread_id}, {"id": 1}):
            step_ids.append(s["id"])

        async for s in self.col_steps.find({"thread_id": thread_id}, {"id": 1}):
            step_ids.append(s["id"])

        await self.col_feedback.delete_many({"threadId": thread_id})

        if step_ids:
            await self.col_feedback.delete_many({"forId": {"$in": step_ids}})

        return True

    # ---------------- Pagination ----------------

    def _calculate_pagination(self, pagination):
        skip, limit = 0, 20
        if not pagination:
            return skip, limit

        if getattr(pagination, "offset", None) is not None:
            skip = int(pagination.offset)

        if getattr(pagination, "first", None) is not None:
            limit = int(pagination.first)

        if getattr(pagination, "page", None) and getattr(pagination, "size", None):
            page = int(pagination.page)
            size = int(pagination.size)
            skip = (page - 1) * size
            limit = size

        if getattr(pagination, "limit", None) is not None:
            limit = int(pagination.limit)

        return skip, limit

    def _prepare_thread_item(self, it: Dict[str, Any]):
        if "id" not in it and "_id" in it:
            it["id"] = str(it["_id"])
        it.setdefault("name", "Untitled")
        it.setdefault("created_at", _now())
        it.setdefault("updated_at", _now())
        it["createdAt"] = _encode_value(it["created_at"])
        it["updatedAt"] = _encode_value(it["updated_at"])
        return _encode_doc(it)

    async def list_threads(self, pagination, filters):
        user_id = filters.__getattribute__("userId")
        doc = await self.col_users.find_one({"_id": ObjectId(user_id)})
        if not doc:
            return CLPaginatedResponse([], 0, 1, 0)

        query = {"userIdentifier": doc["identifier"]}

        if getattr(filters, "chat_profile", None):
            query["chat_profile"] = filters.chat_profile

        skip, limit = self._calculate_pagination(pagination)
        total = await self.col_threads.count_documents(query)

        cursor = (
            self.col_threads.find(query)
            .sort("updated_at", -1)
            .skip(skip)
            .limit(limit)
        )

        items = [self._prepare_thread_item(it) async for it in cursor]
        page = (skip // limit + 1) if limit else 1

        return CLPaginatedResponse(items, total, page, limit)

    async def get_thread(self, thread_id: str, user_identifier: Optional[str] = None):
        query = {"id": thread_id}
        if user_identifier:
            query["userIdentifier"] = user_identifier

        t = await self.col_threads.find_one(query)
        if not t:
            return None

        steps_query = {"threadId": thread_id}
        if user_identifier:
            steps_query["userIdentifier"] = user_identifier

        steps = [_encode_doc(s) async for s in self.col_steps.find(steps_query)]
        t["steps"] = steps

        t.setdefault("created_at", _now())
        t.setdefault("updated_at", _now())
        t["createdAt"] = _encode_value(t["created_at"])
        t["updatedAt"] = _encode_value(t["updated_at"])

        if "id" not in t and "_id" in t:
            t["id"] = str(t["_id"])

        return _encode_doc(t)

    async def update_thread(
        self,
        thread_id: str,
        name: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
        user_identifier: Optional[str] = None,
        chat_profile: Optional[str] = None,
    ):
        try:
            patch = {"updated_at": _now()}

            if name is not None:
                patch["name"] = name
            if user_id is not None:
                patch["user_id"] = user_id
            if metadata is not None:
                patch["metadata"] = metadata
            if tags is not None:
                patch["tags"] = tags
            if user_identifier is not None:
                patch["userIdentifier"] = user_identifier
            if chat_profile is not None:
                patch["chat_profile"] = chat_profile

            await self.col_threads.update_one(
                {"id": thread_id}, {"$set": patch}, upsert=True
            )
            return True
        except Exception:
            logger.exception("Error updating thread")
            return False

