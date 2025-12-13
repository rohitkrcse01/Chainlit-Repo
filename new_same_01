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
    # Return current time in Indian time zone (UTC+5:30)
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
        logger.info(f"MongoDB data layer initialized - database={db_name}")

    async def close(self):
        """Close the MongoDB connection"""
        if hasattr(self, "client") and self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def build_debug_url(self, thread_id: str) -> str:
        return f"mongodb://debug/thread/{thread_id}"

    # ---------------- Users ----------------
    async def get_user(self, identifier: str) -> Optional[cl.User]:
        identifier = identifier.lower() if identifier else identifier
        logger.info(f"Getting user - identifier={identifier}")
        try:
            doc = await self.col_users.find_one({"identifier": identifier})
        except Exception as e:
            logger.error(f"Error fetching user - identifier={identifier}: {e}", exc_info=True)
            return None
        if not doc:
            logger.debug(f"User not found - identifier={identifier}")
            return None
        logger.info(f"User found - identifier={identifier}")
        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id")), **({"created_at": _encode_value(doc["created_at"])} if doc.get("created_at") else {})},
        )

    async def create_user(self, user: cl.User) -> Optional[cl.User]:
        identifier = user.identifier.lower() if user.identifier else user.identifier
        logger.info(f"Creating user - identifier={identifier}")
        payload = {
            "identifier": identifier,
            "metadata": user.metadata or {},
            "created_at": _now(),
        }
        try:
            # Only insert if new; if exists, do not overwrite metadata or created_at
            await self.col_users.update_one(
                {"identifier": identifier},
                {"$setOnInsert": payload},
                upsert=True,
            )
            doc = await self.col_users.find_one({"identifier": identifier})
        except Exception as e:
            logger.error(f"Error creating user - identifier={identifier}: {e}", exc_info=True)
            return None
        logger.info(f"User created/retrieved - identifier={identifier}")
        return cl.User(
            identifier=doc["identifier"],
            metadata={**(user.metadata or {}), "_id": str(doc.get("_id")), **({"created_at": _encode_value(doc["created_at"])} if doc.get("created_at") else {})},
        )

    # ---------------- Feedback ----------------
    async def upsert_feedback(self, feedback) -> Optional[str]:
        logger.info(f"Upserting feedback - feedback={feedback}")
        fid = getattr(feedback, "id", None) or str(uuid.uuid4())
        doc = asdict(feedback)
        doc["id"] = fid
        doc["updated_at"] = _now()
        try:
            await self.col_feedback.update_one(
                {"id": fid},
                {"$set": doc, "$setOnInsert": {"created_at": _now()}},
                upsert=True,
            )
        except Exception as e:
            logger.error(f"Error upserting feedback - id={fid}: {e}", exc_info=True)
            return None
        logger.info(f"Feedback upserted - id={fid}")
        return fid

    async def delete_feedback(self, feedback_id: str) -> bool:
        logger.info(f"Deleting feedback - feedback_id={feedback_id}")
        try:
            res = await self.col_feedback.delete_one({"id": feedback_id})
        except Exception as e:
            logger.error(f"Error deleting feedback - feedback_id={feedback_id}: {e}", exc_info=True)
            return False
        logger.info(f"Feedback deleted - feedback_id={feedback_id}, deleted={res.deleted_count == 1}")
        return res.deleted_count == 1

    # ---------------- Elements ----------------
    async def create_element(self, element_dict: dict) -> Optional[str]:
        logger.info(f"Creating element - element_dict={element_dict}")
        if "id" not in element_dict:
            element_dict["id"] = str(uuid.uuid4())
        if not element_dict.get("created_at"):
            element_dict["created_at"] = _now()
        # Normalize thread field naming
        if element_dict.get("thread_id") and not element_dict.get("threadId"):
            element_dict["threadId"] = element_dict["thread_id"]
            del element_dict["thread_id"]
        try:
            await self.col_elements.insert_one(element_dict)
        except Exception as e:
            logger.error(f"Error creating element - id={element_dict.get('id')}: {e}", exc_info=True)
            return None
        logger.info(f"Element created - id={element_dict['id']}")
        return element_dict["id"]

    async def get_element(self, thread_id: str, element_id: str) -> Optional[Dict[str, Any]]:
        logger.info(f"Getting element - thread_id={thread_id}, element_id={element_id}")
        try:
            doc = await self.col_elements.find_one({"id": element_id, "threadId": thread_id})
        except Exception as e:
            logger.error(f"Error fetching element - element_id={element_id}, thread_id={thread_id}: {e}", exc_info=True)
            return None
        if not doc:
            logger.debug(f"Element not found - element_id={element_id}, thread_id={thread_id}")
            return None
        logger.info(f"Element found - element_id={element_id}, thread_id={thread_id}")
        # Convert ObjectId and datetimes to serializable forms
        if "id" not in doc and doc.get("_id"):
            doc["id"] = str(doc["_id"])
        return _encode_doc(doc)

    async def delete_element(self, element_id: str) -> bool:
        logger.info(f"Deleting element - element_id={element_id}")
        try:
            res = await self.col_elements.delete_one({"id": element_id})
        except Exception as e:
            logger.error(f"Error deleting element - element_id={element_id}: {e}", exc_info=True)
            return False
        logger.info(f"Element deleted - element_id={element_id}, deleted={res.deleted_count == 1}")
        return res.deleted_count == 1

    # ---------------- Steps (messages) ----------------
    async def create_step(self, step_dict: dict) -> Optional[str]:
        logger.info(f"Creating step - step_dict={step_dict}")
        # Normalize user identifier to lowercase if present
        if step_dict.get("userIdentifier"):
            step_dict["userIdentifier"] = step_dict["userIdentifier"].lower()
        # Ensure an id and created_at for the step
        if "id" not in step_dict:
            step_dict["id"] = str(uuid.uuid4())
        if not step_dict.get("created_at"):
            step_dict["created_at"] = _now()
        # Normalize thread field naming
        if step_dict.get("thread_id") and not step_dict.get("threadId"):
            step_dict["threadId"] = step_dict["thread_id"]
            del step_dict["thread_id"]
        try:
            await self.col_steps.insert_one(step_dict)
        except Exception as e:
            logger.error(f"Error inserting step - id={step_dict.get('id')}: {e}", exc_info=True)
            return None
        logger.info(f"Step created - id={step_dict['id']}, type={step_dict.get('type')}")
        # Only create thread when a user starts a new chat (first user message)
        step_type = step_dict.get("type", "")
        is_user_message = step_type in ["user_message", "message"]
        if not is_user_message:
            logger.info(f"Thread creation skipped for non-user message - type={step_type}")
            return step_dict["id"]
        tid = step_dict.get("threadId")
        if not tid:
            # No thread id provided, cannot create thread
            logger.warning(f"No thread id provided for user message - step_id={step_dict['id']}")
            return step_dict["id"]
        # Prepare thread upsert payload
        patch = {
            "$setOnInsert": {"id": tid, "created_at": _now()},
            "$set": {"updated_at": _now()},
        }
        # Persist user_id if provided (legacy support)
        if step_dict.get("user_id"):
            patch["$set"]["user_id"] = step_dict["user_id"]
        # Persist userIdentifier for the thread
        user_identifier = step_dict.get("userIdentifier")
        if user_identifier:
            patch["$set"]["userIdentifier"] = str(user_identifier).lower()
        # Persist chat_profile if present
        if step_dict.get("chat_profile"):
            patch["$set"]["chat_profile"] = step_dict["chat_profile"]
        try:
            await self.col_threads.update_one({"id": tid}, patch, upsert=True)
        except Exception as e:
            logger.error(f"Error creating/updating thread - thread_id={tid}: {e}", exc_info=True)
            # If thread upsert failed, consider rolling back inserted step
            return step_dict["id"]
        logger.info(f"Thread created/updated for user message - tid={tid}")
        return step_dict["id"]

    async def update_step(self, step_dict: dict) -> bool:
        logger.info(f"Updating step - id={step_dict.get('id')}")
        # Update the timestamp for modification
        step_dict["updated_at"] = _now()
        try:
            res = await self.col_steps.update_one({"id": step_dict["id"]}, {"$set": step_dict})
        except Exception as e:
            logger.error(f"Error updating step - id={step_dict.get('id')}: {e}", exc_info=True)
            return False
        success = res.matched_count > 0
        if success:
            logger.info(f"Step updated - id={step_dict['id']}")
        else:
            logger.warning(f"Step not found or not updated - id={step_dict.get('id')}")
        return success

    async def delete_step(self, step_id: str) -> bool:
        logger.info(f"Deleting step - step_id={step_id}")
        try:
            res = await self.col_steps.delete_one({"id": step_id})
        except Exception as e:
            logger.error(f"Error deleting step - step_id={step_id}: {e}", exc_info=True)
            return False
        logger.info(f"Step deleted - step_id={step_id}, deleted={res.deleted_count == 1}")
        return res.deleted_count == 1

    # ---------------- Threads ----------------
    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        logger.info(f"Getting thread author - thread_id={thread_id}")
        try:
            t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        except Exception as e:
            logger.error(f"Error fetching thread author - thread_id={thread_id}: {e}", exc_info=True)
            return None
        author = t.get("userIdentifier") if t else None
        logger.info(f"Thread author retrieved - thread_id={thread_id}, author={author}")
        return author.lower() if author else None

    async def delete_thread(self, thread_id: str) -> bool:
        logger.info(f"Deleting thread - thread_id={thread_id}")
        delete_success = True
        step_ids: List[str] = []
        # Delete the thread document
        try:
            thread_res = await self.col_threads.delete_one({"id": thread_id})
            if thread_res.deleted_count != 1:
                logger.warning(f"Thread not found or already deleted - thread_id={thread_id}")
        except Exception as e:
            logger.error(f"Error deleting thread document - thread_id={thread_id}: {e}", exc_info=True)
            delete_success = False
        # Delete all steps associated with this thread
        try:
            steps_cursor = self.col_steps.find({"threadId": thread_id}, {"id": 1})
            async for st in steps_cursor:
                sid = st.get("id")
                if sid:
                    step_ids.append(sid)
            steps_res = await self.col_steps.delete_many({"threadId": thread_id})
            logger.info(f"Deleted steps (threadId) - thread_id={thread_id}, count={steps_res.deleted_count}")
            # Also delete any steps with legacy thread_id field (if any)
            steps_res_legacy = await self.col_steps.delete_many({"thread_id": thread_id})
            if steps_res_legacy.deleted_count > 0:
                logger.info(f"Deleted legacy steps (thread_id) - thread_id={thread_id}, count={steps_res_legacy.deleted_count}")
        except Exception as e:
            logger.error(f"Error deleting steps - thread_id={thread_id}: {e}", exc_info=True)
            delete_success = False
        # Delete all elements associated with this thread
        try:
            elements_res = await self.col_elements.delete_many({"threadId": thread_id})
            logger.info(f"Deleted elements - thread_id={thread_id}, count={elements_res.deleted_count}")
        except Exception as e:
            logger.error(f"Error deleting elements - thread_id={thread_id}: {e}", exc_info=True)
            delete_success = False
        # Delete all feedback associated with this thread or its steps
        try:
            feedback_res_thread = await self.col_feedback.delete_many({"threadId": thread_id})
            logger.info(f"Deleted feedback by threadId - thread_id={thread_id}, count={feedback_res_thread.deleted_count}")
            if step_ids:
                feedback_res_steps = await self.col_feedback.delete_many({"forId": {"$in": step_ids}})
                logger.info(f"Deleted feedback by step ids - thread_id={thread_id}, count={feedback_res_steps.deleted_count}")
        except Exception as e:
            logger.error(f"Error deleting feedback - thread_id={thread_id}: {e}", exc_info=True)
            delete_success = False
        return delete_success

    async def list_threads(self, pagination, filters) -> CLPaginatedResponse:
        logger.info(f"Listing threads - pagination={pagination}, filters={filters}")
        # Determine user identifier from filters.userId (expected to be Mongo ObjectId string)
        user_id = getattr(filters, "userId", None)
        if not user_id:
            logger.warning("No userId provided in filters")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        try:
            doc = await self.col_users.find_one({"_id": ObjectId(user_id)})
        except Exception as e:
            logger.error(f"Error finding user by ID - userId={user_id}: {e}", exc_info=True)
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        if not doc:
            logger.warning(f"User not found for userId={user_id}")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        query = {"userIdentifier": doc.get("identifier")}
        # Add chat_profile filter if available
        if hasattr(filters, "chat_profile") and filters.chat_profile:
            query["chat_profile"] = filters.chat_profile
        # Pagination calculations
        skip = 0
        limit = 20
        if pagination:
            if getattr(pagination, "offset", None) is not None:
                skip = int(pagination.offset) or 0
            if getattr(pagination, "first", None) is not None:
                limit = int(pagination.first) or limit
            page_num = getattr(pagination, "page", None)
            size_num = getattr(pagination, "size", None)
            if page_num is not None and size_num is not None:
                page_num = int(page_num) or 1
                size_num = int(size_num) or limit
                skip = (page_num - 1) * size_num
                limit = size_num
            if getattr(pagination, "limit", None) is not None:
                limit = int(pagination.limit) or limit
        try:
            total = await self.col_threads.count_documents(query)
            cursor = self.col_threads.find(query).sort("updated_at", -1).limit(limit).skip(skip)
            raw_items = await cursor.to_list(length=limit)
        except Exception as e:
            logger.error(f"Error listing threads - user={doc.get('identifier')}: {e}", exc_info=True)
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        logger.info(f"Listed threads - user={doc.get('identifier')}, total={total}, returned={len(raw_items)}")
        items = []
        for it in raw_items:
            # Ensure id and timestamps are properly formatted
            if "id" not in it and it.get("_id"):
                it["id"] = str(it["_id"])
            it.setdefault("name", "Untitled")
            if "created_at" not in it:
                it["created_at"] = _now()
            if "updated_at" not in it:
                it["updated_at"] = _now()
            it["createdAt"] = _encode_value(it["created_at"])
            it["updatedAt"] = _encode_value(it["updated_at"])
            items.append(_encode_doc(it))
        # Calculate current page number
        page_number = (skip // limit + 1) if limit else 1
        return CLPaginatedResponse(data=items, total=total, page=page_number, size=limit or len(items))

    async def get_thread(self, thread_id: str, user_identifier: Optional[str] = None) -> Optional[Dict[str, Any]]:
        logger.info(f"Getting thread - thread_id={thread_id}, user_identifier={user_identifier}")
        # Fetch thread by id (and user if provided for security)
        thread_query: Dict[str, Any] = {"id": thread_id}
        if user_identifier:
            thread_query["userIdentifier"] = user_identifier
        try:
            t = await self.col_threads.find_one(thread_query)
        except Exception as e:
            logger.error(f"Error fetching thread - thread_id={thread_id}: {e}", exc_info=True)
            return None
        if not t:
            logger.debug(f"Thread not found - thread_id={thread_id}, user={user_identifier}")
            return None
        # Fetch all steps for this thread (filter by user if provided)
        steps_query: Dict[str, Any] = {"threadId": thread_id}
        if user_identifier:
            steps_query["userIdentifier"] = user_identifier
        try:
            steps_cursor = self.col_steps.find(steps_query).sort("created_at", 1)
            steps_raw = await steps_cursor.to_list(length=None)
            for st in steps_raw:
                if "id" not in st and st.get("_id"):
                    st["id"] = str(st["_id"])
                if st.get("created_at"):
                    st["createdAt"] = _encode_value(st["created_at"])
                if st.get("updated_at"):
                    st["updatedAt"] = _encode_value(st["updated_at"])
            steps = [_encode_doc(st) for st in steps_raw]
        except Exception as e:
            logger.error(f"Error fetching steps for thread - thread_id={thread_id}: {e}", exc_info=True)
            return None
        t["steps"] = steps
        logger.info(f"Thread retrieved - thread_id={thread_id}, steps={len(steps)}")
        # Ensure consistent thread fields
        if "id" not in t and t.get("_id"):
            t["id"] = str(t["_id"])
        if "created_at" not in t:
            t["created_at"] = _now()
        if "updated_at" not in t:
            t["updated_at"] = _now()
        t["createdAt"] = _encode_value(t["created_at"])
        t["updatedAt"] = _encode_value(t["updated_at"])
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
    ) -> bool:
        try:
            logger.info(f"Updating thread - thread_id={thread_id}, name={name}")
            patch: Dict[str, Any] = {"updated_at": _now()}
            if name is not None:
                patch["name"] = name
            if user_id is not None:
                patch["user_id"] = user_id
            else:
                try:
                    if hasattr(cl, "context") and cl.context:
                        current_user = cl.user_session.get("user")
                        if current_user and hasattr(current_user, "identifier"):
                            patch["user_id"] = current_user.identifier
                except Exception as user_err:
                    logger.debug(f"Could not get user_id from session for thread {thread_id}: {user_err}")
            if metadata is not None:
                patch["metadata"] = metadata
            if tags is not None:
                patch["tags"] = tags
            if user_identifier is not None:
                patch["userIdentifier"] = user_identifier
            else:
                try:
                    if hasattr(cl, "context") and cl.context:
                        current_user = cl.user_session.get("user")
                        if current_user and hasattr(current_user, "identifier"):
                            patch["userIdentifier"] = current_user.identifier
                except Exception as user_err:
                    logger.debug(f"Could not get userIdentifier from session for thread {thread_id}: {user_err}")
            if chat_profile is not None:
                patch["chat_profile"] = chat_profile
            logger.info(f"Thread update payload: {patch}")
            await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=True)
            logger.info(f"Thread updated successfully - thread_id={thread_id}, name={name}")
            return True
        except Exception as e:
            logger.error(f"Error updating thread - thread_id={thread_id}: {e}", exc_info=True)
            return False

    async def delete_user_session(self, id: str) -> bool:
        logger.info(f"Deleting user session - id={id}")
        # No session persistence is used in this data layer
        logger.info(f"No session data to delete for session id={id}")
        return True
