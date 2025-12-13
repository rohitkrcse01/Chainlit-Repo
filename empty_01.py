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
        """Close the MongoDB connection."""
        if hasattr(self, "client") and self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def build_debug_url(self, thread_id: str) -> str:
        return f"mongodb://debug/thread/{thread_id}"

    # ---------------- Users ----------------
    async def get_user(self, identifier: str) -> Optional[cl.User]:
        """Fetches a user by identifier."""
        identifier = identifier.lower() if identifier else identifier
        logger.info(f"Getting user - identifier={identifier}")
        try:
            doc = await self.col_users.find_one({"identifier": identifier})
        except Exception as e:
            logger.error(f"Error getting user - identifier={identifier}: {e}", exc_info=True)
            return None
        if not doc:
            logger.debug(f"User not found - identifier={identifier}")
            return None
        logger.info(f"User found - identifier={identifier}")
        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id"))},
        )

    async def create_user(self, user: cl.User) -> Optional[cl.User]:
        """Creates a new user or retrieves existing user if already present."""
        identifier = user.identifier.lower() if user.identifier else user.identifier
        logger.info(f"Creating user - identifier={identifier}")
        payload = {
            "identifier": identifier,
            "metadata": user.metadata or {},
            "created_at": _now(),
        }
        try:
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
            identifier=identifier,
            metadata={**(user.metadata or {}), "_id": str(doc.get("_id"))},
        )

    async def update_user(self, identifier: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Updates an existing user's metadata."""
        logger.info(f"Updating user - identifier={identifier}")
        update_fields: Dict[str, Any] = {}
        if metadata is not None:
            update_fields["metadata"] = metadata
        update_fields["updated_at"] = _now()
        try:
            result = await self.col_users.update_one(
                {"identifier": identifier}, {"$set": update_fields}
            )
            if result.matched_count == 0:
                logger.warning(f"User not found - identifier={identifier}")
                return False
            logger.info(f"User update success - identifier={identifier}, modified_count={result.modified_count}")
            return True
        except Exception as e:
            logger.error(f"Error updating user - identifier={identifier}: {e}", exc_info=True)
            return False

    async def delete_user(self, identifier: str) -> bool:
        """Deletes a user and all associated data (threads, steps, elements, feedback)."""
        logger.info(f"Deleting user - identifier={identifier}")
        try:
            # Find all thread IDs for this user
            threads_cursor = self.col_threads.find({"userIdentifier": identifier}, {"id": 1})
            thread_ids = [thread.get("id") async for thread in threads_cursor]
            if thread_ids:
                # Delete all steps, elements, feedback for these threads
                await self.col_steps.delete_many({"threadId": {"$in": thread_ids}})
                await self.col_elements.delete_many({"threadId": {"$in": thread_ids}})
                await self.col_feedback.delete_many({"threadId": {"$in": thread_ids}})
                await self.col_threads.delete_many({"id": {"$in": thread_ids}})
                logger.info(f"Deleted threads and related data for user {identifier} - threads_count={len(thread_ids)}")
            result = await self.col_users.delete_one({"identifier": identifier})
            if result.deleted_count == 0:
                logger.warning(f"User not found - identifier={identifier}")
                return False
            logger.info(f"User deleted - identifier={identifier}")
            return True
        except Exception as e:
            logger.error(f"Error deleting user - identifier={identifier}: {e}", exc_info=True)
            return False

    async def delete_user_session(self, id: str) -> bool:
        """Deletes a user session (no-op, as session data is not persisted)."""
        logger.info(f"Deleting user session - id={id}")
        # No session persistence; nothing to delete.
        return True

    # ---------------- Feedback ----------------
    async def get_feedback(self, feedback_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a feedback by its identifier."""
        logger.info(f"Getting feedback - id={feedback_id}")
        try:
            doc = await self.col_feedback.find_one({"id": feedback_id})
        except Exception as e:
            logger.error(f"Error getting feedback - id={feedback_id}: {e}", exc_info=True)
            return None
        if not doc:
            logger.debug(f"Feedback not found - id={feedback_id}")
            return None
        return _encode_doc(doc)

    async def upsert_feedback(self, feedback) -> str:
        """Inserts or updates a feedback entry."""
        logger.info(f"Upserting feedback - feedback={feedback}")
        fid = getattr(feedback, "id", None) or str(uuid.uuid4())
        doc = asdict(feedback)
        doc["id"] = fid
        now = _now()
        if "created_at" not in doc:
            doc["created_at"] = now
        doc["updated_at"] = now
        try:
            await self.col_feedback.update_one({"id": fid}, {"$set": doc}, upsert=True)
        except Exception as e:
            logger.error(f"Error upserting feedback - id={fid}: {e}", exc_info=True)
            return fid  # return the id even if error occurred
        logger.info(f"Feedback upserted - id={fid}")
        return fid

    async def delete_feedback(self, feedback_id: str) -> bool:
        """Deletes a feedback by its identifier."""
        logger.info(f"Deleting feedback - feedback_id={feedback_id}")
        try:
            result = await self.col_feedback.delete_one({"id": feedback_id})
            if result.deleted_count == 0:
                logger.warning(f"Feedback not found - feedback_id={feedback_id}")
                return False
            logger.info(f"Feedback deleted - feedback_id={feedback_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting feedback - feedback_id={feedback_id}: {e}", exc_info=True)
            return False

    # ---------------- Elements ----------------
    async def create_element(self, element_dict: dict) -> Optional[str]:
        """Adds a new element to the database."""
        logger.info(f"Creating element - element_dict={element_dict}")
        if "id" not in element_dict:
            element_dict["id"] = str(uuid.uuid4())
        now = _now()
        if not element_dict.get("created_at"):
            element_dict["created_at"] = now
        if "updated_at" not in element_dict:
            element_dict["updated_at"] = now
        # Normalize thread field naming
        if element_dict.get("thread_id") and not element_dict.get("threadId"):
            element_dict["threadId"] = element_dict.pop("thread_id")
        try:
            await self.col_elements.insert_one(element_dict)
        except Exception as e:
            logger.error(f"Error creating element - id={element_dict.get('id')}: {e}", exc_info=True)
            return None
        logger.info(f"Element created - id={element_dict['id']}")
        return element_dict["id"]

    async def get_element(self, thread_id: str, element_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves an element by thread_id and element_id."""
        logger.info(f"Getting element - thread_id={thread_id}, element_id={element_id}")
        try:
            doc = await self.col_elements.find_one({"id": element_id, "threadId": thread_id})
        except Exception as e:
            logger.error(f"Error getting element - thread_id={thread_id}, element_id={element_id}: {e}", exc_info=True)
            return None
        if not doc:
            logger.debug(f"Element not found - id={element_id}, thread_id={thread_id}")
            return None
        return _encode_doc(doc)

    async def update_element(self, element_id: str, updates: Dict[str, Any]) -> bool:
        """Updates an existing element's fields."""
        logger.info(f"Updating element - id={element_id}")
        if not updates:
            logger.debug("No updates provided for element")
            return False
        # Do not allow changing id or threadId
        updates.pop("id", None)
        updates.pop("threadId", None)
        updates.pop("thread_id", None)
        updates["updated_at"] = _now()
        try:
            result = await self.col_elements.update_one({"id": element_id}, {"$set": updates})
            if result.matched_count == 0:
                logger.warning(f"Element not found - id={element_id}")
                return False
            logger.info(f"Element update success - id={element_id}, modified_count={result.modified_count}")
            return True
        except Exception as e:
            logger.error(f"Error updating element - id={element_id}: {e}", exc_info=True)
            return False

    async def delete_element(self, element_id: str) -> bool:
        """Deletes an element by its identifier."""
        logger.info(f"Deleting element - element_id={element_id}")
        try:
            result = await self.col_elements.delete_one({"id": element_id})
            if result.deleted_count == 0:
                logger.warning(f"Element not found - element_id={element_id}")
                return False
            logger.info(f"Element deleted - element_id={element_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting element - element_id={element_id}: {e}", exc_info=True)
            return False

    # ---------------- Steps (messages) ----------------
    async def get_step(self, step_id: str) -> Optional[Dict[str, Any]]:
        """Fetches a step (message) by its identifier."""
        logger.info(f"Getting step - step_id={step_id}")
        try:
            doc = await self.col_steps.find_one({"id": step_id})
        except Exception as e:
            logger.error(f"Error getting step - step_id={step_id}: {e}", exc_info=True)
            return None
        if not doc:
            logger.debug(f"Step not found - step_id={step_id}")
            return None
        return _encode_doc(doc)

    async def create_step(self, step_dict: Dict[str, Any]) -> Optional[str]:
        """Creates a new step (message) in a thread."""
        logger.info(f"Creating step - step_dict={step_dict}")
        # Normalize userIdentifier to lowercase if present
        if step_dict.get("userIdentifier"):
            step_dict["userIdentifier"] = step_dict["userIdentifier"].lower()
        if "id" not in step_dict:
            step_dict["id"] = str(uuid.uuid4())
        if not step_dict.get("created_at"):
            step_dict["created_at"] = _now()
        try:
            await self.col_steps.insert_one(step_dict)
            logger.info(f"Step created - id={step_dict['id']}, type={step_dict.get('type')}")
            # Only create threads when a user sends the first message
            step_type = step_dict.get("type", "")
            is_user_message = step_type in ["user_message", "message"]
            if is_user_message:
                tid = step_dict.get("thread_id") or step_dict.get("threadId")
                if not tid:
                    # No thread id provided, cannot create thread
                    logger.warning("Thread ID missing in user message, thread not created")
                    return step_dict["id"]
                # Prepare thread upsert fields
                patch = {
                    "$setOnInsert": {"id": tid, "created_at": _now()},
                    "$set": {"updated_at": _now()},
                }
                # Persist user_id if provided
                if step_dict.get("user_id"):
                    patch["$set"]["user_id"] = step_dict["user_id"]
                # Persist userIdentifier if available
                user_identifier = step_dict.get("userIdentifier")
                if user_identifier:
                    patch["$set"]["userIdentifier"] = str(user_identifier).lower()
                # Persist chat_profile if present
                if step_dict.get("chat_profile"):
                    patch["$set"]["chat_profile"] = step_dict["chat_profile"]
                await self.col_threads.update_one({"id": tid}, patch, upsert=True)
                logger.info(f"Thread created/updated for user message - thread_id={tid}")
            return step_dict["id"]
        except Exception as e:
            logger.error(f"Error creating step - step_id={step_dict.get('id')}: {e}", exc_info=True)
            return None

    async def update_step(self, step_dict: Dict[str, Any]) -> bool:
        """Updates an existing step."""
        logger.info(f"Updating step - id={step_dict.get('id')}")
        if "id" not in step_dict:
            logger.error("Step update failed: 'id' not provided in step_dict")
            return False
        try:
            result = await self.col_steps.update_one({"id": step_dict["id"]}, {"$set": step_dict})
            if result.matched_count == 0:
                logger.warning(f"Step not found - id={step_dict['id']}")
                return False
            logger.info(f"Step updated - id={step_dict['id']}, modified_count={result.modified_count}")
            return True
        except Exception as e:
            logger.error(f"Error updating step - id={step_dict.get('id')}: {e}", exc_info=True)
            return False

    async def delete_step(self, step_id: str) -> bool:
        """Deletes a step by its identifier."""
        logger.info(f"Deleting step - step_id={step_id}")
        try:
            result = await self.col_steps.delete_one({"id": step_id})
            if result.deleted_count == 0:
                logger.warning(f"Step not found - step_id={step_id}")
                return False
            logger.info(f"Step deleted - step_id={step_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting step - step_id={step_id}: {e}", exc_info=True)
            return False

    # ---------------- Threads ----------------
    async def create_thread(
        self,
        user_identifier: str,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        chat_profile: Optional[str] = None,
    ) -> Optional[str]:
        """Creates a new thread for the given user."""
        logger.info(f"Creating thread - user_identifier={user_identifier}, name={name}")
        thread_id = str(uuid.uuid4())
        doc: Dict[str, Any] = {
            "id": thread_id,
            "userIdentifier": user_identifier,
            "created_at": _now(),
            "updated_at": _now(),
        }
        if name is not None:
            doc["name"] = name
        if metadata is not None:
            doc["metadata"] = metadata
        if tags is not None:
            doc["tags"] = tags
        if chat_profile is not None:
            doc["chat_profile"] = chat_profile
        try:
            await self.col_threads.insert_one(doc)
        except Exception as e:
            logger.error(f"Error creating thread - user_identifier={user_identifier}: {e}", exc_info=True)
            return None
        logger.info(f"Thread created - id={thread_id}")
        return thread_id

    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        """Fetches the author (user identifier) of a given thread."""
        logger.info(f"Getting thread author - thread_id={thread_id}")
        try:
            thread = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        except Exception as e:
            logger.error(f"Error getting thread author - thread_id={thread_id}: {e}", exc_info=True)
            return None
        if not thread:
            logger.debug(f"Thread not found for author fetch - thread_id={thread_id}")
            return None
        author = thread.get("userIdentifier")
        logger.info(f"Thread author retrieved - thread_id={thread_id}, author={author}")
        return author.lower() if author else None

    async def delete_thread(self, thread_id: str) -> bool:
        """Deletes a thread and all its associated steps, elements, and feedback."""
        logger.info(f"Deleting thread - thread_id={thread_id}")
        try:
            # Delete the thread document
            thread_result = await self.col_threads.delete_one({"id": thread_id})
            if thread_result.deleted_count == 0:
                logger.warning(f"Thread not found - thread_id={thread_id}")
            else:
                logger.info(f"Thread deleted - thread_id={thread_id}")
            # Delete all steps associated with this thread
            steps_result = await self.col_steps.delete_many({"threadId": thread_id})
            if steps_result.deleted_count:
                logger.info(f"Deleted steps - thread_id={thread_id}, count={steps_result.deleted_count}")
            # Delete any legacy steps with 'thread_id' field (for backward compatibility)
            legacy_steps_result = await self.col_steps.delete_many({"thread_id": thread_id})
            if legacy_steps_result.deleted_count:
                logger.info(f"Deleted legacy steps - thread_id={thread_id}, count={legacy_steps_result.deleted_count}")
            # Delete all elements associated with this thread
            elements_result = await self.col_elements.delete_many({"threadId": thread_id})
            if elements_result.deleted_count:
                logger.info(f"Deleted elements - thread_id={thread_id}, count={elements_result.deleted_count}")
            # Delete all feedback associated with this thread
            feedback_result = await self.col_feedback.delete_many({"threadId": thread_id})
            if feedback_result.deleted_count:
                logger.info(f"Deleted feedback - thread_id={thread_id}, count={feedback_result.deleted_count}")
            return True
        except Exception as e:
            logger.error(f"Error deleting thread - thread_id={thread_id}: {e}", exc_info=True)
            return False

    async def list_threads(self, pagination, filters) -> CLPaginatedResponse:
        """Lists threads for a given user (and optional chat profile) with pagination."""
        logger.info(f"Listing threads - pagination={pagination}, filters={filters}")
        # Retrieve user identifier from provided userId filter
        user_id = getattr(filters, "userId", None)
        if not user_id:
            logger.warning("No userId provided in filters for list_threads")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        try:
            user_doc = await self.col_users.find_one({"_id": ObjectId(user_id)})
        except Exception as e:
            logger.error(f"Error listing threads - userId={user_id}: {e}", exc_info=True)
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        if not user_doc:
            logger.warning(f"User not found for userId={user_id}")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        query: Dict[str, Any] = {"userIdentifier": user_doc.get("identifier")}
        # Add chat_profile filter if provided
        if hasattr(filters, "chat_profile") and filters.chat_profile:
            query["chat_profile"] = filters.chat_profile
        # Calculate pagination skip and limit
        skip = 0
        limit = 20
        if pagination:
            page = getattr(pagination, "page", None)
            size = getattr(pagination, "size", None)
            if page and size:
                page_num = int(page) if str(page).isdigit() else 1
                size_num = int(size) if str(size).isdigit() else limit
                skip = (page_num - 1) * size_num
                limit = size_num
            # Support offset/first or limit attributes if present
            offset = getattr(pagination, "offset", None)
            if offset is not None:
                skip = int(offset) or 0
            first = getattr(pagination, "first", None)
            if first is not None:
                limit = int(first) or limit
            limit_attr = getattr(pagination, "limit", None)
            if limit_attr is not None:
                limit = int(limit_attr) or limit
        try:
            total = await self.col_threads.count_documents(query)
            cursor = (
                self.col_threads.find(query)
                .sort("updated_at", -1)
                .limit(limit)
                .skip(skip)
            )
            raw_threads = await cursor.to_list(length=limit)
        except Exception as e:
            logger.error(f"Error fetching threads list for user={user_doc.get('identifier')}: {e}", exc_info=True)
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        threads = []
        for t in raw_threads:
            # Ensure thread has essential fields
            t = t.copy()
            if "_id" in t:
                t["id"] = str(t["_id"])
            t.setdefault("name", "Untitled")
            t.setdefault("created_at", _now())
            t.setdefault("updated_at", _now())
            # Format date fields as ISO strings
            t["createdAt"] = _encode_value(t["created_at"])
            t["updatedAt"] = _encode_value(t["updated_at"])
            threads.append(_encode_value(t))
        page_number = (skip // limit + 1) if limit else 1
        return CLPaginatedResponse(data=threads, total=total, page=page_number, size=limit or len(threads))

    async def get_thread(self, thread_id: str, user_identifier: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Retrieves a thread by its identifier, including its steps."""
        logger.info(f"Getting thread - thread_id={thread_id}, user_identifier={user_identifier}")
        # Build query for thread and steps
        thread_query: Dict[str, Any] = {"id": thread_id}
        steps_query: Dict[str, Any] = {"threadId": thread_id}
        if user_identifier:
            thread_query["userIdentifier"] = user_identifier
            steps_query["userIdentifier"] = user_identifier
        try:
            thread_doc = await self.col_threads.find_one(thread_query)
        except Exception as e:
            logger.error(f"Error getting thread - thread_id={thread_id}: {e}", exc_info=True)
            return None
        if not thread_doc:
            logger.debug(f"Thread not found - thread_id={thread_id}")
            return None
        # Fetch all steps for this thread
        try:
            steps_cursor = self.col_steps.find(steps_query)
            steps = [_encode_doc(s) async for s in steps_cursor]
        except Exception as e:
            logger.error(f"Error getting steps for thread - thread_id={thread_id}: {e}", exc_info=True)
            steps = []
        thread_doc["steps"] = steps
        # Ensure thread doc has required fields
        if "created_at" not in thread_doc:
            thread_doc["created_at"] = _now()
        if "updated_at" not in thread_doc:
            thread_doc["updated_at"] = _now()
        if "_id" in thread_doc and "id" not in thread_doc:
            thread_doc["id"] = str(thread_doc["_id"])
        thread_doc["createdAt"] = _encode_value(thread_doc["created_at"])
        thread_doc["updatedAt"] = _encode_value(thread_doc["updated_at"])
        return _encode_doc(thread_doc)

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
        """Updates a thread's metadata (name, tags, etc.)."""
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
            except Exception as e:
                logger.debug(f"Could not determine user_id in update_thread: {e}")
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
            except Exception as e:
                logger.debug(f"Could not determine userIdentifier in update_thread: {e}")
        if chat_profile is not None:
            patch["chat_profile"] = chat_profile
        logger.info(f"Thread update patch: {patch}")
        try:
            await self.col_threads.update_one(
                {"id": thread_id},
                {"$set": patch, "$setOnInsert": {"id": thread_id, "created_at": _now()}},
                upsert=True,
            )
            logger.info(f"Thread updated successfully - thread_id={thread_id}, name={name}")
            return True
        except Exception as e:
            logger.error(f"Error updating thread - thread_id={thread_id}: {e}", exc_info=True)
            return False

