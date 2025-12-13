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
    """Return current time in Indian time zone (UTC+5:30)."""
    # Note: Adjusting to IST (UTC+5:30)
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)

def _encode_value(v: Any) -> Any:
    """Helper to encode MongoDB values for JSON serialization."""
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
    """Encode all values in the document (dates, ObjectIds) for JSON."""
    return _encode_value(doc) if doc else None

class CLPaginatedResponse:
    """Class for paginated responses with page info."""
    def __init__(self, data: List[Dict[str, Any]], total: int, page: int, size: int):
        self.data = data
        self.total = total
        self.page_info = {"page": page, "size": size, "total": total}

    def to_dict(self) -> Dict[str, Any]:
        return {"data": self.data, "total": self.total, "pageInfo": self.page_info}

class MongoDataLayer(BaseDataLayer):
    def __init__(self, uri: str, db_name: str):
        """Initialize MongoDB data layer and collection references."""
        self.client = motor.AsyncIOMotorClient(uri)
        self.db = self.client[db_name]
        # Set up collections
        self.col_users = self.db["users"]
        self.col_threads = self.db["threads"]
        self.col_steps = self.db["steps"]
        self.col_elements = self.db["elements"]
        self.col_feedback = self.db["feedback"]
        # Note: Session collection is not used in this data layer
        logger.info(f"MongoDB data layer initialized - database={db_name}")

    async def close(self):
        """Close the MongoDB connection."""
        if hasattr(self, "client") and self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def build_debug_url(self, thread_id: str) -> str:
        """Return a debug URL for a given thread (for internal debugging)."""
        return f"mongodb://debug/thread/{thread_id}"

    # ---------------- Users ----------------
    async def get_user(self, identifier: str) -> Optional[cl.User]:
        """Fetch a user by their identifier."""
        identifier = identifier.lower() if identifier else identifier
        logger.info(f"Getting user - identifier={identifier}")
        try:
            doc = await self.col_users.find_one({"identifier": identifier})
        except Exception as e:
            logger.error(f"Error fetching user {identifier}: {e}", exc_info=True)
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
        """Create a new user if it does not exist (upsert) and return the persisted user."""
        identifier = user.identifier.lower() if user.identifier else user.identifier
        logger.info(f"Creating user - identifier={identifier}")
        payload = {
            "identifier": identifier,
            "metadata": user.metadata or {},
            "created_at": _now(),
        }
        try:
            # Only insert if new; do not overwrite existing user data
            await self.col_users.update_one(
                {"identifier": identifier},
                {"$setOnInsert": payload},
                upsert=True,
            )
            doc = await self.col_users.find_one({"identifier": identifier})
        except Exception as e:
            logger.error(f"Error creating/upserting user {identifier}: {e}", exc_info=True)
            return None
        logger.info(f"User created/retrieved - identifier={identifier}")
        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id"))},
        )

    async def update_user(self, identifier: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Update an existing user's metadata or other details."""
        identifier = identifier.lower() if identifier else identifier
        logger.info(f"Updating user - identifier={identifier}")
        update_fields = {}
        if metadata is not None:
            update_fields["metadata"] = metadata
        if not update_fields:
            logger.info("No fields to update for user.")
            return True  # Nothing to update, treat as success
        update_fields["updated_at"] = _now()
        try:
            result = await self.col_users.update_one({"identifier": identifier}, {"$set": update_fields})
            if result.modified_count == 0:
                logger.warning(f"No user found to update for identifier={identifier}")
            else:
                logger.info(f"User updated - identifier={identifier}")
            return True
        except Exception as e:
            logger.error(f"Error updating user {identifier}: {e}", exc_info=True)
            return False

    async def delete_user(self, identifier: str) -> bool:
        """Delete a user and all associated data (threads, steps, elements, feedback)."""
        identifier = identifier.lower() if identifier else identifier
        logger.info(f"Deleting user - identifier={identifier}")
        success = True
        # Find all threads belonging to this user
        try:
            cursor = self.col_threads.find({"userIdentifier": identifier}, {"id": 1})
            thread_ids: List[str] = [t.get("id") async for t in cursor]
        except Exception as e:
            logger.error(f"Error fetching threads for user {identifier}: {e}", exc_info=True)
            return False
        # Delete each thread and its data
        for tid in thread_ids:
            ok = await self.delete_thread(tid)
            if not ok:
                logger.error(f"Failed to delete thread {tid} for user {identifier}")
                success = False
        # Delete the user document itself
        try:
            await self.col_users.delete_one({"identifier": identifier})
            logger.info(f"User deleted - identifier={identifier}")
        except Exception as e:
            logger.error(f"Error deleting user {identifier}: {e}", exc_info=True)
            success = False
        return success

    async def delete_user_session(self, id: str) -> bool:
        """Delete a user session. (No-op: session persistence not used)"""
        logger.info(f"Deleting user session - id={id} (no session storage implemented)")
        # Session data is not persisted, so just return True.
        return True

    # ---------------- Feedback ----------------
    async def upsert_feedback(self, feedback: Any) -> Optional[str]:
        """Insert or update a feedback entry. Returns the feedback ID."""
        logger.info(f"Upserting feedback - feedback={feedback}")
        # Use existing feedback ID or generate a new one
        fid = getattr(feedback, "id", None) or str(uuid.uuid4())
        try:
            doc = asdict(feedback)
        except Exception as e:
            # If feedback is not a dataclass, assume it's already a dict
            doc = dict(feedback)
        # Ensure the correct ID and timestamps
        doc["id"] = fid
        doc["updated_at"] = _now()
        update_doc = {"$set": doc}
        # Only set created_at on insert if not already present in doc
        if "created_at" not in doc:
            update_doc["$setOnInsert"] = {"created_at": _now()}
        try:
            await self.col_feedback.update_one({"id": fid}, update_doc, upsert=True)
            logger.info(f"Feedback upserted - id={fid}")
            return fid
        except Exception as e:
            logger.error(f"Error upserting feedback {fid}: {e}", exc_info=True)
            return None

    async def delete_feedback(self, feedback_id: str) -> bool:
        """Delete a feedback entry by its ID."""
        logger.info(f"Deleting feedback - feedback_id={feedback_id}")
        try:
            res = await self.col_feedback.delete_one({"id": feedback_id})
            if res.deleted_count == 1:
                logger.info(f"Feedback deleted - feedback_id={feedback_id}")
            else:
                logger.warning(f"Feedback not found - feedback_id={feedback_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting feedback {feedback_id}: {e}", exc_info=True)
            return False

    async def get_feedback(self, feedback_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a feedback entry by its ID."""
        logger.info(f"Getting feedback - feedback_id={feedback_id}")
        try:
            doc = await self.col_feedback.find_one({"id": feedback_id})
        except Exception as e:
            logger.error(f"Error fetching feedback {feedback_id}: {e}", exc_info=True)
            return None
        if not doc:
            logger.debug(f"Feedback not found - feedback_id={feedback_id}")
            return None
        logger.info(f"Feedback found - feedback_id={feedback_id}")
        return _encode_doc(doc)

    # ---------------- Elements ----------------
    async def create_element(self, element_dict: dict) -> Optional[str]:
        """Add a new element to the data layer. Returns the element ID."""
        logger.info(f"Creating element - element_dict keys={list(element_dict.keys())}")
        # Assign unique ID if not provided
        if "id" not in element_dict:
            element_dict["id"] = str(uuid.uuid4())
        # Set timestamps
        if not element_dict.get("created_at"):
            element_dict["created_at"] = _now()
        element_dict.setdefault("updated_at", element_dict["created_at"])
        # Normalize threadId field naming
        if element_dict.get("thread_id") and not element_dict.get("threadId"):
            element_dict["threadId"] = element_dict["thread_id"]
            del element_dict["thread_id"]
        try:
            await self.col_elements.insert_one(element_dict)
            logger.info(f"Element created - id={element_dict['id']}")
            return element_dict["id"]
        except Exception as e:
            logger.error(f"Error creating element: {e}", exc_info=True)
            return None

    async def get_element(self, thread_id: str, element_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve an element by thread_id and element_id."""
        logger.info(f"Getting element - thread_id={thread_id}, element_id={element_id}")
        try:
            doc = await self.col_elements.find_one({"id": element_id, "threadId": thread_id})
        except Exception as e:
            logger.error(f"Error fetching element {element_id} in thread {thread_id}: {e}", exc_info=True)
            return None
        if not doc:
            logger.debug(f"Element not found - id={element_id}, thread_id={thread_id}")
            return None
        logger.info(f"Element found - id={element_id}, thread_id={thread_id}")
        return _encode_doc(doc)

    async def update_element(self, element_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing element's fields given its ID."""
        logger.info(f"Updating element - element_id={element_id}, updates={list(updates.keys())}")
        if not updates:
            logger.info("No fields provided to update for element.")
            return True
        updates["updated_at"] = _now()
        try:
            result = await self.col_elements.update_one({"id": element_id}, {"$set": updates})
            if result.modified_count == 0:
                logger.warning(f"No element found or no changes made for id={element_id}")
            else:
                logger.info(f"Element updated - id={element_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating element {element_id}: {e}", exc_info=True)
            return False

    async def delete_element(self, element_id: str) -> bool:
        """Delete an element by its ID."""
        logger.info(f"Deleting element - element_id={element_id}")
        try:
            await self.col_elements.delete_one({"id": element_id})
            logger.info(f"Element deleted - element_id={element_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting element {element_id}: {e}", exc_info=True)
            return False

    # ---------------- Steps (Messages) ----------------
    async def create_step(self, step_dict: dict) -> Optional[str]:
        """Create a new step (message) in the data layer. Returns the step ID."""
        logger.info(f"Creating step - initial keys={list(step_dict.keys())}, type={step_dict.get('type')}")
        # Normalize user identifier to lowercase if present
        if step_dict.get("userIdentifier"):
            step_dict["userIdentifier"] = str(step_dict["userIdentifier"]).lower()
        # Assign unique ID if not provided
        if "id" not in step_dict:
            step_dict["id"] = str(uuid.uuid4())
        # Set creation time if not present
        if not step_dict.get("created_at"):
            step_dict["created_at"] = _now()
        try:
            await self.col_steps.insert_one(step_dict)
        except Exception as e:
            logger.error(f"Error inserting step {step_dict.get('id')}: {e}", exc_info=True)
            return None
        logger.info(f"Step created - id={step_dict['id']}, type={step_dict.get('type')}")
        # Only create/update thread when a user sends the first message
        step_type = step_dict.get("type", "")
        is_user_message = step_type in ["user_message", "message"]
        if not is_user_message:
            logger.info(f"Thread creation skipped for non-user message - type={step_type}")
            return step_dict["id"]
        # Ensure a thread ID is present
        tid = step_dict.get("threadId") or step_dict.get("thread_id")
        if not tid:
            logger.warning(f"No thread_id provided for user message step id={step_dict['id']}")
            return step_dict["id"]
        # Normalize threadId field
        step_dict["threadId"] = tid
        if "thread_id" in step_dict:
            del step_dict["thread_id"]
        # Update the step in DB with threadId for consistency
        try:
            await self.col_steps.update_one({"id": step_dict["id"]}, {"$set": {"threadId": tid}})
        except Exception as e:
            logger.error(f"Error updating step with threadId for step {step_dict['id']}: {e}", exc_info=True)
            # Attempt to rollback the inserted step if thread update fails
            try:
                await self.col_steps.delete_one({"id": step_dict["id"]})
            except Exception as del_err:
                logger.error(f"Failed to delete orphan step {step_dict['id']} after thread update error: {del_err}", exc_info=True)
            return None
        # Prepare thread upsert data
        patch = {
            "$setOnInsert": {"id": tid, "created_at": _now()},
            "$set": {"updated_at": _now()}
        }
        # If user_id is provided (legacy support), store it
        if step_dict.get("user_id"):
            patch["$set"]["user_id"] = step_dict["user_id"]
        # Ensure userIdentifier for thread
        user_identifier = step_dict.get("userIdentifier")
        if not user_identifier:
            try:
                if hasattr(cl, "context") and cl.context:
                    u = cl.user_session.get("user")
                    user_identifier = getattr(u, "identifier", u) if u else None
            except Exception as ctx_err:
                logger.debug(f"Could not get user from Chainlit session: {ctx_err}")
                user_identifier = None
        if user_identifier:
            patch["$set"]["userIdentifier"] = str(user_identifier).lower()
        # Pass along chat_profile if present in step
        if step_dict.get("chat_profile"):
            patch["$set"]["chat_profile"] = step_dict["chat_profile"]
        # Upsert the thread document
        try:
            await self.col_threads.update_one({"id": tid}, patch, upsert=True)
        except Exception as e:
            logger.error(f"Error upserting thread {tid} for step {step_dict['id']}: {e}", exc_info=True)
            # Rollback: delete the inserted step (since thread creation failed)
            try:
                await self.col_steps.delete_one({"id": step_dict["id"]})
            except Exception as del_err:
                logger.error(f"Failed to delete orphan step {step_dict['id']} after thread upsert error: {del_err}", exc_info=True)
            return None
        logger.info(f"Thread created/updated for user message - thread_id={tid}")
        return step_dict["id"]

    async def update_step(self, step_dict: dict) -> bool:
        """Update an existing step (message)."""
        logger.info(f"Updating step - id={step_dict.get('id')}")
        step_dict["updated_at"] = _now()
        try:
            result = await self.col_steps.update_one({"id": step_dict["id"]}, {"$set": step_dict})
            if result.modified_count == 0:
                logger.warning(f"No step found or no changes made for id={step_dict.get('id')}")
            else:
                logger.info(f"Step updated - id={step_dict.get('id')}")
            return True
        except Exception as e:
            logger.error(f"Error updating step {step_dict.get('id')}: {e}", exc_info=True)
            return False

    async def delete_step(self, step_id: str) -> bool:
        """Delete a step (message) by its ID."""
        logger.info(f"Deleting step - step_id={step_id}")
        try:
            await self.col_steps.delete_one({"id": step_id})
            logger.info(f"Step deleted - step_id={step_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting step {step_id}: {e}", exc_info=True)
            return False

    async def get_step(self, step_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single step (message) by its ID."""
        logger.info(f"Getting step - step_id={step_id}")
        try:
            doc = await self.col_steps.find_one({"id": step_id})
        except Exception as e:
            logger.error(f"Error fetching step {step_id}: {e}", exc_info=True)
            return None
        if not doc:
            logger.debug(f"Step not found - step_id={step_id}")
            return None
        logger.info(f"Step found - step_id={step_id}")
        return _encode_doc(doc)

    # ---------------- Threads ----------------
    async def create_thread(
        self,
        user_identifier: str,
        chat_profile: Optional[str] = None,
        name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Create a new thread for a user. Returns the new thread ID."""
        user_identifier = user_identifier.lower() if user_identifier else user_identifier
        tid = str(uuid.uuid4())
        now = _now()
        doc: Dict[str, Any] = {
            "id": tid,
            "userIdentifier": user_identifier,
            "created_at": now,
            "updated_at": now,
        }
        if chat_profile is not None:
            doc["chat_profile"] = chat_profile
        if name is not None:
            doc["name"] = name
        if metadata is not None:
            doc["metadata"] = metadata
        if tags is not None:
            doc["tags"] = tags
        logger.info(f"Creating thread - id={tid}, userIdentifier={user_identifier}")
        try:
            await self.col_threads.insert_one(doc)
            logger.info(f"Thread created - id={tid}, userIdentifier={user_identifier}")
            return tid
        except Exception as e:
            logger.error(f"Error creating thread for user {user_identifier}: {e}", exc_info=True)
            return None

    async def get_thread_author(self, thread_id: str) -> Optional[str]:
        """Get the author (user identifier) of a given thread."""
        logger.info(f"Getting thread author - thread_id={thread_id}")
        try:
            t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        except Exception as e:
            logger.error(f"Error fetching thread author for {thread_id}: {e}", exc_info=True)
            return None
        if not t:
            logger.debug(f"Thread not found - thread_id={thread_id}")
            return None
        author = t.get("userIdentifier")
        logger.info(f"Thread author retrieved - thread_id={thread_id}, author={author}")
        return author.lower() if author else None

    async def delete_thread(self, thread_id: str) -> bool:
        """Delete a thread and all associated steps, elements, and feedback."""
        logger.info(f"Deleting thread - thread_id={thread_id}")
        success = True
        # First, gather all step IDs for this thread (including any legacy field usage)
        step_ids: List[str] = []
        try:
            steps_cursor = self.col_steps.find({"$or": [{"threadId": thread_id}, {"thread_id": thread_id}]}, {"id": 1})
            async for st in steps_cursor:
                if st.get("id"):
                    step_ids.append(st["id"])
        except Exception as e:
            logger.error(f"Error finding steps for thread {thread_id}: {e}", exc_info=True)
            return False
        # Delete all feedback associated with this thread or its steps
        try:
            if step_ids:
                res1 = await self.col_feedback.delete_many({"forId": {"$in": step_ids}})
                logger.debug(f"Deleted feedback by forId (step_ids) - count={res1.deleted_count}")
            res2 = await self.col_feedback.delete_many({"threadId": thread_id})
            logger.debug(f"Deleted feedback by threadId - count={res2.deleted_count}")
        except Exception as e:
            logger.error(f"Error deleting feedback for thread {thread_id}: {e}", exc_info=True)
            success = False
        # Delete all steps for this thread
        try:
            await self.col_steps.delete_many({"$or": [{"threadId": thread_id}, {"thread_id": thread_id}]})
        except Exception as e:
            logger.error(f"Error deleting steps for thread {thread_id}: {e}", exc_info=True)
            success = False
        # Delete all elements for this thread
        try:
            await self.col_elements.delete_many({"threadId": thread_id})
        except Exception as e:
            logger.error(f"Error deleting elements for thread {thread_id}: {e}", exc_info=True)
            success = False
        # Finally, delete the thread document itself
        try:
            await self.col_threads.delete_one({"id": thread_id})
        except Exception as e:
            logger.error(f"Error deleting thread document {thread_id}: {e}", exc_info=True)
            success = False
        if success:
            logger.info(f"Thread deleted successfully - thread_id={thread_id}")
        else:
            logger.warning(f"Thread deletion completed with errors - thread_id={thread_id}")
        return success

    async def list_threads(self, pagination, filters) -> CLPaginatedResponse:
        """List threads for the current user (with optional filters and pagination)."""
        logger.info(f"Listing threads - pagination={pagination}, filters={filters}")
        # Extract userId (expected to be a Mongo _id as string)
        user_id = getattr(filters, "userId", None)
        if not user_id:
            logger.warning("No userId provided in filters for list_threads")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        try:
            user_obj_id = ObjectId(user_id)
        except Exception as e:
            logger.warning(f"Invalid userId for list_threads: {user_id} - {e}")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        # Retrieve user by ID
        try:
            doc = await self.col_users.find_one({"_id": user_obj_id})
        except Exception as e:
            logger.error(f"Error fetching user by _id for list_threads: {e}", exc_info=True)
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        if not doc:
            logger.warning(f"User not found for userId={user_id}")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        user_identifier = doc.get("identifier")
        query = {"userIdentifier": user_identifier}
        # Apply chat_profile filter if present
        if hasattr(filters, "chat_profile") and filters.chat_profile:
            query["chat_profile"] = filters.chat_profile
        # Calculate pagination skip and limit
        skip = 0
        limit = 20
        if pagination:
            offset = getattr(pagination, "offset", None)
            if offset is not None:
                skip = int(offset) or 0
            first = getattr(pagination, "first", None)
            if first is not None:
                limit = int(first) or limit
            page_num = getattr(pagination, "page", None)
            page_size = getattr(pagination, "size", None)
            if page_num and page_size:
                page_num = int(page_num) or 1
                page_size = int(page_size) or limit
                skip = (page_num - 1) * page_size
                limit = page_size
            limit_attr = getattr(pagination, "limit", None)
            if limit_attr is not None:
                limit = int(limit_attr) or limit
        # Query total count and paginated items
        try:
            total = await self.col_threads.count_documents(query)
            cursor = self.col_threads.find(query).sort("updated_at", -1).limit(limit).skip(skip)
            raw_items = await cursor.to_list(length=limit)
        except Exception as e:
            logger.error(f"Error listing threads for user {user_identifier}: {e}", exc_info=True)
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)
        logger.info(f"Threads listed - user={user_identifier}, total={total}, returned={len(raw_items)}")
        # Prepare thread items for response
        items = [self._prepare_thread_item(it) for it in raw_items]
        # Determine current page number
        current_page = (skip // limit + 1) if limit else 1
        return CLPaginatedResponse(data=items, total=total, page=current_page, size=limit or len(items))

    async def get_thread(self, thread_id: str, user_identifier: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Retrieve a thread by its ID, including all steps, if owned by the user_identifier (if provided)."""
        logger.info(f"Getting thread - thread_id={thread_id}, user_identifier={user_identifier}")
        # Build query for thread
        thread_query: Dict[str, Any] = {"id": thread_id}
        if user_identifier:
            thread_query["userIdentifier"] = user_identifier.lower()
        try:
            thread_doc = await self.col_threads.find_one(thread_query)
        except Exception as e:
            logger.error(f"Error fetching thread {thread_id}: {e}", exc_info=True)
            return None
        if not thread_doc:
            logger.debug(f"Thread not found or not accessible - thread_id={thread_id}")
            return None
        # Fetch all steps for this thread
        steps_query: Dict[str, Any] = {"threadId": thread_id}
        if user_identifier:
            steps_query["userIdentifier"] = user_identifier.lower()
        try:
            steps_cursor = self.col_steps.find(steps_query)
            steps = [_encode_doc(s) async for s in steps_cursor]
        except Exception as e:
            logger.error(f"Error fetching steps for thread {thread_id}: {e}", exc_info=True)
            steps = []
        thread_doc["steps"] = steps
        logger.info(f"Thread retrieved - thread_id={thread_id}, steps_count={len(steps)}")
        # Ensure required fields
        if "name" not in thread_doc:
            # Do not force "Untitled" here to avoid overwriting existing name
            pass
        # Add createdAt and updatedAt ISO strings
        thread_doc.setdefault("created_at", _now())
        thread_doc.setdefault("updated_at", _now())
        thread_doc["createdAt"] = _encode_value(thread_doc["created_at"])
        thread_doc["updatedAt"] = _encode_value(thread_doc["updated_at"])
        if "id" not in thread_doc and thread_doc.get("_id"):
            thread_doc["id"] = str(thread_doc["_id"])
        return _encode_doc(thread_doc)

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
        """Update a thread's details (name, user, metadata, tags, profile)."""
        logger.info(f"Updating thread - thread_id={thread_id}, name={name}")
        logger.info(f"Parameters: user_id={user_id}, user_identifier={user_identifier}, metadata keys={None if metadata is None else list(metadata.keys())}, tags={tags}, chat_profile={chat_profile}")
        patch: Dict[str, Any] = {"updated_at": _now()}
        if name is not None:
            patch["name"] = name
        # If user_id (internal user identifier) is provided, update it
        if user_id is not None:
            patch["user_id"] = user_id
        else:
            # If not provided, attempt to use current session user (if any)
            try:
                if hasattr(cl, "context") and cl.context:
                    current_user = cl.user_session.get("user")
                    if current_user and hasattr(current_user, "identifier"):
                        patch["user_id"] = current_user.identifier
            except Exception as e:
                logger.debug(f"Could not get user_id from session while updating thread {thread_id}: {e}")
        # If user_identifier (user's public identifier) is provided
        if user_identifier is not None:
            patch["userIdentifier"] = user_identifier.lower()
        else:
            try:
                if hasattr(cl, "context") and cl.context:
                    current_user = cl.user_session.get("user")
                    if current_user and hasattr(current_user, "identifier"):
                        patch["userIdentifier"] = current_user.identifier.lower()
            except Exception as e:
                logger.debug(f"Could not get user identifier from session while updating thread {thread_id}: {e}")
        if metadata is not None:
            patch["metadata"] = metadata
        if tags is not None:
            patch["tags"] = tags
        if chat_profile is not None:
            patch["chat_profile"] = chat_profile
        logger.info(f"Patching thread {thread_id} with fields: {list(patch.keys())}")
        try:
            await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=True)
            logger.info(f"Thread updated successfully - thread_id={thread_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating thread {thread_id}: {e}", exc_info=True)
            return False

    # ---------------- Internal Helpers ----------------
    def _prepare_thread_item(self, it: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a thread document for listing (formatting fields)."""
        # Ensure 'id' field is present as string
        if "id" not in it and it.get("_id"):
            it["id"] = str(it["_id"])
        it.setdefault("name", "Untitled")
        it.setdefault("created_at", _now())
        it.setdefault("updated_at", _now())
        it["createdAt"] = _encode_value(it["created_at"])
        it["updatedAt"] = _encode_value(it["updated_at"])
        # Remove internal fields that should not be exposed
        it.pop("_id", None)
        it.pop("userIdentifier", None)
        return _encode_doc(it)
