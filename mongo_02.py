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
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)  # only 5:30 hr added


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
        """Close the MongoDB connection"""
        if hasattr(self, "client") and self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def build_debug_url(self, thread_id: str) -> str:
        return f"mongodb://debug/thread/{thread_id}"

    # ---------------- Users ----------------

    async def get_user(self, identifier: str):
        identifier = identifier.lower() if identifier else identifier
        logger.info(f"Getting user - identifier={identifier}")
        doc = await self.col_users.find_one({"identifier": identifier})
        if not doc:
            logger.debug(f"User not found - identifier={identifier}")
            return None
        logger.info(f"User found - identifier={identifier}")
        return cl.User(
            identifier=doc["identifier"],
            metadata={**doc.get("metadata", {}), "_id": str(doc.get("_id"))},
        )

    async def create_user(self, user: cl.User):
        identifier = user.identifier.lower() if user.identifier else user.identifier
        logger.info(f"Creating user - identifier={identifier}")

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
        logger.info(f"User created/retrieved - identifier={identifier}")

        return cl.User(
            identifier=identifier,
            metadata={**(user.metadata or {}), "_id": str(doc.get("_id"))},
        )

    async def delete_user_session(self, id: str) -> bool:
        logger.info(f"Deleting user session - id={id}")
        # Keeping functionality exactly as you wrote: delete by "_id": <string>
        await self.col_sessions.delete_one({"_id": id})
        logger.info(f"User session deleted - id={id}")
        return True

    # ---------------- Feedback ----------------

    async def upsert_feedback(self, feedback):
        logger.info(f"Upserting feedback - feedback={feedback}")
        fid = getattr(feedback, "id", None) or str(uuid.uuid4())
        doc = asdict(feedback)
        doc["id"] = fid
        doc["updated_at"] = _now()
        await self.col_feedback.update_one({"id": fid}, {"$set": doc}, upsert=True)
        logger.info(f"Feedback upserted - id={fid}")
        return fid

    async def delete_feedback(self, feedback_id: str) -> bool:
        logger.info(f"Deleting feedback - feedback_id={feedback_id}")
        res = await self.col_feedback.delete_one({"id": feedback_id})
        logger.info(
            f"Feedback deleted - feedback_id={feedback_id}, deleted={res.deleted_count == 1}"
        )
        return res.deleted_count == 1

    # ---------------- Elements ----------------
    # (Your pasted code referenced elements deletion, so we keep minimal element support here)

    async def create_element(self, element_dict: dict):
        logger.info(f"Creating element - element_dict={element_dict}")

        if "id" not in element_dict:
            element_dict["id"] = str(uuid.uuid4())
        if not element_dict.get("created_at"):
            element_dict["created_at"] = _now()

        # Normalize thread field naming (same style as steps)
        if element_dict.get("thread_id") and not element_dict.get("threadId"):
            element_dict["threadId"] = element_dict["thread_id"]
            del element_dict["thread_id"]

        await self.col_elements.insert_one(element_dict)
        logger.info(f"Element created - id={element_dict['id']}")
        return element_dict["id"]

    async def delete_element(self, element_id: str):
        logger.info(f"Deleting element - element_id={element_id}")
        await self.col_elements.delete_one({"id": element_id})
        logger.info(f"Element deleted - element_id={element_id}")
        return True

    # ---------------- Steps (messages) ----------------

    async def create_step(self, step_dict: dict):
        logger.info(f"Creating step - step_dict={step_dict}")

        # Normalize userIdentifier to lowercase if present
        if step_dict.get("userIdentifier"):
            step_dict["userIdentifier"] = step_dict["userIdentifier"].lower()

        if "id" not in step_dict:
            step_dict["id"] = str(uuid.uuid4())
        if not step_dict.get("created_at"):
            step_dict["created_at"] = _now()

        await self.col_steps.insert_one(step_dict)
        logger.info(
            f"Step created - id={step_dict['id']}, type={step_dict.get('type')}"
        )

        # Check if this is a user message - only create threads when user sends first message
        step_type = step_dict.get("type", "")
        is_user_message = step_type in ["user_message", "message"]

        # Skip thread creation for non-user messages during initial chat start
        if not is_user_message:
            logger.info(
                f"Thread creation skipped for non-user message - type={step_type}"
            )
            return step_dict["id"]

        # Ensure thread exists & update
        tid = step_dict.get("thread_id") or step_dict.get("threadId")
        if not tid:
            return step_dict["id"]

        # Normalize threadId for consistent schema - always use threadId
        step_dict["threadId"] = tid
        if "thread_id" in step_dict:
            del step_dict["thread_id"]

        # Update the step in database with consistent threadId
        await self.col_steps.update_one(
            {"id": step_dict["id"]},
            {"$set": {"threadId": tid}},
        )

        patch = {
            "$setOnInsert": {"id": tid, "created_at": _now()},
            "$set": {"updated_at": _now()},
        }

        # Persist user_id if provided (for legacy consumers)
        if step_dict.get("user_id"):
            patch["$set"]["user_id"] = step_dict["user_id"]

        # Persist userIdentifier consistently
        user_identifier = step_dict.get("userIdentifier")
        if not user_identifier:
            try:
                # Only try to get user from Chainlit session if we're in a Chainlit context
                if hasattr(cl, "context") and cl.context:
                    u = cl.user_session.get("user")
                    user_identifier = getattr(u, "identifier", u) if u else None
            except Exception as e:
                logger.debug(
                    f"Could not get user from Chainlit session (normal for FastAPI calls): {e}"
                )
                user_identifier = None

        if user_identifier:
            patch["$set"]["userIdentifier"] = str(user_identifier).lower()

        # Persist chat_profile if present on step (optional)
        if step_dict.get("chat_profile"):
            patch["$set"]["chat_profile"] = step_dict["chat_profile"]

        await self.col_threads.update_one({"id": tid}, patch, upsert=True)
        logger.info(f"Thread created/updated for user message - tid={tid}")

        return step_dict["id"]

    async def update_step(self, step_dict: dict):
        logger.info(f"Updating step - id={step_dict['id']}")
        await self.col_steps.update_one({"id": step_dict["id"]}, {"$set": step_dict})
        logger.info(f"Step updated - id={step_dict['id']}")
        return True

    async def delete_step(self, step_id: str):
        logger.info(f"Deleting step - step_id={step_id}")
        await self.col_steps.delete_one({"id": step_id})
        logger.info(f"Step deleted - step_id={step_id}")
        return True

    # ---------------- Threads ----------------

    async def get_thread_author(self, thread_id: str):
        logger.info(f"Getting thread author - thread_id={thread_id}")
        t = await self.col_threads.find_one({"id": thread_id}, {"userIdentifier": 1})
        author = t.get("userIdentifier") if t else None
        logger.info(f"Thread author retrieved - thread_id={thread_id}, author={author}")
        return author.lower() if author else None

    async def delete_thread(self, thread_id: str):
        logger.info(f"Deleting thread - thread_id={thread_id}")

        # Delete the thread itself
        thread_result = await self.col_threads.delete_one({"id": thread_id})
        logger.info(
            f"Deleted thread - thread_id={thread_id}, deleted_count={thread_result.deleted_count}"
        )

        # Delete all steps associated with this thread (using consistent threadId field)
        steps_result = await self.col_steps.delete_many({"threadId": thread_id})
        logger.info(
            f"Deleted steps - thread_id={thread_id}, deleted_count={steps_result.deleted_count}"
        )

        # Also delete any remaining legacy steps with thread_id field (cleanup)
        steps_result_legacy = await self.col_steps.delete_many({"thread_id": thread_id})
        if steps_result_legacy.deleted_count > 0:
            logger.info(
                f"Deleted legacy steps - thread_id={thread_id}, deleted_count={steps_result_legacy.deleted_count}"
            )
        else:
            logger.debug("No legacy steps found with thread_id field")

        # Delete all elements associated with this thread (your code referenced this)
        elements_result = await self.col_elements.delete_many({"threadId": thread_id})
        logger.info(
            f"Deleted elements - thread_id={thread_id}, deleted_count={elements_result.deleted_count}"
        )

        # --- Feedback deletion flow (same intention as your code, corrected) ---
        logger.info(f"Starting feedback deletion process - thread_id={thread_id}")

        # Get all step IDs for this thread (for possible legacy cleanup / future use)
        step_ids: List[str] = []

        # Primary query using standard threadId field
        steps_cursor = self.col_steps.find({"threadId": thread_id}, {"id": 1})
        async for st in steps_cursor:
            sid = st.get("id")
            if sid:
                step_ids.append(sid)

        # Legacy query for any remaining thread_id fields (cleanup)
        steps_cursor_legacy = self.col_steps.find({"thread_id": thread_id}, {"id": 1})
        legacy_count = 0
        async for st in steps_cursor_legacy:
            sid = st.get("id")
            if sid:
                step_ids.append(sid)
                legacy_count += 1

        if legacy_count > 0:
            logger.debug(f"Found {legacy_count} legacy steps with thread_id field")

        logger.debug(f"Found {len(step_ids)} step IDs for thread {thread_id}")

        # Delete feedback (your code's Method 1)
        feedback_result_threadid = await self.col_feedback.delete_many(
            {"threadId": thread_id}
        )
        logger.debug(
            f"Deleted feedback by threadId: {feedback_result_threadid.deleted_count}"
        )

        # Keep same flow: return True at the end
        return True

    # ---------------- Pagination + Thread read ----------------

    def _calculate_pagination(self, pagination):
        """Helper method to calculate skip and limit from pagination object"""
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
        """Helper method to prepare a thread item for response"""
        if "id" not in it and it.get("_id"):
            it["id"] = str(it["_id"])
        it.setdefault("name", "Untitled")
        it.setdefault("created_at", _now())
        it.setdefault("updated_at", _now())
        it["createdAt"] = _encode_value(it["created_at"])
        it["updatedAt"] = _encode_value(it["updated_at"])
        return _encode_doc(it)

    async def list_threads(self, pagination, filters):
        logger.info(f"Listing threads - pagination={pagination}, filters={filters}")

        # Keep same functionality: userId is expected to be a Mongo ObjectId string
        user_id = filters.__getattribute__("userId")
        doc = await self.col_users.find_one({"_id": ObjectId(user_id)})

        if not doc:
            logger.warning(f"User not found for userId={user_id}")
            return CLPaginatedResponse(data=[], total=0, page=1, size=0)

        query = {"userIdentifier": doc.get("identifier")}

        # Add chat_profile filter if available
        if hasattr(filters, "chat_profile") and filters.chat_profile:
            query["chat_profile"] = filters.chat_profile

        skip, limit = self._calculate_pagination(pagination)

        total = await self.col_threads.count_documents(query)
        cursor = (
            self.col_threads.find(query)
            .sort("updated_at", -1)
            .limit(limit)
            .skip(skip)
        )

        raw_items = await cursor.to_list(length=limit)
        logger.info(
            f"Listed threads - user={doc.get('identifier')}, total={total}, returned={len(raw_items)}"
        )

        items = [self._prepare_thread_item(it) for it in raw_items]

        page_number = (skip // limit + 1) if limit else 1
        return CLPaginatedResponse(
            data=items, total=total, page=page_number, size=limit or len(items)
        )

    async def get_thread(self, thread_id: str, user_identifier: Optional[str] = None):
        logger.info(
            f"Getting thread - thread_id={thread_id}, user_identifier={user_identifier}"
        )

        # Fetch thread with both thread_id and userIdentifier
        thread_query: Dict[str, Any] = {"id": thread_id}
        if user_identifier:
            thread_query["userIdentifier"] = user_identifier

        t = await self.col_threads.find_one(thread_query)

        if not t:
            logger.debug(
                f"Thread not found - thread_id={thread_id}, users={user_identifier}"
            )
            return None

        steps_query: Dict[str, Any] = {"threadId": thread_id}
        if user_identifier:
            steps_query["userIdentifier"] = user_identifier

        steps_cursor = self.col_steps.find(steps_query)
        steps = [_encode_doc(s) async for s in steps_cursor]

        t["steps"] = steps
        logger.info(f"Thread retrived - thread_id={thread_id}, steps={len(steps)}")

        # Only set name if it exists in the thread document
        if "name" not in t:
            logger.debug(
                f"Thread name missing for thread_id={thread_id}, not setting to 'Untitled'."
            )

        if "created_at" not in t:
            t["created_at"] = _now()
        if "updated_at" not in t:
            t["updated_at"] = _now()

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
        user_identifier: Optional[str] = None,
        chat_profile: Optional[str] = None,
    ):
        try:
            logger.info(f"UPDATE THREAD CALLED: thread_id={thread_id}, name={name}")
            logger.info(
                f"Parameters: user_id={user_id}, user_identifier={user_identifier}, metadata={metadata}, tags={tags}, chat_profile={chat_profile}"
            )

            patch: Dict[str, Any] = {"updated_at": _now()}

            if name is not None:
                patch["name"] = name

            # Handle user_id with better error handling (same behavior)
            if user_id is not None:
                patch["user_id"] = user_id
            else:
                try:
                    # Only try to get user from Chainlit session if we're in a Chainlit context
                    if hasattr(cl, "context") and cl.context:
                        current_user = cl.user_session.get("user")
                        if current_user and hasattr(current_user, "identifier"):
                            patch["user_id"] = current_user.identifier
                        else:
                            logger.debug(
                                f"No user session found when updating thread {thread_id}"
                            )
                    else:
                        logger.debug(
                            f"Not in Chainlit context when updating thread {thread_id}"
                        )
                except Exception as user_err:
                    logger.debug(
                        f"Error getting user from session (this is normal for FastAPI calls): {user_err}"
                    )

            if metadata is not None:
                patch["metadata"] = metadata
            if tags is not None:
                patch["tags"] = tags

            # Handle userIdentifier with better error handling (same behavior)
            if user_identifier is not None:
                patch["userIdentifier"] = user_identifier
            else:
                try:
                    # Only try to get user from Chainlit session if we're in a Chainlit context
                    if hasattr(cl, "context") and cl.context:
                        current_user = cl.user_session.get("user")
                        if current_user and hasattr(current_user, "identifier"):
                            patch["userIdentifier"] = current_user.identifier
                        else:
                            logger.debug(
                                f"No user identifier found when updating thread {thread_id}"
                            )
                    else:
                        logger.debug(
                            f"Not in Chainlit context when updating thread {thread_id}"
                        )
                except Exception as user_err:
                    logger.debug(
                        f"Error getting user identifier from session (this is normal for FastAPI calls): {user_err}"
                    )

            if chat_profile is not None:
                patch["chat_profile"] = chat_profile

            logger.info(f"Updating thread with patch: {patch}")

            await self.col_threads.update_one({"id": thread_id}, {"$set": patch}, upsert=True)

            logger.info(
                f"Thread updated successfully - thread_id={thread_id}, name={name}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Error updating thread - thread_id={thread_id}: {str(e)}",
                exc_info=True,
            )
            return False
