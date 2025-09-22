# app.py
import os
from dotenv import load_dotenv
import chainlit as cl
from mongodb_data_layer import MongoDataLayer
from chainlit.types import ThreadDict
from passlib.hash import bcrypt
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

# -----------------------------
# Data layer setup
# -----------------------------
@cl.data_layer
def get_data_layer():
    return MongoDataLayer(
        uri=os.environ["MONGODB_URI"],
        db_name=os.environ.get("MONGODB_DB", "chainlit"),
    )

# -----------------------------
# Mongo setup (for optional auth lookup)
# -----------------------------
_mongo = AsyncIOMotorClient(os.environ["MONGODB_URI"])[
    os.environ.get("MONGODB_DB", "chainlit")
]
_users = _mongo["users"]

# -----------------------------
# Authentication
# -----------------------------
@cl.password_auth_callback
def auth_callback(username: str, password: str):
    import asyncio

    async def _fetch():
        return await _users.find_one({"identifier": username})

    doc = asyncio.get_event_loop().run_until_complete(_fetch())

    # ✅ Always allow "user" / "user" for testing
    if username == "user" and password == "user":
        return cl.User(identifier="user", metadata={"role": "user"})

    if doc:
        pw_hash = None
        if isinstance(doc.get("metadata"), dict):
            pw_hash = doc["metadata"].get("password_hash")
        if not pw_hash:
            pw_hash = doc.get("password_hash")

        if pw_hash and bcrypt.verify(password, pw_hash):
            return cl.User(
                identifier=doc["identifier"],
                metadata={**(doc.get("metadata") or {}), "_id": str(doc["_id"])},
            )
    return None

# -----------------------------
# Chat lifecycle
# -----------------------------
@cl.on_chat_start
async def on_chat_start():
    # Start fresh chat history
    cl.user_session.set("chat_history", [])

@cl.on_chat_resume
async def on_chat_resume(thread: ThreadDict):
    """
    Resume a previous chat when a thread is reopened from history.
    Chainlit passes the full thread object here, including steps.
    """
    history = []
    for step in thread.get("steps", []):
        msg_type = step.get("type")
        content = step.get("output") or step.get("content") or step.get("text")
        if msg_type == "user_message" and content:
            history.append({"role": "user", "content": content})
        elif msg_type == "assistant_message" and content:
            history.append({"role": "assistant", "content": content})
    cl.user_session.set("chat_history", history)
    # (Optional) Send a system message when resuming
    await cl.Message(content="✅ Chat resumed from history.").send() 
# -----------------------------
# Message handler
# -----------------------------
@cl.on_message
async def on_message(msg: cl.Message):
    chat_history = cl.user_session.get("chat_history") or []

    # Append user message
    chat_history.append({"role": "user", "content": msg.content})

    # For demo: just echo back
    response = f"Echo: {msg.content}"

    # Append assistant response
    chat_history.append({"role": "assistant", "content": response})
    cl.user_session.set("chat_history", chat_history)

    await cl.Message(content=response).send()
