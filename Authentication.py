# app.py
import os
from dotenv import load_dotenv
import chainlit as cl
from mongodb_data_layer import MongoDataLayer

load_dotenv()

@cl.data_layer
def get_data_layer():
    return MongoDataLayer(
        uri=os.environ["MONGODB_URI"],
        db_name=os.environ.get("MONGODB_DB", "chainlit"),
    )

# --- Authentication (required for Chat History UI) ---
# Set CHAINLIT_AUTH_SECRET in the environment (.env)
# Then provide at least one auth callback. (This example looks up users in Mongo.)
from passlib.hash import bcrypt
from motor.motor_asyncio import AsyncIOMotorClient

_mongo = AsyncIOMotorClient(os.environ["MONGODB_URI"])[os.environ.get("MONGODB_DB","chainlit")]
_users = _mongo["users"]



@cl.password_auth_callback
def auth_callback(username: str, password: str):
    import asyncio
    async def _fetch():
        return await _users.find_one({"identifier": username})
    doc = asyncio.get_event_loop().run_until_complete(_fetch())

    if doc and "password_hash" in doc and bcrypt.verify(password, doc["password_hash"]):
        return cl.User(
            identifier=doc["identifier"],
            metadata={**(doc.get("metadata") or {}), "_id": str(doc["_id"])}
        )
    return None
