# seed_user.py
import os
from passlib.hash import bcrypt
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import asyncio

load_dotenv()
db = AsyncIOMotorClient(os.environ["MONGODB_URI"])[os.environ["MONGODB_DB"]]
async def main():
    await db["users"].update_one(
        {"identifier": "user"},
        {"$setOnInsert": {
            "identifier": "user",
            "password_hash": bcrypt.hash("user"),
            "metadata": {"role": "user"}
        }},
        upsert=True,
    )
asyncio.run(main())
print("Seeded user / user")
