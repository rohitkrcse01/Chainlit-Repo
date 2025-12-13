async def create_step(self, step_dict: Dict[str, Any]) -> str:
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

    # Create thread ONLY on first USER message
    step_type = (step.get("type") or "").strip()
    is_user_message = step_type in ["user_message", "message"]

    if not is_user_message or not tid:
        return step["id"]

    # derive proper title for chat
    derived_title = _derive_thread_title(step)

    existing = await self.col_threads.find_one({"id": tid})
    now = _utcnow()

    if not existing:
        # Create thread with correct name
        thread_doc: Dict[str, Any] = {
            "id": tid,
            "name": derived_title,  # ✅ FIX: proper title (NOT userIdentifier)
            "userIdentifier": step.get("userIdentifier"),
            "chat_profile": step.get("chat_profile"),
            "created_at": now,
            "updated_at": now,
            "metadata": {},
            "tags": [],
        }
        await self.col_threads.insert_one(thread_doc)
    else:
        # Update last activity
        patch: Dict[str, Any] = {"updated_at": now}

        if step.get("userIdentifier"):
            patch["userIdentifier"] = step.get("userIdentifier")
        if step.get("chat_profile"):
            patch["chat_profile"] = step.get("chat_profile")

        # ✅ FIX: if thread name is missing/Untitled, set it once from first message
        if (not existing.get("name")) or existing.get("name") == "Untitled":
            patch["name"] = derived_title

        await self.col_threads.update_one({"id": tid}, {"$set": patch})

    return step["id"]

