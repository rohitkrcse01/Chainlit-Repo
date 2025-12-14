@cl.on_chat_resume
async def on_chat_resume(thread: ThreadDict):
    """
    Production-safe chat resume:
    - Never creates a thread.
    - Verifies the thread exists in MongoDB; if missing/deleted, blocks resume.
    - Restores chat history from persisted steps.
    - Re-initializes in-memory agents only.
    """
    thread_id = thread.get("id")
    logger.info("Resuming chat session for thread: %s", thread_id or "unknown")

    try:
        # ---- Hard guard: no thread id, nothing to resume ----
        if not thread_id or not isinstance(thread_id, str):
            logger.warning("on_chat_resume: Missing/invalid thread id. Aborting resume.")
            cl.user_session.set("thread_deleted_or_missing", True)
            await cl.Message(content="Unable to resume this chat. Please start a new chat.").send()
            return

        # ---- Verify the thread exists in persistence (prevents resuming deleted threads) ----
        # NOTE: This does NOT create anything; it's a pure read.
        db_thread: Optional[Dict[str, Any]] = None
        try:
            # cl.data_layer should be your MongoDataLayer (registered via @cl.data_layer)
            data_layer = cl.data_layer
            if data_layer:
                db_thread = await data_layer.get_thread(thread_id)
        except Exception as e:
            logger.error(
                "on_chat_resume: Failed to verify thread existence for thread_id=%s: %s",
                thread_id,
                e,
                exc_info=True,
            )

        if not db_thread:
            # Thread missing in DB => deleted or never existed => do not resume.
            logger.warning(
                "on_chat_resume: Thread not found in DB (deleted/missing). thread_id=%s",
                thread_id,
            )
            cl.user_session.set("thread_deleted_or_missing", True)
            await cl.Message(
                content="This chat was deleted or no longer exists. Please start a new chat."
            ).send()
            return

        # Thread exists -> allow resume
        cl.user_session.set("thread_deleted_or_missing", False)

        # ---- Resolve user identifier safely ----
        user_id = (
            db_thread.get("userIdentifier")
            or thread.get("userIdentifier")
            or "UNKNOWN"
        )
        if not user_id or user_id == "UNKNOWN":
            current_user = cl.user_session.get("user")
            if current_user:
                user_id = (
                    current_user
                    if isinstance(current_user, str)
                    else getattr(current_user, "identifier", "UNKNOWN")
                )

        if isinstance(user_id, str):
            user_id = user_id.lower()

        logger.info("Resuming session for user: %s (thread_id=%s)", user_id, thread_id)

        # ---- Restore minimal session state (no DB writes here) ----
        cl.user_session.set("user", user_id)
        cl.user_session.set("session_id", thread_id)

        # ---- Restore chat history from persisted steps ----
        steps = db_thread.get("steps") or thread.get("steps") or []
        logger.info("Restoring chat history: %d steps found", len(steps))

        history: List[Dict[str, str]] = []
        for step in steps:
            msg_type = step.get("type")
            content = step.get("output") or step.get("content") or step.get("text")
            if not content:
                continue

            # Chainlit step types vary; keep your two main ones and ignore the rest
            if msg_type == "user_message":
                history.append({"role": "user", "content": str(content)})
            elif msg_type == "assistant_message":
                history.append({"role": "assistant", "content": str(content)})

        cl.user_session.set("chat_history", history)
        logger.info("Restored %d messages into session chat_history", len(history))

        # ---- Re-init in-memory agents only (no persistence calls) ----
        try:
            logger.info("Re-initializing OrchestraDocumentAgentStudio for resumed session...")
            orchestra_document_agent = OrchestraDocumentAgentStudio()
            await orchestra_document_agent.initialize_session()
            cl.user_session.set("orchestra_document_agent", orchestra_document_agent)
            logger.info("OrchestraDocumentAgentStudio re-initialized successfully")
        except Exception as e:
            # Degrade gracefully; user can still chat
            logger.error(
                "on_chat_resume: Failed to re-initialize OrchestraDocumentAgentStudio: %s",
                e,
                exc_info=True,
            )
            cl.user_session.set("orchestra_document_agent", None)

        # Ensure profiles exist (idempotent)
        if not cl.user_session.get("knowledge_assistant_profile"):
            cl.user_session.set("knowledge_assistant_profile", KnowledgeAssistantProfile())
        if not cl.user_session.get("nfr_profile"):
            cl.user_session.set("nfr_profile", NfrProfile())

        logger.info("Chat session resumed successfully (thread_id=%s)", thread_id)

    except Exception as e:
        logger.error("Error resuming chat session: %s", e, exc_info=True)
        cl.user_session.set("thread_deleted_or_missing", True)
        await cl.Message(content="Failed to resume chat. Please start a new chat.").send()

