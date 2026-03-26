import logging
from app.core.pipeline import run_pipeline

log = logging.getLogger("pipeline_refresh")


async def run_pipeline_refresh(bot, ib_client, admin_chat_id: int = 0):
    try:
        snapshot = await run_pipeline(ib_client)
        final_count = len(snapshot.get("final_candidates", []) or [])
        log.info("[pipeline_refresh] done | final=%d", final_count)
    except Exception as e:
        log.exception("[pipeline_refresh] failed: %s", e)
        if bot and admin_chat_id:
            await bot.send_message(
                admin_chat_id,
                f"Pipeline refresh failed: {e}"
            )