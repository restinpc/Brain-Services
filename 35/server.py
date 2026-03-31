import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from brain_framework import BaseBrainService
from common import log

# ─────────────────────────────────────────────────────────────────────────────
# КОНФИГУРАЦИЯ
# ─────────────────────────────────────────────────────────────────────────────

SERVICE_ID = 35
PORT       = 8898
NODE_NAME  = "brain-news-weights"

CTX_TABLE     = "vlad_news_context_idx"
WEIGHTS_TABLE = "vlad_news_weights_table"
CTX_MAP_TABLE = "vlad_news_ctx_map"


# ─────────────────────────────────────────────────────────────────────────────
# КЛАСС СЕРВИСА
# ─────────────────────────────────────────────────────────────────────────────

class NewsWeightsService(BaseBrainService):
    # ─────────────────────────────────────────────────────────────────────────
    # МЕТОД 1: Загрузка данных
    # ─────────────────────────────────────────────────────────────────────────

    async def load_my_data(self, conn_vlad, conn_brain):
        # ── 1. Коды весов ─────────────────────────────────────────────────
        res = await conn_vlad.execute(text(
            f"SELECT weight_code FROM `{WEIGHTS_TABLE}`"
        ))
        self.weight_codes.extend(
            r["weight_code"] for r in res.mappings().all()
        )

        # ── 2. Индекс контекстов ──────────────────────────────────────────
        res = await conn_vlad.execute(text(f"""
            SELECT id, occurrence_count
            FROM `{CTX_TABLE}`
        """))
        for r in res.mappings().all():
            key = (r["id"],)
            self.ctx_index[key] = {
                "occurrence_count": r["occurrence_count"] or 0
            }

        # ── 3. Сырые события ──────────────────────────────────────────────
        res = await conn_vlad.execute(text(f"""
            SELECT ctx_id, news_date
            FROM `{CTX_MAP_TABLE}`
            WHERE news_date IS NOT NULL
            ORDER BY news_date
        """))

        loaded  = 0
        skipped = 0
        for r in res.mappings().all():
            dt  = r["news_date"]
            key = (r["ctx_id"],)

            if key in self.ctx_index:
                self.register_event(dt, key)
                loaded += 1
            else:
                skipped += 1

        log(
            f"  NewsWeightsService loaded: "
            f"weight_codes={len(self.weight_codes)}, "
            f"ctx={len(self.ctx_index)}, "
            f"events={loaded}, skipped={skipped}",
            self.node_name,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # МЕТОД 2: Формирование кода веса
    # ─────────────────────────────────────────────────────────────────────────

    def make_weight_code(self, event_key, mode, shift_arg):
        ctx_id = event_key[0]
        shift  = shift_arg if shift_arg is not None else 0
        return f"NW{ctx_id}_{mode}_{shift}"


# ─────────────────────────────────────────────────────────────────────────────
# СОЗДАНИЕ И ЗАПУСК
# ─────────────────────────────────────────────────────────────────────────────

service = NewsWeightsService(
    service_id=SERVICE_ID,
    port=PORT,
    node_name=NODE_NAME,
    shift_window=24,
    recurring_min_count=2,
    filter_future_events=True,
    description=(
        "News NER-context weights. "
        "Sources: CNN, NYT, TWP, TGD, WSJ. "
        "Context = NER fingerprint (feed_cat | person | location | misc). "
        "shift_window=24h, recurring_min_count=2."
    ),
)

app = service.create_app()

if __name__ == "__main__":
    service.run()
