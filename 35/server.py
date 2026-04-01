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
    Наследует BaseBrainService и реализует 2 обязательных метода:
        load_my_data()     — загрузка данных из БД (с предварительным обновлением индекса)
        make_weight_code() — формирование строкового кода веса

    Всю остальную логику (котировки, экстремумы, bisect, lifecycle,
    endpoints /, /weights, /values, /patch, кэш) берёт BaseBrainService.
    """

    async def load_my_data(self, conn_vlad, conn_brain) -> None:
        """
        Загружает все данные сервиса из БД в RAM.

        Вызывается при старте и каждые 3600 сек через preload_all_data().
        """

        # ── Шаг 0: инкрементальное обновление ────────────────────────────────
        log("  [Шаг 0] Инкрементальное обновление индекса...", self.node_name)
        try:
            result = await asyncio.to_thread(run_incremental_update)
            log(
                f"  [Шаг 0] OK: новых новостей={result.get('new_news', 0)}, "
                f"новых fingerprints={result.get('new_ctx', 0)}",
                self.node_name,
            )
        except Exception as e:
            log(
                f"  [Шаг 0] ⚠️ Обновление не удалось: {e} "
                f"— загружаем данные из текущего состояния таблиц",
                self.node_name,
            )

        # ── Шаг 1: коды весов ─────────────────────────────────────────────────
        log("  [Шаг 1] Загрузка weight_codes...", self.node_name)
        res = await conn_vlad.execute(text(
            f"SELECT weight_code FROM `{WEIGHTS_TABLE}`"
        ))
        self.weight_codes.extend(
            r["weight_code"] for r in res.mappings().all()
        )
        log(f"  [Шаг 1] OK: weight_codes={len(self.weight_codes)}", self.node_name)

        # ── Шаг 2: индекс контекстов ──────────────────────────────────────────
        log("  [Шаг 2] Загрузка ctx_index...", self.node_name)
        res = await conn_vlad.execute(text(f"""
            SELECT id, occurrence_count
            FROM `{CTX_TABLE}`
        """))
        for r in res.mappings().all():
            key = (r["id"],)
            self.ctx_index[key] = {
                "occurrence_count": r["occurrence_count"] or 0
            }
        log(f"  [Шаг 2] OK: ctx_index={len(self.ctx_index)} записей", self.node_name)

        # ── Шаг 3: события (маппинг news → ctx) ──────────────────────────────
        log("  [Шаг 3] Загрузка событий из ctx_map...", self.node_name)
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
                # ctx_id есть в ctx_map, но нет в ctx_index — скорее всего
                # occurrence_count < 2 и он был отфильтрован при сборке индекса.
                skipped += 1

        log(
            f"  [Шаг 3] OK: events={loaded} загружено, {skipped} пропущено "
            f"(нет в ctx_index — вероятно count < 2)",
            self.node_name,
        )

    def make_weight_code(self, event_key: tuple, mode: int,
                         shift_arg: int | None) -> str:
        """
        Собирает строку кода в формате NW{ctx_id}_{mode}_{shift}.

        Вызывается из calculate() дважды на каждое наблюдение:
        один раз для mode=0 (T1) и один раз для mode=1 (Extremum).

        Строка должна точно совпадать с weight_code в vlad_news_weights_table —

        shift_arg=None означает нерегулярное событие, подставляем 0.

            make_weight_code((42,), mode=0, shift_arg=3)    → "NW42_0_3"
            make_weight_code((42,), mode=1, shift_arg=None) → "NW42_1_0"
            make_weight_code((7,),  mode=0, shift_arg=0)    → "NW7_0_0"
        """
        ctx_id = event_key[0]
        shift  = shift_arg if shift_arg is not None else 0
        return f"NW{ctx_id}_{mode}_{shift}"


# ─────────────────────────────────────────────────────────────────────────────
# ИНИЦИАЛИЗАЦИЯ И ЗАПУСК
# ─────────────────────────────────────────────────────────────────────────────

service = NewsWeightsService(
    service_id=SERVICE_ID,
    port=PORT,
    node_name=NODE_NAME,
    # shift_window=24: ищем события в окне [-24ч, 0] от target_date.
    # Для регулярных ctx_id это даёт 25 возможных сдвигов (shift 0..24).
    # Для нерегулярных — только точное совпадение (shift=0).
    shift_window=24,
    # recurring_min_count=2: ctx_id встречался >= 2 раз → "регулярный".
    # Для нерегулярных (count=1) генерируем только 2 кода (shift=0, mode 0 и 1).
    recurring_min_count=2,
    # filter_future_events=True: новость не может быть из будущего — look-ahead bias.
    filter_future_events=True,
    description=(
        "News NER-context weights (dynamic). "
        "Sources: CNN, NYT, TWP, TGD, WSJ. "
        "Context = NER fingerprint (feed_cat | person | location | misc). "
        "Index auto-updates every 3600s via news_index_updater.py. "
        "shift_window=24h, recurring_min_count=2."
    ),
)

# create_app() создаёт FastAPI со всеми стандартными endpoints:
#   GET  /         — метаданные (version, ctx_index size, last_reload)
#   GET  /weights  — список всех weight_codes
#   GET  /values   — основной endpoint (кэш → calculate)
#   POST /patch    — обновление версии в version_microservice
app = service.create_app()

if __name__ == "__main__":
    # access_log=False убирает "GET /values?... 200 OK" из stdout — шумит при высоком RPS.
    service.run()
