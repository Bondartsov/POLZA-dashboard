# Рефакторинг POLZA Dashboard — План для обсуждения

> Ветка: `refactor` | Дата: 2026-04-23 | Статус: **DRAFT**

## Проблема

`polza_dashboard.py` — **2168 строк** монолита. Содержит всё:
- Flask app + routes (40+ endpoints)
- LLM providers (Ollama, Anthropic, OpenRouter)
- Embedding pipeline (Qdrant)
- Analyze-all worker (background thread)
- Session summarizer (background thread)
- Config/env loading
- Provider state management
- Key parsing
- Prompt templates

Результат: медленная навигация, высокий риск конфликтов, сложно тестировать.

## Текущая структура (файлы проекта)

```
polza_dashboard.py   2168 строк  — ВСЁ
db.py                 395 строк  — SQLAlchemy модели + CRUD
sync_worker.py        245 строк  — SyncWorker (background)
static/app.js        1250 строк  — ВЕСЬ frontend SPA
static/style.css      490 строк  — OK
static/index.html     218 строк  — OK
```

## Предлагаемая структура

```
polza_dashboard.py        ~80 строк  — entrypoint: main(), app factory
config.py                ~100 строк  — load_env(), все globals, .env persist
providers/
  __init__.py              ~10 строк  — registry, dispatcher
  base.py                  ~20 строк  — BaseLLMProvider protocol
  ollama.py               ~120 строк  — _llm_call_ollama()
  anthropic.py            ~100 строк  — _llm_call_anthropic()
  openrouter.py           ~130 строк  — _llm_call_openrouter() + retry + models
  prompt.py                ~30 строк  — GEN_SUMMARIZE_PROMPT + _parse_llm_json
embeddings/
  __init__.py              ~10 строк
  qdrant.py               ~100 строк  — _get_qdrant_client, ensure, upsert
  embed.py                 ~40 строк  — _embed_text() через Ollama
workers/
  __init__.py              ~10 строк
  analyze_all.py          ~200 строк  — _analyze_all_worker + _analyze_single_gen
  session_summarizer.py   ~200 строк  — _summarize_all_worker + session endpoints
  auto_analyze.py          ~50 строк  — _auto_analyze_new_records
routes/
  __init__.py              ~10 строк  — register_all(app)
  generations.py          ~120 строк  — /api/db/all, /api/generations, фильтры
  summarize.py            ~150 строк  — /api/generation/summarize, batch, delete
  sessions.py             ~120 строк  — /api/sessions/*, backfill
  analyze.py              ~100 строк  — /api/analyze-all/*, /api/analysis-stats
  keys.py                  ~50 строк  — /api/keys
  sync.py                  ~40 строк  — /api/sync/*
  provider.py              ~60 строк  — /api/provider/*
  proxy.py                 ~40 строк  — /api/generations/<id>, /api/log, /api/balance
  employee.py             ~180 строк  — /api/employee-report/*
  misc.py                  ~30 строк  — /api/config, /api/health
db.py                     395 строк  — без изменений
sync_worker.py            245 строк  — без изменений
```

## Визуально — что куда

### polza_dashboard.py → entrypoint
```
Сейчас: 2168 строк (всё)
После:  ~80 строк (app factory + main)
```

### providers/ — LLM провайдеры
```
Сейчас: строки 1287-1500 (~210 строк в середине монолита)
После:  каждый провайдер в своём файле, легко добавить новый

Диспетчер:
  providers.dispatch(user_text) → выберет нужного по _provider_state
  
Новый провайдер = новый файл + 1 строка в registry
```

### embeddings/ — векторизация
```
Сейчас: строки 1512-1600 (Qdrant + embed)
После:  изолированный модуль, можно менять Qdrant на что-то другое
```

### workers/ — фоновые задачи
```
Сейчас: строки 1828-2223 (analyze-all + session summarize + auto-analyze)
После:  каждый worker в своём файле, свои thread/lock

_analyze_all_worker, _summarize_all_worker, _auto_analyze_new_records
```

### routes/ — HTTP endpoints
```
Сейчас: ~40 @app.route разбросаны по всему файлу
После:  группировка по домену (generations, sessions, provider, ...)

Blueprint pattern:
  routes/generations.py → Blueprint('generations')
  routes/summarize.py   → Blueprint('summarize')
  ...
  routes/__init__.py    → register_all(app) — подключает все blueprints
```

## app.js —要不要 пилить?

**Пока нет.** 1250 строк — это много, но SPA на vanilla JS не даёт много вариантов без bundler'а. Варианты:
1. Оставить как есть (проще деплоя — один файл)
2. Разбить на модули + `<script type="module">` (без bundler'а)
3. Переписать на Vue/React (overkill для дашборда)

**Рекомендация**: оставить как есть. Если станет 2000+ — обсуждаем.

## Что НЕ меняется

- `db.py` — уже изолирован, менять не нужно
- `sync_worker.py` — уже изолирован
- `static/style.css` — ОК
- `static/index.html` — ОК
- `.env` формат — без изменений
- API endpoints — те же URL, тот же JSON
- GRACE артефакты — обновятся после рефакторинга

## Порядок рефакторинга (пошагово)

Каждый шаг — **отдельный коммит**, сервер работает после каждого.

| Шаг | Что | Риск | Время |
|-----|-----|------|-------|
| 1 | `config.py` — вынести globals, load_env, persist | низкий | 10 мин |
| 2 | `providers/` — вынести LLM провайдеры + prompt | низкий | 15 мин |
| 3 | `embeddings/` — вынести Qdrant + embed | низкий | 10 мин |
| 4 | `routes/` — обернуть endpoints в Blueprints | средний | 30 мин |
| 5 | `workers/` — вынести background workers | низкий | 15 мин |
| 6 | `polza_dashboard.py` — почистить entrypoint | низкий | 5 мин |
| 7 | Обновить GRACE артефакты | — | 10 мин |
| 8 | Тест на сервере | — | 10 мин |

**Итого: ~1.5 часа**

## Преимущества

1. **Навигация** — `providers/openrouter.py` вместо поиска по 2168 строкам
2. **Новый провайдер** — файл + 1 строка в registry, не трогая остальное
3. **Тестирование** — можно unit-test отдельный провайдер или worker
4. **Параллельная разработка** — разные файлы = меньше merge конфликтов
5. **Читаемость** — каждый файл < 200 строк, понимается за 1 минуту

## Риски и митигация

| Риск | Митигация |
|------|-----------|
| Circular imports | Чёткие слои: config → providers → workers → routes |
| Обрыв связей между функциями | Каждый шаг — commit + тест на сервере |
| GRACE markup устареет | Обновить knowledge-graph после всех шагов |
| Blueprint namespace | `/api/*` префикс сохраняется |

## Открытые вопросы для обсуждения

1. **app.js** — пилить или оставить?
2. **Тесты** — добавить pytest параллельно с рефакторингом или после?
3. **providers/base.py** — нужен ли абстрактный класс или хватит duck typing?
4. **requirements.txt** — зафиксировать зависимости? (сейчас всё в system python)
5. **Docker** — упаковать после рефакторинга?
