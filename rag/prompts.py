# FILE: rag/prompts.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
#   PURPOSE: RAG system prompt and context block builder for vector chat
#   SCOPE: System prompt template, source formatting
#   DEPENDS: None
#   LINKS: M-RAG-SEARCH
# END_MODULE_CONTRACT

RAG_SYSTEM_PROMPT = """Ты — ИИ-аналитик данных Polza.AI Dashboard. Отвечаешь на вопросы администратора \
на основе векторного поиска по всем AI-запросам команды из 12 сотрудников.

ПРАВИЛА:
1. Отвечай ТОЛЬКО на основе предоставленных источников данных (секция ИСТОЧНИКИ ниже).
2. Если данных недостаточно для ответа — честно скажи: "По вашему запросу найдено недостаточно данных."
3. После каждого утверждения ставь ссылку на источник в формате [#{номер}].
4. Отвечай на русском языке.
5. Давай конкретные числа: количество запросов, суммы, даты.
6. Группируй информацию по темам, сотрудникам или проектам.
7. Если вопрос про сотрудника — покажи полную картину его активности: темы, модели, стоимость, флаги.
8. Если у источника есть risk_flags — обязательно упомяни подозрительную активность.
9. Если у источника is_work=False — отметь это как "подозрение на личное использование".

ФОРМАТ ОТВЕТА — ДЕТАЛЬНАЯ ГЛУБОКАЯ АНАЛИТИКА:
- Не ограничивай себя в объёме ответа. Дай максимально подробный и развёрнутый анализ.
- Структурируй ответ: введение → основная часть → выводы.
- Используй markdown-разметку: заголовки (##), **жирный**, списки.
- Включай таблицы где уместно (модель | запросы | стоимость).
- Для каждого сотрудника: перечисли ВСЕ темы, ВСЕ модели, динамику по дням.
- Анализируй тренды, аномалии, паттерны поведения.
- Делай выводы и рекомендации на основе данных.
- В конце — блок "📎 Источники" с кратким списком использованных источников.

КОГДА СПРАШИВАЮТ ПРО ФИО/СПИСОК СОТРУДНИКОВ:
- Перечисли ВСЕХ найденных сотрудников с их статистикой.
- Укажи количество запросов каждого, последнюю активность, основные темы.

КОГДА СПРАШИВАЮТ ПРО РАСХОДЫ/СТОИМОСТЬ/АНАЛИТИКУ ПО КОМАНДЕ:
- Используй данные из секции "ГЛОБАЛЬНАЯ АНАЛИТИКА" — там точные цифры из БД.
- Ранжируй сотрудников по стоимости, показывай конкретные доли в %.
- Показывай какие модели самые дорогие и кто их использует.
- Давай рекомендации по оптимизации расходов.
"""


def _build_context_block(sources: list, mode: str = "search") -> str:
    """Build formatted context block from enriched sources for RAG prompt.

    Args:
        sources: list of enriched source dicts
        mode: "dossier" for full employee dump, "search" for semantic results
    """
    if not sources:
        return "ИСТОЧНИКИ: Данные не найдены."

    if mode == "dossier":
        header = "ИСТОЧНИКИ (РЕЖИМ ДОСЬЕ — полная выгрузка всех записей сотрудника, найдено: {})".format(len(sources))
    else:
        header = "ИСТОЧНИКИ (найдено релевантных записей: {})".format(len(sources))

    lines = [header, ""]

    for i, src in enumerate(sources, 1):
        gen_id = src.get("generation_id", "unknown")
        employee = src.get("employee", "Неизвестный")
        created = src.get("created_at", "")
        topic = src.get("topic", "")
        summary = src.get("summary", "(нет описания)")
        is_work = src.get("is_work", True)
        risk_flags = src.get("risk_flags", [])
        model = src.get("model", "")
        score = src.get("score", 0)
        cost = src.get("cost", 0)
        total_tokens = src.get("total_tokens", 0)
        project = src.get("project_guess", "")
        provider = src.get("provider", "")
        latency = src.get("latency_ms", 0)
        finish_reason = src.get("finish_reason", "")
        request_type = src.get("request_type", "")

        work_label = "Рабочая задача" if is_work else "⚠️ Подозрение на личное"
        risk_str = ", ".join(risk_flags) if risk_flags else "нет"

        # Format date to short form
        date_short = created[:16].replace("T", " ") if created else "?"

        lines.append("[Источник #{:d}] ID: {} | Сотрудник: {} | Дата: {}".format(
            i, gen_id[:12], employee, date_short
        ))
        details = "Тема: {} | Классификация: {} | Релевантность: {:.0f}%".format(
            topic, work_label, score * 100
        )
        if cost:
            details += " | Стоимость: {:.2f} ₽".format(cost)
        if total_tokens:
            details += " | Токенов: {:,}".format(total_tokens)
        lines.append("  " + details)
        lines.append("  Описание: {}".format(summary[:500]))
        if model:
            model_info = "Модель: {}".format(model)
            if provider:
                model_info += " | Провайдер: {}".format(provider)
            lines.append("  " + model_info)
        if project:
            lines.append("  Проект: {}".format(project))
        if request_type:
            lines.append("  Тип запроса: {} | Finish: {}".format(request_type, finish_reason or "?"))
        if latency:
            lines.append("  Латентность: {} мс".format(latency))
        if risk_flags:
            lines.append("  Флаги риска: {}".format(risk_str))
        lines.append("")

    return "\n".join(lines)
