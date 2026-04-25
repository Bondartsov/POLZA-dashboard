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

ФОРМАТ ОТВЕТА:
- Структурированный текст с markdown-разметкой
- Нумерованные списки для перечислений
- **Жирный** для ключевых выводов и чисел
- В конце — блок "📎 Источники" с кратким списком использованных источников

ПРИМЕРЫ ВОПРОСОВ:
- "Что делал Радько Кирилл на этой неделе?"
- "Какие темы были у команды за сегодня?"
- "Была ли подозрительная активность?"
- "Кто использовал GPT-4 и сколько потратил?"
- "Сколько запросов было за вчерашний день?"
"""


def _build_context_block(sources: list) -> str:
    """Build formatted context block from enriched sources for RAG prompt."""
    if not sources:
        return "ИСТОЧНИКИ: Данные не найдены."

    lines = ["ИСТОЧНИКИ (найдено релевантных записей: {})".format(len(sources)), ""]

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

        work_label = "Рабочая задача" if is_work else "⚠️ Подозрение на личное"
        risk_str = ", ".join(risk_flags) if risk_flags else "нет"

        # Format date to short form
        date_short = created[:16].replace("T", " ") if created else "?"

        lines.append("[Источник #{:d}] ID: {} | Сотрудник: {} | Дата: {}".format(
            i, gen_id[:12], employee, date_short
        ))
        lines.append("  Тема: {} | Классификация: {} | Релевантность: {:.0f}%".format(
            topic, work_label, score * 100
        ))
        lines.append("  Описание: {}".format(summary[:500]))
        if model:
            lines.append("  Модель: {}".format(model))
        if risk_flags:
            lines.append("  Флаги риска: {}".format(risk_str))
        lines.append("")

    return "\n".join(lines)
