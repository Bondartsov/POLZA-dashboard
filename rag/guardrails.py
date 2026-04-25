"""
M-RAG-GUARDRAILS: Security and safety guardrails for RAG.
Prompt injection detection, XML escaping, PII redaction.
"""
import re
from typing import Optional


# START_MODULE_CONTRACT
# PURPOSE: Provide security guardrails for RAG chat (injection detection, PII redaction, XML escaping)
# SCOPE: Text sanitization, injection pattern detection, PII masking
# INPUTS: text: str (user message or context)
# OUTPUTS: escaped/validated/redacted text: str, boolean flags
# DEPENDS: None (pure utility)
# LINKS: M-RAG-CHAT
# END_MODULE_CONTRACT


# START_BLOCK_XML_ESCAPE
def _escape_xml(text: str) -> str:
    """Escape XML special chars to prevent injection attacks.
    
    Converts: & < > " ' to XML entities.
    Ensures user-supplied text cannot break out of XML tags in prompt.
    """
    if not text:
        return text
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )
# END_BLOCK_XML_ESCAPE


# START_BLOCK_INJECTION_DETECTION
def _detect_injection_attempt(text: str) -> bool:
    """Detect common prompt injection patterns.
    
    Returns True if suspicious keywords found:
    - Russian: "забудь", "инструкция", "выполни", "новые инструкции"
    - English: "forget", "instruction", "execute", "ignore", "system prompt"
    
    Note: This is a first-pass heuristic. Advanced injections may bypass.
    """
    if not text:
        return False
    
    text_lower = text.lower()
    injection_keywords = [
        # Russian
        "забудь", "ignore", "system prompt", "системный промпт",
        "инструкция", "instruction", "execute", "выполни",
        "new instructions", "новые инструкции", "изменить инструкцию",
        # English variants
        "override", "bypass", "disable", "turn off",
    ]
    
    for kw in injection_keywords:
        if kw in text_lower:
            return True
    
    return False
# END_BLOCK_INJECTION_DETECTION


# START_BLOCK_PII_REDACTION
def _redact_pii(text: str) -> str:
    """Redact personally identifiable information (PII).
    
    Patterns masked:
    - Phone numbers: +7 XXX XXX XXXX, 8 XXX XXX XXXX
    - Email addresses: user@domain.com
    - Russian names: [А-Я][а-я]+ patterns (basic)
    
    Returns text with [REDACTED:type] markers.
    """
    if not text:
        return text
    
    # START_BLOCK_PHONE_REDACTION
    # Russian phone patterns
    text = re.sub(
        r'\+7\s?\d{3}\s?\d{3}\s?\d{2}\s?\d{2}',
        '[REDACTED:phone]',
        text
    )
    text = re.sub(
        r'8\s?\d{3}\s?\d{3}\s?\d{2}\s?\d{2}',
        '[REDACTED:phone]',
        text
    )
    # END_BLOCK_PHONE_REDACTION
    
    # START_BLOCK_EMAIL_REDACTION
    text = re.sub(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        '[REDACTED:email]',
        text
    )
    # END_BLOCK_EMAIL_REDACTION
    
    # START_BLOCK_NAME_REDACTION (basic Russian names)
    # Pattern: [А-Я][а-я]+ followed by space and another [А-Я][а-я]+
    # This is basic — advanced NER would be better
    text = re.sub(
        r'\b[А-Я][а-я]+\s+[А-Я][а-я]+\b',
        '[REDACTED:name]',
        text,
        flags=re.UNICODE
    )
    # END_BLOCK_NAME_REDACTION
    
    return text
# END_BLOCK_PII_REDACTION


# START_BLOCK_COMBINED_VALIDATION
def validate_and_sanitize(text: str, redact_pii: bool = False) -> tuple[str, Optional[str]]:
    """Combined validation and sanitization.
    
    Returns (sanitized_text, warning_message)
    - sanitized_text: escaped and optionally redacted
    - warning_message: None if safe, otherwise human-readable alert
    
    Args:
        text: input text
        redact_pii: if True, apply PII redaction
    
    Example:
        safe_text, warn = validate_and_sanitize(user_input)
        if warn:
            log_warning(f"Input validation: {warn}")
        use_in_prompt(safe_text)
    """
    if not text:
        return text, None
    
    # Check for injection attempts
    injection_detected = _detect_injection_attempt(text)
    warning = None
    
    if injection_detected:
        warning = "Detected potential prompt injection attempt"
    
    # Escape XML
    escaped = _escape_xml(text)
    
    # Optional PII redaction
    if redact_pii:
        escaped = _redact_pii(escaped)
    
    return escaped, warning
# END_BLOCK_COMBINED_VALIDATION
