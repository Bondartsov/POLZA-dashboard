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


# START_BLOCK_INJECTION_DETECTION_PATTERNS
# WAVE-2 FIX: Precompile patterns and use word boundaries to prevent bypasses via spacing/capitalization
# Note: This is a first-pass heuristic. Advanced injections with obfuscation may still bypass.
_INJECTION_KEYWORDS = [
    # Russian
    "забудь", "system prompt", "системный промпт",
    "инструкция", "instruction", "execute", "выполни",
    "новые инструкции", "изменить инструкцию",
    # English variants
    "forget", "ignore", "override", "bypass", "disable", "turn off",
]

_INJECTION_PATTERNS = [
    re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE | re.UNICODE)
    for kw in _INJECTION_KEYWORDS
]
# END_BLOCK_INJECTION_DETECTION_PATTERNS


def _detect_injection_attempt(text: str) -> bool:
    """Detect common prompt injection patterns using word-boundary matching.
    
    Returns True if suspicious keywords found (case-insensitive, word boundaries).
    
    Keywords monitored:
    - Russian: "забудь", "инструкция", "выполни", "новые инструкции"
    - English: "forget", "instruction", "execute", "ignore", "system prompt"
    
    Note: This is a first-pass heuristic. Advanced injections may bypass:
    - Obfuscation (Unicode variants, spelling variations, synonyms)
    - Indirect commands (prompting for content that triggers injection)
    - Encoding (base64, ROT13, etc.)
    
    For production: Consider multi-stage detection (semantic detection via LLM, SHAP explanations).
    """
    if not text:
        return False
    
    # Check if ANY pattern matches (word boundary prevents partial matches)
    for pattern in _INJECTION_PATTERNS:
        if pattern.search(text):
            return True
    
    return False
# END_BLOCK_INJECTION_DETECTION


# START_BLOCK_PII_REDACTION_PATTERNS
# WAVE-2 FIX: Precompile regex patterns to avoid recompilation on each redaction call
_PHONE_PATTERN_RU1 = re.compile(r'\+7\s?\d{3}\s?\d{3}\s?\d{2}\s?\d{2}')
_PHONE_PATTERN_RU2 = re.compile(r'8\s?\d{3}\s?\d{3}\s?\d{2}\s?\d{2}')
_EMAIL_PATTERN = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', re.IGNORECASE)
# Note: Russian name redaction commented out as it's overly broad (high false positive rate)
# Recommendation: Use NER library (natasha, deeppavlov) for production
# _NAME_PATTERN = re.compile(r'\b[А-Я][а-я]+\s+[А-Я][а-я]+\b', re.UNICODE)
# END_BLOCK_PII_REDACTION_PATTERNS


def _redact_pii(text: str) -> str:
    """Redact personally identifiable information (PII).
    
    Patterns masked:
    - Phone numbers: +7 XXX XXX XXXX, 8 XXX XXX XXXX (Russian formats)
    - Email addresses: user@domain.com
    
    NOTE: Russian name redaction disabled due to high false positive rate.
    For production, consider using natasha (NER) or deeppavlov.
    
    Returns text with [REDACTED:type] markers.
    """
    if not text:
        return text
    
    # Phone redaction (Russian formats with optional spaces)
    text = _PHONE_PATTERN_RU1.sub('[REDACTED:phone]', text)
    text = _PHONE_PATTERN_RU2.sub('[REDACTED:phone]', text)
    
    # Email redaction
    text = _EMAIL_PATTERN.sub('[REDACTED:email]', text)
    
    # Name redaction disabled (WAVE-2: too many false positives)
    # Uncomment when using proper NER:
    # text = _NAME_PATTERN.sub('[REDACTED:name]', text)
    
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
