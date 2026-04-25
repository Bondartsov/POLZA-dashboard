/* FILE: static/chat.js
   VERSION: 1.0.0
   PURPOSE: Self-contained chat panel for RAG conversations over Qdrant vectors
   Integration: adds button to topbar, creates panel, zero changes to app.js */

(function() {
    "use strict";

    // State
    let sessionId = null;
    let isOpen = false;
    let isStreaming = false;
    let messagesEl = null;
    let inputEl = null;
    let sendBtn = null;
    let panelEl = null;

    // --- Init ---
    function initChat() {
        // Create panel HTML
        const mount = document.getElementById("chat-panel-mount");
        if (!mount) return;

        mount.innerHTML = `
        <div class="chat-panel" id="chatPanel">
            <div class="chat-header">
                <h3>💬 Чат с данными</h3>
                <div class="chat-header-actions">
                    <button class="chat-btn-new" onclick="window._chatNew()" title="Новый чат">🔄 Новый чат</button>
                    <button class="chat-btn-close" onclick="window._chatToggle()" title="Закрыть">✕</button>
                </div>
            </div>
            <div class="chat-messages" id="chatMessages">
                <div class="chat-empty" id="chatEmpty">
                    <div class="chat-empty-icon">💬</div>
                    <div class="chat-empty-title">Спрашивайте что угодно о данных команды</div>
                    <div class="chat-empty-hint">
                        <span onclick="window._chatSuggest('Какие темы были у команды за сегодня?')">Темы за сегодня</span>
                        <span onclick="window._chatSuggest('Была ли подозрительная активность?')">Подозрительная активность</span>
                        <span onclick="window._chatSuggest('Кто больше всего потратил на AI?')">Кто больше потратил?</span>
                    </div>
                </div>
            </div>
            <div class="chat-input-bar">
                <textarea id="chatInput" placeholder="Спросите что-нибудь..." rows="1"
                    onkeydown="window._chatKeydown(event)"></textarea>
                <button class="chat-btn-send" id="chatSendBtn" onclick="window._chatSend()">➤</button>
            </div>
        </div>`;

        // Cache refs
        panelEl = document.getElementById("chatPanel");
        messagesEl = document.getElementById("chatMessages");
        inputEl = document.getElementById("chatInput");
        sendBtn = document.getElementById("chatSendBtn");

        // Add topbar button
        addTopbarButton();

        // Auto-resize textarea
        if (inputEl) {
            inputEl.addEventListener("input", function() {
                this.style.height = "auto";
                this.style.height = Math.min(this.scrollHeight, 100) + "px";
            });
        }

        // Create session
        createSession();
    }

    function addTopbarButton() {
        const topbar = document.querySelector(".topbar, header, .header, #topbar");
        if (!topbar) {
            // Fallback: try to find any suitable container
            const body = document.body;
            const btn = document.createElement("button");
            btn.className = "chat-topbar-btn";
            btn.innerHTML = "💬 Спросить ИИ";
            btn.onclick = function() { window._chatToggle(); };
            btn.style.cssText = "position:fixed;top:10px;right:10px;z-index:9999;";
            body.appendChild(btn);
            return;
        }
        const btn = document.createElement("button");
        btn.className = "chat-topbar-btn";
        btn.innerHTML = "💬 Спросить ИИ";
        btn.onclick = function() { window._chatToggle(); };
        topbar.appendChild(btn);
    }

    // --- Session ---
    async function createSession() {
        try {
            const r = await fetch("/api/chat/new", { method: "POST" });
            const d = await r.json();
            sessionId = d.session_id;
        } catch (e) {
            console.error("[ChatUI] session create failed:", e);
            sessionId = "fallback-" + Date.now();
        }
    }

    // --- Toggle ---
    window._chatToggle = function() {
        if (!panelEl) return;
        isOpen = !isOpen;
        if (isOpen) {
            panelEl.classList.add("open");
            if (inputEl) inputEl.focus();
        } else {
            panelEl.classList.remove("open");
        }
    };

    // --- New Chat ---
    window._chatNew = async function() {
        if (isStreaming) return;
        // Clear messages
        if (messagesEl) {
            messagesEl.innerHTML = `
                <div class="chat-empty" id="chatEmpty">
                    <div class="chat-empty-icon">💬</div>
                    <div class="chat-empty-title">Спрашивайте что угодно о данных команды</div>
                    <div class="chat-empty-hint">
                        <span onclick="window._chatSuggest('Какие темы были у команды за сегодня?')">Темы за сегодня</span>
                        <span onclick="window._chatSuggest('Была ли подозрительная активность?')">Подозрительная активность</span>
                    </div>
                </div>`;
        }
        await createSession();
    };

    // --- Suggest ---
    window._chatSuggest = function(text) {
        if (inputEl) {
            inputEl.value = text;
            window._chatSend();
        }
    };

    // --- Keydown ---
    window._chatKeydown = function(e) {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            window._chatSend();
        }
    };

    // --- Send ---
    window._chatSend = async function() {
        if (isStreaming || !inputEl) return;
        const text = inputEl.value.trim();
        if (!text) return;

        inputEl.value = "";
        inputEl.style.height = "auto";
        isStreaming = true;
        if (sendBtn) sendBtn.disabled = true;

        // Hide empty state
        const empty = document.getElementById("chatEmpty");
        if (empty) empty.remove();

        // Add user message
        appendUserMsg(text);

        // Add AI message placeholder
        const aiDiv = appendAiMsgStart();

        try {
            const r = await fetch("/api/chat/message", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ session_id: sessionId, message: text }),
            });

            if (!r.ok) {
                throw new Error("HTTP " + r.status);
            }

            const reader = r.body.getReader();
            const decoder = new TextDecoder();
            let buffer = "";
            let sourcesShown = false;

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split("\n");
                buffer = lines.pop() || "";

                for (const line of lines) {
                    if (!line.startsWith("data: ")) continue;
                    const dataStr = line.slice(6).trim();
                    if (!dataStr) continue;

                    let evt;
                    try { evt = JSON.parse(dataStr); } catch { continue; }

                    if (evt.type === "sources") {
                        renderSourcesBar(aiDiv, evt.count, evt.data, evt.mode);
                        sourcesShown = true;
                    } else if (evt.type === "token") {
                        appendToken(aiDiv, evt.content);
                    } else if (evt.type === "done") {
                        // Finalize
                    } else if (evt.type === "error") {
                        appendError(aiDiv, evt.message);
                    }
                }
            }

            // Finalize message
            finalizeAiMsg(aiDiv);

        } catch (e) {
            appendError(aiDiv, "Ошибка: " + e.message);
        } finally {
            isStreaming = false;
            if (sendBtn) sendBtn.disabled = false;
            if (inputEl) inputEl.focus();
        }
    };

    // --- Render helpers ---
    function appendUserMsg(text) {
        const div = document.createElement("div");
        div.className = "chat-msg chat-msg-user";
        div.textContent = text;
        messagesEl.appendChild(div);
        scrollToBottom();
    }

    function appendAiMsgStart() {
        const div = document.createElement("div");
        div.className = "chat-msg chat-msg-ai";
        div.innerHTML = '<div class="chat-typing"><div class="chat-typing-dot"></div><div class="chat-typing-dot"></div><div class="chat-typing-dot"></div></div>';
        div._content = "";  // Track raw content
        messagesEl.appendChild(div);
        scrollToBottom();
        return div;
    }

    function renderSourcesBar(aiDiv, count, sources, mode) {
        // Remove typing indicator
        const typing = aiDiv.querySelector(".chat-typing");
        if (typing) typing.remove();

        const bar = document.createElement("div");
        bar.className = "chat-sources-bar";

        const label = document.createElement("div");
        label.className = "sources-label sources-toggle";
        const modeTag = mode === "dossier" ? ' 📋 Досье' : mode === "employee_list" ? ' 👥 Сотрудники' : mode === "global_agg" ? ' 📊 Аналитика' : '';
        const arrow = "▶";
        label.textContent = arrow + " 💡 Найдено " + count + " источников" + modeTag;
        bar.appendChild(label);

        if (sources && sources.length > 0) {
            const chips = document.createElement("div");
            chips.className = "chat-source-chips sources-collapsed";
            sources.slice(0, 15).forEach(function(s) {
                const chip = document.createElement("span");
                chip.className = "chat-source-chip";
                chip.textContent = (s.topic || s.id || "src").substring(0, 30);
                chip.title = (s.employee || "") + " | score: " + (s.score || 0);
                chip.onclick = function(e) {
                    e.stopPropagation();
                    if (typeof window.openDetail === "function") {
                        window.openDetail(s.id);
                    }
                };
                chips.appendChild(chip);
            });
            bar.appendChild(chips);

            // Toggle collapse/expand on label click
            label.onclick = function() {
                const isCollapsed = chips.classList.contains("sources-collapsed");
                if (isCollapsed) {
                    chips.classList.remove("sources-collapsed");
                    chips.classList.add("sources-expanded");
                    label.textContent = "▼" + label.textContent.substring(1);
                } else {
                    chips.classList.remove("sources-expanded");
                    chips.classList.add("sources-collapsed");
                    label.textContent = arrow + label.textContent.substring(1);
                }
            };
        }

        aiDiv.appendChild(bar);
        aiDiv._sourcesBar = bar;
        scrollToBottom();
    }

    function appendToken(aiDiv, token) {
        // Remove typing indicator on first token
        const typing = aiDiv.querySelector(".chat-typing");
        if (typing) typing.remove();

        aiDiv._content += token;

        // Render markdown-lite
        const contentDiv = aiDiv._contentDiv;
        if (!contentDiv) {
            const d = document.createElement("div");
            d.className = "chat-ai-content";
            aiDiv.appendChild(d);
            aiDiv._contentDiv = d;
        }

        aiDiv._contentDiv.innerHTML = renderMarkdown(aiDiv._content);
        scrollToBottom();
    }

    function appendError(aiDiv, message) {
        const typing = aiDiv.querySelector(".chat-typing");
        if (typing) typing.remove();
        const err = document.createElement("div");
        err.className = "chat-error";
        err.textContent = "⚠️ " + message;
        aiDiv.appendChild(err);
    }

    function finalizeAiMsg(aiDiv) {
        // Nothing special needed — content already rendered
        scrollToBottom();
    }

    function renderMarkdown(text) {
        // Basic markdown: bold, lists, headers
        let html = text
            // Escape HTML
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            // Bold
            .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
            // Headers (### ## #)
            .replace(/^### (.+)$/gm, "<strong style='font-size:13px'>$1</strong>")
            .replace(/^## (.+)$/gm, "<strong style='font-size:14px'>$1</strong>")
            .replace(/^# (.+)$/gm, "<strong style='font-size:15px'>$1</strong>")
            // Numbered lists
            .replace(/^(\d+)\. (.+)$/gm, "<br>$1. $2")
            // Bullet lists
            .replace(/^[-*] (.+)$/gm, "<br>• $1")
            // Line breaks
            .replace(/\n/g, "<br>");

        return html;
    }

    function scrollToBottom() {
        if (messagesEl) {
            messagesEl.scrollTop = messagesEl.scrollHeight;
        }
    }

    // --- Boot ---
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initChat);
    } else {
        initChat();
    }
})();
