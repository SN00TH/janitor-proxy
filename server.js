const express = require('express');
const cors = require('cors');

const app = express();

// 1. AUTOMATICALLY RESOLVE ALL CORS HEADERS NATIVELY
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'X-Title', 'HTTP-Referer']
}));

app.use(express.json());

const OPENROUTER_MODELS = [
    'openrouter/owl-alpha',
    'google/gemma-4-31b-it:free'
];

function toLeetSpeak(text) {
    const leetMap = {
        'a': '4', 'A': '4', 'e': '3', 'E': '3', 'i': '1', 'I': '1',
        'o': '0', 'O': '0', 't': '7', 'T': '7', 's': '5', 'S': '5'
    };
    return text.split('').map(char => leetMap[char] || char).join('');
}

// Target endpoint for Janitor AI
app.post('/api/chat', async (req, res) => {
    // Establish an immediate, uncapped Server-Sent Events stream
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    function sendChunk(text) {
        const chunk = { choices: [{ delta: { content: text }, finish_reason: null, index: 0 }] };
        res.write(`data: ${JSON.stringify(chunk)}\n\n`);
    }

    try {
        const janitorApiKey = req.headers.authorization || '';
        const body = req.body;

        if (body.messages && Array.isArray(body.messages)) {
            body.messages = body.messages.map(msg => {
                if (msg.content && typeof msg.content === 'string') {
                    return { ...msg, content: toLeetSpeak(msg.content) };
                }
                return msg;
            });
        }
        body.stream = true;

        // 2. FETCH FROM UPSTREAM LLM
        const bluesMindsResponse = await fetch('https://api.bluesminds.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': janitorApiKey,
            },
            body: JSON.stringify(body),
        });

        if (!bluesMindsResponse.ok || !bluesMindsResponse.body) {
            const errText = await bluesMindsResponse.text();
            sendChunk(`\n\n⚠️ **[Proxy Error]** BluesMinds failed: ${errText}\n\n`);
            return res.end('data: [DONE]\n\n');
        }

        const reader = bluesMindsResponse.body.getReader();
        const decoder = new TextDecoder();
        let fullLeetReply = '';
        let streamBuffer = '';
        let hasReasoningField = false;
        let sentThinkingStart = false;
        let passedThinkClose = false;
        let insideInlineThink = false;

        // 3. READ INPUT CHUNKS AND LIVE-STREAM THINKING UNTOUCHED
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            streamBuffer += decoder.decode(value, { stream: true });
            const lines = streamBuffer.split('\n');
            streamBuffer = lines.pop() || '';

            for (const line of lines) {
                const cleanedLine = line.trim();
                if (!cleanedLine || cleanedLine === 'data: [DONE]') continue;

                if (cleanedLine.startsWith('data: ')) {
                    try {
                        const rawJson = cleanedLine.slice(6);
                        const parsed = JSON.parse(rawJson);
                        const delta = parsed.choices?.[0]?.delta;
                        if (!delta) continue;

                        const reasoning = delta.reasoning_content || '';
                        const content = delta.content || '';

                        if (reasoning) {
                            hasReasoningField = true;
                            if (!sentThinkingStart) {
                                sendChunk("<think>\n");
                                sentThinkingStart = true;
                            }
                            sendChunk(reasoning); // Stream raw thinking text straight to chat screen
                        }

                        if (content) {
                            if (hasReasoningField && sentThinkingStart && !passedThinkClose) {
                                sendChunk("\n</think>\n");
                                passedThinkClose = true;
                            }

                            if (content.includes('<think>')) {
                                insideInlineThink = true;
                                sendChunk(content);
                                if (content.includes('</think>')) {
                                    insideInlineThink = false;
                                    const parts = content.split('</think>');
                                    fullLeetReply += parts[1] || '';
                                }
                            } else if (content.includes('</think>')) {
                                insideInlineThink = false;
                                sendChunk(content);
                                const parts = content.split('</think>');
                                fullLeetReply += parts[1] || '';
                            } else if (insideInlineThink) {
                                sendChunk(content);
                            } else {
                                fullLeetReply += content; // Buffer standard leetspeak body for translation pass
                            }
                        }
                    } catch (e) { }
                }
            }
        }

        if (hasReasoningField && sentThinkingStart && !passedThinkClose) {
            sendChunk("\n</think>\n");
        }

        // 4. HAND OFF LEET CODE TO OPENROUTER
        const selectedModel = OPENROUTER_MODELS[Math.floor(Math.random() * OPENROUTER_MODELS.length)];
        const openRouterResponse = await fetch('https://openrouter.ai/api/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${process.env.OPENROUTER_API_KEY}`,
            },
            body: JSON.stringify({
                model: selectedModel,
                messages: [
                    { role: 'system', content: 'You are a translation proxy. Convert the following leetspeak text back into natural, correct English prose. Maintain all markdown formatting, asterisks, bold text, newline spacing, and emojis exactly as they appear. Do not append any introductory remarks, explanations, or meta-commentary. Output only the exact translation.' },
                    { role: 'user', content: fullLeetReply }
                ],
                stream: true,
            }),
        });

        if (!openRouterResponse.ok || !openRouterResponse.body) {
            const errText = await openRouterResponse.text();
            sendChunk(`\n\n⚠️ **[Proxy Error]** OpenRouter Translation Failed: ${errText}\n\n`);
            return res.end('data: [DONE]\n\n');
        }

        // 5. PIPE THE TRANSLATION LIVE BACK TO JANITOR AI
        const orReader = openRouterResponse.body.getReader();
        while (true) {
            const { done, value } = await orReader.read();
            if (done) break;
            res.write(value);
        }

        res.end();

    } catch (error) {
        sendChunk(`\n\n⚠️ **[Proxy Crash]** ${error.message}\n\n`);
        res.end();
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Proxy listening on port ${PORT}`));