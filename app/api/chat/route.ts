import { NextRequest } from 'next/server';

// 1. INCREASE RUNTIME LIMIT TO 60 SECONDS TO PREVENT TRUNCATION
export const runtime = 'nodejs';
export const maxDuration = 60; // Bumps max execution time from 30s to 60s on Vercel Hobby

const OPENROUTER_MODELS = [
    'openrouter/owl-alpha',
    'google/gemma-4-31b-it:free'
];

const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS, PUT, PATCH, DELETE',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, X-Title, HTTP-Referer',
};

interface JanitorMessage {
    content?: unknown;
    role?: string;
    [key: string]: unknown;
}

export async function OPTIONS() {
    return new Response(null, { status: 204, headers: corsHeaders });
}

/**
 * Helper to stream chunks instantly back to Janitor AI
 */
function sendContentChunk(controller: ReadableStreamDefaultController, text: string) {
    const encoder = new TextEncoder();
    const chunk = {
        choices: [
            {
                delta: { content: text },
                finish_reason: null,
                index: 0
            }
        ]
    };
    controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`));
}

function sendStreamError(controller: ReadableStreamDefaultController, message: string) {
    sendContentChunk(controller, `\n\n⚠️ **[Proxy Network Error]** ${message}\n\n`);
    const encoder = new TextEncoder();
    controller.enqueue(encoder.encode(`data: [DONE]\n\n`));
    controller.close();
}

function toLeetSpeak(text: string): string {
    const leetMap: Record<string, string> = {
        'a': '4', 'A': '4',
        'e': '3', 'E': '3',
        'i': '1', 'I': '1',
        'o': '0', 'O': '0',
        't': '7', 'T': '7',
        's': '5', 'S': '5'
    };
    return text.split('').map(char => leetMap[char] || char).join('');
}

export async function POST(req: NextRequest) {
    let janitorApiKey = '';
    let body: any = {};

    try {
        janitorApiKey = req.headers.get('authorization') || '';
        body = await req.json();
    } catch (err) {
        return new Response(JSON.stringify({ error: 'Malformed JSON payload' }), { status: 400, headers: corsHeaders });
    }

    const responseStream = new ReadableStream({
        async start(controller) {
            try {
                // 2. CONVERT MESSAGES TO LEETSPACE BEFORE SENDING
                if (body.messages && Array.isArray(body.messages)) {
                    body.messages = body.messages.map((msg: JanitorMessage) => {
                        if (msg.content && typeof msg.content === 'string') {
                            return { ...msg, content: toLeetSpeak(msg.content) };
                        }
                        return msg;
                    });
                }

                body.stream = true;

                // 3. CONTACT UPSTREAM MODEL
                const bluesMindsResponse = await fetch('https://api.bluesminds.com/v1/chat/completions', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': janitorApiKey,
                    },
                    body: JSON.stringify(body),
                });

                if (!bluesMindsResponse.ok || !bluesMindsResponse.body) {
                    const errorText = await bluesMindsResponse.text();
                    sendStreamError(controller, `BluesMinds rejected request: ${errorText}`);
                    return;
                }

                const reader = bluesMindsResponse.body.getReader();
                const decoder = new TextDecoder();
                let fullLeetReply = '';
                let streamBuffer = '';

                // Parsing state monitors for thinking/reasoning data splits
                let hasReasoningField = false;
                let sentThinkingStart = false;
                let passedThinkClose = false;
                let insideInlineThink = false;

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

                                // A. HANDLE EXPLICIT REASONING FIELDS (e.g. DeepSeek style structures)
                                if (reasoning) {
                                    hasReasoningField = true;
                                    if (!sentThinkingStart) {
                                        sendContentChunk(controller, "<think>\n");
                                        sentThinkingStart = true;
                                    }
                                    sendContentChunk(controller, reasoning); // Stream thinking straight to UI raw
                                }

                                // B. HANDLE STANDARD CONTENT STRINGS
                                if (content) {
                                    // If explicit reasoning text just ended, cleanly close the block
                                    if (hasReasoningField && sentThinkingStart && !passedThinkClose) {
                                        sendContentChunk(controller, "\n</think>\n");
                                        passedThinkClose = true;
                                    }

                                    // Catch text blocks that put text inside inline <think> tags natively
                                    if (content.includes('<think>')) {
                                        insideInlineThink = true;
                                        sendContentChunk(controller, content);
                                        if (content.includes('</think>')) {
                                            insideInlineThink = false;
                                            const parts = content.split('</think>');
                                            fullLeetReply += parts[1] || '';
                                        }
                                    } else if (content.includes('</think>')) {
                                        insideInlineThink = false;
                                        sendContentChunk(controller, content);
                                        const parts = content.split('</think>');
                                        fullLeetReply += parts[1] || '';
                                    } else if (insideInlineThink) {
                                        sendContentChunk(controller, content); // Direct stream raw thinking text
                                    } else {
                                        // Regular reply text (Leetspeak context) -> Buffer for OpenRouter pass
                                        fullLeetReply += content;
                                    }
                                }
                            } catch (e) {
                                // Ignore broken stream boundaries safely
                            }
                        }
                    }
                }

                // Secure edge-case safety cloaking for thinking flags
                if (hasReasoningField && sentThinkingStart && !passedThinkClose) {
                    sendContentChunk(controller, "\n</think>\n");
                }

                if (!fullLeetReply.trim()) {
                    sendStreamError(controller, "BluesMinds finished streaming but found no payload to translate.");
                    return;
                }

                // 4. FORWARD REPLAY PAYLOAD TO OPENROUTER FOR TRANSLATION
                const selectedModel = OPENROUTER_MODELS[Math.floor(Math.random() * OPENROUTER_MODELS.length)];
                const openRouterPayload = {
                    model: selectedModel,
                    messages: [
                        {
                            role: 'system',
                            content: 'You are a translation proxy. Convert the following leetspeak text back into natural, correct English prose. Maintain all markdown formatting, asterisks, bold text, newline spacing, and emojis exactly as they appear. Do not append any introductory remarks, explanations, or meta-commentary. Output only the exact translation.'
                        },
                        {
                            role: 'user',
                            content: fullLeetReply
                        }
                    ],
                    stream: true,
                };

                const openRouterResponse = await fetch('https://openrouter.ai/api/v1/chat/completions', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${process.env.OPENROUTER_API_KEY}`,
                        'HTTP-Referer': 'http://localhost:3000',
                        'X-Title': 'Janitor-Leet-Proxy',
                    },
                    body: JSON.stringify(openRouterPayload),
                });

                if (!openRouterResponse.ok || !openRouterResponse.body) {
                    const errorText = await openRouterResponse.text();
                    sendStreamError(controller, `OpenRouter (${selectedModel}) translation failed: ${errorText}`);
                    return;
                }

                // 5. STREAM THE TRANSLATED TEXT COHESIVELY INTO THE OPEN CONNECTION
                const orReader = openRouterResponse.body.getReader();
                while (true) {
                    const { done, value } = await orReader.read();
                    if (done) break;
                    controller.enqueue(value);
                }

                controller.close();
            } catch (error) {
                const errorMessage = error instanceof Error ? error.message : String(error);
                sendStreamError(controller, `Internal Pipeline Crash: ${errorMessage}`);
            }
        }
    });

    return new Response(responseStream, {
        headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            ...corsHeaders,
        },
    });
}