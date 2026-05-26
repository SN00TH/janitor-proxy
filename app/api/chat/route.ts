import { NextRequest } from 'next/server';

export const runtime = 'edge';

// OpenRouter free tier models to rotate between
const OPENROUTER_MODELS = [
  'openrouter/owl-alpha',
  'google/gemma-4-31b-it:free'
];

// Global CORS headers
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

/**
 * Handles browser preflight safety check
 */
export async function OPTIONS() {
  return new Response(null, {
    status: 204,
    headers: corsHeaders,
  });
}

/**
 * Helper to inject clean errors directly into the Janitor UI chat stream
 */
function sendStreamError(controller: ReadableStreamDefaultController, message: string) {
  const encoder = new TextEncoder();
  const errorDelta = {
    choices: [
      {
        delta: {
          content: `\n\n⚠️ **[Proxy Network Error]** ${message}\n\n`
        },
        finish_reason: "stop",
        index: 0
      }
    ]
  };
  controller.enqueue(encoder.encode(`data: ${JSON.stringify(errorDelta)}\n\n`));
  controller.enqueue(encoder.encode(`data: [DONE]\n\n`));
  controller.close();
}

/**
 * Utility to convert English characters into common Leetspeak
 */
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
  // Extract configuration parameters upfront
  let janitorApiKey = '';
  let body: any = {};
  
  try {
    janitorApiKey = req.headers.get('authorization') || '';
    body = await req.json();
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Malformed JSON payload' }), { 
      status: 400, 
      headers: corsHeaders 
    });
  }

  // CREATE AN IMMEDIATE RESPONSE STREAM
  // This opens the connection instantly, passing CORS checks and clearing the 504 watchdog timer.
  const responseStream = new ReadableStream({
    async start(controller) {
      try {
        // 1. CONVERT INCOMING MESSAGES TO LEETSPEAK
        if (body.messages && Array.isArray(body.messages)) {
          body.messages = body.messages.map((msg: JanitorMessage) => {
            if (msg.content && typeof msg.content === 'string') {
              return { ...msg, content: toLeetSpeak(msg.content) };
            }
            return msg;
          });
        }

        body.stream = true;

        // 2. FETCH FROM BLUESMINDS
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

        // 3. AGGREGATE THE BLUESMINDS CHUNKS
        const reader = bluesMindsResponse.body.getReader();
        const decoder = new TextDecoder();
        let fullLeetReply = '';
        let streamBuffer = '';

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
                const content = parsed.choices?.[0]?.delta?.content || '';
                fullLeetReply += content;
              } catch (e) {
                // Ignore split packet fragment errors
              }
            }
          }
        }

        if (!fullLeetReply.trim()) {
          sendStreamError(controller, "BluesMinds completed streaming but returned empty text.");
          return;
        }

        // 4. PICK AND PREPARE ROTATED OPENROUTER MODEL
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

        // 5. FETCH TRANSLATION FROM OPENROUTER
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

        // 6. PIPELINE OPENROUTER STREAM DIRECTLY INTO JANITOR'S OPEN CONNECTION
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

  // Returns status 200 setup instantly to Janitor AI client
  return new Response(responseStream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      ...corsHeaders,
    },
  });
}