import { NextRequest } from 'next/server';

export const runtime = 'edge';

// OpenRouter free tier models to rotate between
const OPENROUTER_MODELS = [
  'openrouter/owl-alpha',
  'google/gemma-4-31b-it:free'
];

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

/**
 * Standardized OpenAI error format so Janitor AI can read it cleanly
 */
function createErrorResponse(message: string, status: number = 500) {
  return new Response(
    JSON.stringify({
      error: {
        message: message,
        type: 'proxy_error',
        param: null,
        code: null
      }
    }),
    {
      status,
      headers: { 'Content-Type': 'application/json' }
    }
  );
}

export async function POST(req: NextRequest) {
  try {
    // 1. EXTRACT CREDENTIALS FROM JANITOR AI
    // Extracts the Bearer token/API key sent by Janitor AI
    const janitorApiKey = req.headers.get('authorization') || '';
    
    // Parses the JSON body from Janitor AI (contains messages, model, temperature, etc.)
    const body = await req.json();
    
    // Log for debugging inside Vercel console (Optional)
    console.log(`Routing request for model: ${body.model}`);

    // 2. CONVERT INCOMING MESSAGES TO LEETSPEAK
    if (body.messages && Array.isArray(body.messages)) {
      body.messages = body.messages.map((msg: any) => {
        if (msg.content && typeof msg.content === 'string') {
          return { ...msg, content: toLeetSpeak(msg.content) };
        }
        return msg;
      });
    }

    // Force streaming mode from BluesMinds so we can capture chunks dynamically
    body.stream = true;

    // 3. FORWARD TO BLUESMINDS
    // This passes the exact model and body configurations from Janitor AI,
    // and applies the Janitor AI API key straight to the Authorization header.
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
      return createErrorResponse(`BluesMinds API Error (${body.model}): ${errorText}`, bluesMindsResponse.status);
    }

    // 4. BUFFER THE BLUESMINDS STREAM
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
            // Skips partial JSON fragments split across stream chunks
          }
        }
      }
    }

    // 5. ROTATE OPENROUTER TRANSLATION MODELS
    const selectedModel = OPENROUTER_MODELS[Math.floor(Math.random() * OPENROUTER_MODELS.length)];

    // 6. CONSTRUCT OPENROUTER PAYLOAD
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

    // 7. REQUEST TRANSLATION FROM OPENROUTER
    // Uses the OPENROUTER_API_KEY environment variable you set in Vercel settings
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

    if (!openRouterResponse.ok) {
      const errorText = await openRouterResponse.text();
      return createErrorResponse(`OpenRouter Translation Error (${selectedModel}): ${errorText}`, openRouterResponse.status);
    }

    // 8. STREAM FINAL ENGLISH RESPONSE BACK TO JANITOR AI
    return new Response(openRouterResponse.body, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      },
    });

  } catch (error) {
    return createErrorResponse(`Internal Proxy Error: ${error.message}`, 500);
  }
}