import express from "express";
import http from "http";
import OpenAI from "openai";
import WebSocket, { WebSocketServer } from "ws";

const app = express();
const server = http.createServer(app);

const port = process.env.PORT || 10000;

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
  webhookSecret: process.env.OPENAI_WEBHOOK_SECRET,
});

const CONTROL_TOKEN = process.env.STEELC_CONTROL_TOKEN || "";

const sessions = new Map();

let controllerSocket = null;

/* -------------------------------------------------- */
/* Basic HTTP                                          */
/* -------------------------------------------------- */

app.get("/", (req, res) => {
  res.status(200).send("STEELC Voice Agent is running");
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    controllerConnected:
      controllerSocket?.readyState === WebSocket.OPEN,
    activeSessions: sessions.size,
  });
});

/* -------------------------------------------------- */
/* Helpers                                             */
/* -------------------------------------------------- */

function getCallerNumber(sipHeaders = []) {
  const fromHeader = sipHeaders.find(
    (header) =>
      String(header.name || "").toLowerCase() === "from"
  );

  if (!fromHeader?.value) {
    return null;
  }

  const value = String(fromHeader.value);

  const match = value.match(/sip:(\+\d+)/i);

  return match ? match[1] : null;
}

function getInitialLanguage(callerNumber) {
  if (!callerNumber) {
    return "en";
  }

  const germanPrefixes = [
    "+49",
    "+43",
    "+41",
  ];

  return germanPrefixes.some((prefix) =>
    callerNumber.startsWith(prefix)
  )
    ? "de"
    : "en";
}

function safeJsonParse(value) {
  if (
    value &&
    typeof value === "object"
  ) {
    return value;
  }

  if (typeof value !== "string") {
    return {};
  }

  try {
    return JSON.parse(value);
  } catch {
    return {};
  }
}

function sendController(message) {
  if (
    !controllerSocket ||
    controllerSocket.readyState !== WebSocket.OPEN
  ) {
    console.error(
      "Controller is not connected:",
      message.type
    );

    return false;
  }

  controllerSocket.send(
    JSON.stringify(message)
  );

  console.log(
    "Sent to controller:",
    message.type
  );

  return true;
}

function getSession(sessionId) {
  if (
    sessionId &&
    sessions.has(sessionId)
  ) {
    return sessions.get(sessionId);
  }

  /*
   * Only one telephone call is currently allowed
   * by the Asterisk controller, so this fallback
   * is safe for the present architecture.
   */
  const first = sessions.values().next();

  return first.done ? null : first.value;
}

function appendInstructions(
  session,
  content
) {
  if (
    !session?.sideband ||
    session.sideband.readyState !== WebSocket.OPEN
  ) {
    console.error(
      "Sideband unavailable for session",
      session?.sessionId
    );

    return;
  }

  session.sideband.send(
    JSON.stringify({
      type: "session.instructions.append",
      event_id: `instruction_${Date.now()}`,
      delegation_id: null,
      content,
    })
  );
}

/* -------------------------------------------------- */
/* Controller WebSocket                                */
/* -------------------------------------------------- */

const controllerWss =
  new WebSocketServer({
    server,
    path: "/controller",
  });

controllerWss.on(
  "connection",
  (ws, request) => {
    let token = "";

    try {
      const url = new URL(
        request.url,
        "https://steelc.local"
      );

      token =
        url.searchParams.get("token") || "";
    } catch {}

    if (
      !CONTROL_TOKEN ||
      token !== CONTROL_TOKEN
    ) {
      console.error(
        "Rejected unauthorized controller connection"
      );

      ws.close(
        1008,
        "Unauthorized"
      );

      return;
    }

    if (
      controllerSocket &&
      controllerSocket.readyState === WebSocket.OPEN
    ) {
      try {
        controllerSocket.close(
          1000,
          "Replaced"
        );
      } catch {}
    }

    controllerSocket = ws;

    console.log(
      "STEELC controller connected"
    );

    ws.send(
      JSON.stringify({
        type: "controller_ready",
      })
    );

    ws.on(
      "message",
      (raw) => {
        let message;

        try {
          message = JSON.parse(
            raw.toString()
          );
        } catch {
          console.error(
            "Invalid controller JSON"
          );

          return;
        }

        handleControllerMessage(
          message
        );
      }
    );

    ws.on(
      "close",
      () => {
        if (
          controllerSocket === ws
        ) {
          controllerSocket = null;
        }

        console.log(
          "STEELC controller disconnected"
        );
      }
    );

    ws.on(
      "error",
      (error) => {
        console.error(
          "Controller WebSocket error:",
          error.message
        );
      }
    );
  }
);

function handleControllerMessage(
  message
) {
  const session = getSession(
    message.sessionId
  );

  if (
    message.type ===
    "andrey_confirmation"
  ) {
    if (!session) {
      return;
    }

    session.mode =
      "ANDREY_CONFIRMATION";

    appendInstructions(
      session,
      `
MODE CHANGE.

You are no longer speaking to the external customer.

You are now connected ONLY to Andrey, a STEELC employee.

Speak Russian.

Immediately say only:

"Андрей, входящий звонок STEELC. Для подтверждения нажмите один."

Then stop speaking and wait.

Do not brief him yet.
Do not talk to the customer.
Do not translate.
`.trim()
    );

    return;
  }

  if (
    message.type ===
    "private_briefing"
  ) {
    if (!session) {
      return;
    }

    session.mode =
      "PRIVATE_BRIEFING";

    appendInstructions(
      session,
      `
MODE CHANGE: PRIVATE BRIEFING.

You are now connected ONLY to Andrey from STEELC.

The external caller cannot hear this conversation.

Speak Russian with Andrey.

You retain the complete context of the telephone conversation you just had with the external caller.

Immediately give Andrey a short useful briefing:
- who is calling, if known;
- company, if known;
- RFQ number, if mentioned;
- what the caller wants;
- important quantities, materials, technical points or questions that were mentioned.

Do not invent anything that the caller did not say.

After the briefing, remain available for Andrey's questions.

He may ask things such as:
- что он спрашивал?
- много спрашивал о компании?
- какое количество называл?
- какой RFQ?
- что ему нужно?
- какие технические вопросы были?

Answer from the earlier caller conversation.

IMPORTANT:
When Andrey says any clear equivalent of:
"соединяй",
"соединяйте",
"подключай",
"подключай клиента",
"можно соединять",

say:

"Соединяю."

Then immediately call the function connect_customer exactly once.

Do not begin translating yourself.
The dedicated translation system will take over after the tool call.
`.trim()
    );

    return;
  }

  if (
    message.type ===
    "back_to_customer"
  ) {
    if (!session) {
      return;
    }

    session.mode =
      "RECEPTION";

    appendInstructions(
      session,
      `
MODE CHANGE.

You are connected to the external caller again.

Continue in the caller's language.

Politely tell the caller that the colleague is currently unavailable and briefly offer to take their contact details and reason for the call.

Do not discuss prices or promise delivery dates.
`.trim()
    );

    return;
  }

  if (
    message.type ===
    "call_finished"
  ) {
    return;
  }

  console.log(
    "Unknown controller message:",
    message.type
  );
}

/* -------------------------------------------------- */
/* OpenAI tools                                        */
/* -------------------------------------------------- */

const liveTools = [
  {
    type: "function",
    name: "handoff_to_andrey",
    description:
      "Call this once the receptionist has enough information to transfer the external caller to the appropriate STEELC colleague.",
    parameters: {
      type: "object",
      properties: {
        language: {
          type: "string",
          enum: [
            "de",
            "en",
          ],
          description:
            "The actual language currently spoken by the caller.",
        },

        callerName: {
          type: "string",
          description:
            "Caller name if known, otherwise empty string.",
        },

        company: {
          type: "string",
          description:
            "Caller company if known, otherwise empty string.",
        },

        rfq: {
          type: "string",
          description:
            "RFQ number if mentioned, otherwise empty string.",
        },

        reason: {
          type: "string",
          description:
            "Short reason for the call.",
        },

        summary: {
          type: "string",
          description:
            "Concise factual summary of important information already discussed with the caller.",
        },
      },

      required: [
        "language",
        "callerName",
        "company",
        "rfq",
        "reason",
        "summary",
      ],

      additionalProperties: false,
    },
  },

  {
    type: "function",
    name: "connect_customer",
    description:
      "Use only during the private briefing with Andrey, after Andrey explicitly asks to connect the external caller.",
    parameters: {
      type: "object",
      properties: {},
      additionalProperties: false,
    },
  },
];

/* -------------------------------------------------- */
/* Function-call detection                             */
/* -------------------------------------------------- */

function collectFunctionCalls(
  value,
  found = []
) {
  if (!value) {
    return found;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      collectFunctionCalls(
        item,
        found
      );
    }

    return found;
  }

  if (
    typeof value !== "object"
  ) {
    return found;
  }

  const name =
    typeof value.name === "string"
      ? value.name
      : null;

  const isKnownFunction =
    name === "handoff_to_andrey" ||
    name === "connect_customer";

  const hasArguments =
    Object.prototype.hasOwnProperty.call(
      value,
      "arguments"
    );

  const looksLikeFunctionCall =
    value.type === "function_call" ||
    value.type === "tool_call" ||
    Boolean(value.call_id);

  if (
    isKnownFunction &&
    hasArguments &&
    looksLikeFunctionCall
  ) {
    found.push(value);
  }

  for (
    const child of Object.values(value)
  ) {
    if (
      child &&
      typeof child === "object"
    ) {
      collectFunctionCalls(
        child,
        found
      );
    }
  }

  return found;
}

function handleFunctionCall(
  session,
  call
) {
  const args =
    safeJsonParse(call.arguments);

  const callKey =
    call.call_id ||
    call.id ||
    `${call.name}:${JSON.stringify(args)}`;

  if (
    session.handledCalls.has(
      callKey
    )
  ) {
    return;
  }

  /*
   * Ignore streamed / incomplete argument JSON.
   */
  if (
    typeof call.arguments ===
      "string" &&
    call.arguments.trim() &&
    Object.keys(args).length === 0 &&
    call.arguments.trim() !== "{}"
  ) {
    return;
  }

  session.handledCalls.add(
    callKey
  );

  if (
    call.name ===
    "handoff_to_andrey"
  ) {
    if (
      session.handoffRequested
    ) {
      return;
    }

    session.handoffRequested = true;

    session.language =
      args.language === "en"
        ? "en"
        : "de";

    console.log(
      "Receptionist requested transfer:",
      session.sessionId
    );

    sendController({
      type: "reception_ready",
      sessionId:
        session.sessionId,

      language:
        session.language,

      callerName:
        args.callerName || "",

      company:
        args.company || "",

      rfq:
        args.rfq || "",

      reason:
        args.reason || "",

      summary:
        args.summary || "",
    });

    return;
  }

  if (
    call.name ===
    "connect_customer"
  ) {
    if (
      session.connectRequested
    ) {
      return;
    }

    session.connectRequested = true;

    console.log(
      "Andrey requested connection:",
      session.sessionId
    );

    sendController({
      type: "connect_customer",
      sessionId:
        session.sessionId,
    });
  }
}

/* -------------------------------------------------- */
/* OpenAI sideband                                     */
/* -------------------------------------------------- */

function attachSideband(
  sessionId,
  initialLanguage
) {
  const url =
    `wss://api.openai.com/v1/live/sessions/${sessionId}/attach`;

  const ws = new WebSocket(
    url,
    {
      headers: {
        Authorization:
          `Bearer ${process.env.OPENAI_API_KEY}`,
      },
    }
  );

  const session = {
    sessionId,
    sideband: ws,
    initialLanguage,
    language:
      initialLanguage,
    mode: "RECEPTION",
    handledCalls: new Set(),
    handoffRequested: false,
    connectRequested: false,
  };

  sessions.set(
    sessionId,
    session
  );

  ws.on(
    "open",
    () => {
      console.log(
        "Sideband connected:",
        sessionId
      );

      const greetingInstruction =
        initialLanguage === "de"
          ? `
Greet the caller now in German.
Speak first immediately.
Do not wait for the caller to speak.

Say only:

"Guten Tag, STEELC. Wie können wir Ihnen helfen?"

Then stop speaking and listen.

The caller's actual spoken language always has priority.
If the caller speaks English, immediately continue in English.
If the caller speaks German, continue in German.
`.trim()
          : `
Greet the caller now in English.
Speak first immediately.
Do not wait for the caller to speak.

Say only:

"Hello, STEELC. How can we help you?"

Then stop speaking and listen.

The caller's actual spoken language always has priority.
If the caller speaks German, immediately continue in German.
If the caller speaks English, continue in English.
`.trim();

      appendInstructions(
        session,
        greetingInstruction
      );
    }
  );

  ws.on(
    "message",
    (raw) => {
      let event;

      try {
        event = JSON.parse(
          raw.toString()
        );
      } catch {
        return;
      }

      const calls =
        collectFunctionCalls(
          event
        );

      for (const call of calls) {
        handleFunctionCall(
          session,
          call
        );
      }

      if (
        event.type === "error"
      ) {
        console.error(
          "OpenAI Live error:",
          JSON.stringify(event)
        );
      }
    }
  );

  ws.on(
    "close",
    () => {
      console.log(
        "Sideband closed:",
        sessionId
      );

      sessions.delete(
        sessionId
      );
    }
  );

  ws.on(
    "error",
    (error) => {
      console.error(
        "Sideband error:",
        sessionId,
        error.message
      );
    }
  );

  return ws;
}

/* -------------------------------------------------- */
/* OpenAI incoming SIP webhook                         */
/* -------------------------------------------------- */

app.post(
  "/webhook",
  express.raw({
    type: "application/json",
  }),

  async (
    req,
    res
  ) => {
    try {
      const event =
        await client.webhooks.unwrap(
          req.body.toString(
            "utf8"
          ),
          req.headers
        );

      res.sendStatus(200);

      if (
        event.type !==
        "live.transport.incoming"
      ) {
        return;
      }

      if (
        event.data?.type !==
          "sip" ||
        !event.data?.session_id
      ) {
        return;
      }

      const sessionId =
        event.data.session_id;

      const callerNumber =
        getCallerNumber(
          event.data.sip_headers ||
            []
        );

      const initialLanguage =
        getInitialLanguage(
          callerNumber
        );

      console.log(
        "Incoming STEELC Live session:",
        sessionId,
        "caller:",
        callerNumber ||
          "unknown",
        "initial language:",
        initialLanguage
      );

      await client.live.sessions.accept(
        sessionId,
        {
          session: {
            type: "live",

            model:
              "gpt-live-1",

            instructions: `
You handle incoming telephone calls for STEELC.

ROLE
You are the STEELC telephone receptionist before the external caller is transferred to a human STEELC employee.

Speak naturally, calmly and professionally.
Do not sound like a menu or IVR.

LANGUAGE
The application chooses an initial greeting language from the caller's telephone country code.

After the caller speaks, the caller's actual spoken language always has priority.

If the caller speaks German, respond in German.
If the caller speaks English, respond in English.
If the caller switches between German and English, switch immediately.

RECEPTION
Briefly determine why the caller is calling.

Correctly understand manufacturing terminology including:
- RFQ numbers
- quotations
- drawings
- orders
- CNC turning
- CNC turn-mill
- CNC milling
- materials
- tolerances
- DIN and EN terminology
- surface roughness
- fits
- heat treatment
- batch quantities

Understand common terms such as:
Rundlauf,
Planlauf,
Passung,
Oberflächenrauheit,
Wärmebehandlung,
Einsatzhärten,
Losgröße,
Drehteil,
Frästeil,
Ra,
Rz,
H7,
g6,
1.4301,
1.4571,
42CrMo4,
C45,
EN AW-2024,
Verzinken,
Werkstoffzeugnis.

If useful, briefly ask for:
- RFQ number;
- company;
- caller name.

Do not ask unnecessary questions.

You may naturally clarify what the caller needs, but once you have enough information to brief Andrey, stop extending the receptionist conversation.

COMMERCIAL RULES
Do not negotiate.
Do not quote prices.
Do not promise delivery dates.
Do not confirm orders.
Do not make commercial decisions.
Do not make technical commitments.
Do not invent information.

TRANSFER
Once you understand sufficiently who is calling and why:

If speaking German, say:
"Vielen Dank. Einen Moment bitte, ich verbinde Sie mit dem zuständigen Mitarbeiter."

If speaking English, say:
"Thank you. One moment please, I'll connect you with the appropriate colleague."

Say the transfer sentence BEFORE invoking the transfer tool.

Immediately after saying it, call handoff_to_andrey exactly once.

Fill the tool arguments only with information actually obtained from the caller.

After calling handoff_to_andrey, do not continue the business discussion.

If the caller speaks while waiting, respond only briefly that the connection is in progress.

PRIVATE BRIEFING
Later the server may explicitly switch you into PRIVATE BRIEFING mode.

When that happens, follow the new private-briefing instructions exactly.

The private briefing is with Andrey, not with the external caller.

You retain the entire earlier caller conversation and may answer Andrey's questions about it.
`.trim(),

            tools:
              liveTools,


            audio: {
              output: {
                voice:
                  "marin",
              },
            },
          },
        }
      );

      attachSideband(
        sessionId,
        initialLanguage
      );
    } catch (error) {
      console.error(
        "Webhook error:",
        error
      );

      if (
        !res.headersSent
      ) {
        res
          .status(400)
          .send(
            "Webhook error"
          );
      }
    }
  }
);

/* -------------------------------------------------- */
/* Start                                               */
/* -------------------------------------------------- */

server.listen(
  port,
  "0.0.0.0",
  () => {
    console.log(
      `STEELC Voice Agent listening on port ${port}`
    );

    if (!CONTROL_TOKEN) {
      console.warn(
        "STEELC_CONTROL_TOKEN is not configured yet"
      );
    }
  }
);
