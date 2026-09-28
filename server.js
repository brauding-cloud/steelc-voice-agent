import express from "express";
import OpenAI from "openai";
import WebSocket from "ws";

const app = express();
const port = process.env.PORT || 10000;

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
  webhookSecret: process.env.OPENAI_WEBHOOK_SECRET,
});

app.get("/", (req, res) => {
  res.status(200).send("STEELC Voice Agent is running");
});

function getCallerNumber(sipHeaders = []) {
  const fromHeader = sipHeaders.find(
    (header) =>
      String(header.name || "").toLowerCase() === "from"
  );

  if (!fromHeader?.value) {
    return null;
  }

  const value = String(fromHeader.value);

  // Examples:
  // sip:+49123456789@example.com
  // <sip:+49123456789@example.com>
  const match = value.match(/sip:(\+\d+)/i);

  return match ? match[1] : null;
}

function getInitialLanguage(callerNumber) {
  if (!callerNumber) {
    return "en";
  }

  const germanPrefixes = [
    "+49", // Germany
    "+43", // Austria
    "+41", // Switzerland
  ];

  return germanPrefixes.some((prefix) =>
    callerNumber.startsWith(prefix)
  )
    ? "de"
    : "en";
}

function attachSideband(sessionId, initialLanguage) {
  const url =
    `wss://api.openai.com/v1/live/sessions/${sessionId}/attach`;

  const ws = new WebSocket(url, {
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
  });

  ws.on("open", () => {
    console.log("Sideband connected:", sessionId);

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

    ws.send(
      JSON.stringify({
        type: "session.instructions.append",
        event_id: `greeting_${Date.now()}`,
        delegation_id: null,
        content: greetingInstruction,
      })
    );

    console.log(
      "Greeting instruction sent:",
      sessionId,
      "language:",
      initialLanguage
    );
  });

  ws.on("message", (data) => {
    try {
      const event = JSON.parse(data.toString());

      if (event.type === "session.instructions.appended") {
        console.log(
          "Greeting instruction accepted:",
          sessionId
        );
      }

      if (event.type === "error") {
        console.error(
          "Sideband OpenAI error:",
          JSON.stringify(event)
        );
      }
    } catch (error) {
      console.error("Sideband message error:", error);
    }
  });

  ws.on("error", (error) => {
    console.error("Sideband WebSocket error:", error);
  });

  ws.on("close", (code, reason) => {
    console.log(
      "Sideband closed:",
      sessionId,
      code,
      reason.toString()
    );
  });

  return ws;
}

app.post(
  "/webhook",
  express.raw({ type: "application/json" }),
  async (req, res) => {
    try {
      const event = await client.webhooks.unwrap(
        req.body.toString("utf8"),
        req.headers
      );

      res.sendStatus(200);

      if (event.type !== "live.transport.incoming") {
        return;
      }

      if (
        event.data?.type !== "sip" ||
        !event.data?.session_id
      ) {
        console.log(
          "Unsupported incoming transport:",
          event.data
        );
        return;
      }

      const sessionId = event.data.session_id;

      const callerNumber = getCallerNumber(
        event.data.sip_headers || []
      );

      const initialLanguage =
        getInitialLanguage(callerNumber);

      console.log(
        "Incoming STEELC SIP call:",
        sessionId
      );

      console.log(
        "Caller:",
        callerNumber || "unknown",
        "initial language:",
        initialLanguage
      );

      await client.live.sessions.accept(sessionId, {
        session: {
          type: "live",
          model: "gpt-live-1",

          instructions: `
You handle incoming telephone calls for STEELC.

ROLE

You are telephone reception before the caller is transferred
to a human STEELC employee.

Keep responses short, natural and professional.

LANGUAGE

The application chooses an initial greeting language based
only on the caller's telephone country code.

After the caller speaks, their actual spoken language
always has priority.

If the caller speaks German, respond in German.

If the caller speaks English, respond in English.

If the caller changes between German and English,
immediately change with them.

CALL HANDLING

Briefly determine why the person is calling.

Correctly understand:
- RFQ numbers
- quotations
- drawings
- orders
- CNC turning
- CNC milling
- materials
- tolerances
- DIN and EN terminology
- common manufacturing terminology

If necessary, briefly ask for:
- RFQ number
- company name
- caller name

Do not ask unnecessary questions.

Once you have enough information to tell a human employee
who is calling and why, stop asking questions.

Do not conduct the business conversation yourself.

Do not discuss technical matters in detail.
Do not negotiate.
Do not quote prices.
Do not promise delivery dates.
Do not confirm orders.
Do not make commercial or technical decisions.
Do not invent information.

TRANSFER

When you understand the reason for the call:

German:
"Vielen Dank. Einen Moment bitte, ich verbinde Sie mit dem zuständigen Mitarbeiter."

English:
"Thank you. One moment please, I'll connect you with the appropriate colleague."

After saying this, do not continue discussing the business matter.
          `.trim(),

          audio: {
            output: {
              voice: "marin",
            },
          },
        },
      });

      console.log("Call accepted:", sessionId);

      attachSideband(
        sessionId,
        initialLanguage
      );

    } catch (error) {
      console.error("Webhook/call error:", error);

      if (!res.headersSent) {
        res.status(400).send("Webhook error");
      }
    }
  }
);

app.listen(port, "0.0.0.0", () => {
  console.log(
    `STEELC Voice Agent listening on port ${port}`
  );
});
