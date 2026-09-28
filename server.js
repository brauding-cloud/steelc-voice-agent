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

function attachSideband(sessionId) {
  const url =
    `wss://api.openai.com/v1/live/sessions/${sessionId}/attach`;

  const ws = new WebSocket(url, {
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
  });

  ws.on("open", () => {
    console.log("Sideband connected:", sessionId);

    // GPT-Live must speak first instead of waiting for the caller.
    ws.send(
      JSON.stringify({
        type: "session.instructions.append",
        event_id: `greeting_${Date.now()}`,
        delegation_id: null,
        content: `
Greet the caller now.

Speak first immediately. Do not wait for the caller to speak.

For this initial test, greet in English.

Say only:
"Hello, STEELC. How can we help you?"

After the greeting, stop speaking and listen to the caller.

If the caller then speaks German, immediately continue in German.
If the caller speaks English, continue in English.
        `.trim(),
      })
    );

    console.log("Greeting instruction sent:", sessionId);
  });

  ws.on("message", (data) => {
    try {
      const event = JSON.parse(data.toString());

      if (event.type === "session.instructions.appended") {
        console.log("Greeting instruction accepted:", sessionId);
      }

      if (event.type === "error") {
        console.error("Sideband OpenAI error:", event);
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

      console.log("Incoming STEELC SIP call:", sessionId);

      await client.live.sessions.accept(sessionId, {
        session: {
          type: "live",
          model: "gpt-live-1",

          instructions: `
You handle incoming telephone calls for STEELC.

You are acting only as telephone reception before transferring
the caller to a human employee.

Keep all responses short, natural and professional.

LANGUAGE

Use the language spoken by the caller.

German and English are the primary languages.

If the caller speaks German, respond in German.
If the caller speaks English, respond in English.

If the caller changes language, immediately change with them.

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

If necessary, ask briefly for:
- RFQ number
- company name
- caller name

Do not conduct the business conversation yourself.

Do not discuss technical matters in detail.
Do not negotiate.
Do not quote prices.
Do not promise delivery dates.
Do not confirm orders.
Do not make decisions for STEELC.
Do not invent information.

As soon as you understand the reason for the call,
tell the caller that you will connect them with the
appropriate employee.

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

      // Attach only after OpenAI has accepted the SIP call.
      attachSideband(sessionId);

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
