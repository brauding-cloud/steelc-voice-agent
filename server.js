import express from "express";
import OpenAI from "openai";

const app = express();
const port = process.env.PORT || 10000;

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
  webhookSecret: process.env.OPENAI_WEBHOOK_SECRET,
});

// Health check for Render
app.get("/", (req, res) => {
  res.status(200).send("STEELC Voice Agent is running");
});

// Keep raw body for OpenAI webhook signature verification
app.post(
  "/webhook",
  express.raw({ type: "application/json" }),
  async (req, res) => {
    try {
      const event = await client.webhooks.unwrap(
        req.body.toString("utf8"),
        req.headers
      );

      // Respond to OpenAI quickly
      res.sendStatus(200);

      if (event.type !== "live.transport.incoming") {
        return;
      }

      if (event.data?.type !== "sip" || !event.data?.session_id) {
        console.log("Unsupported incoming transport");
        return;
      }

      const sessionId = event.data.session_id;

      console.log("Incoming STEELC SIP call:", sessionId);

      await client.live.sessions.accept(sessionId, {
        session: {
          type: "live",
          model: "gpt-live-1",

          instructions: `
You are the digital telephone assistant for STEELC.

At the beginning of the call, clearly identify yourself as STEELC's
digital assistant.

Speak naturally and professionally.

Automatically use the caller's language.
German and English are the primary languages.

Your initial task is reception only.

Greet the caller and ask how you can help.

If relevant, collect:
- caller name
- company name
- reason for the call
- RFQ number

STEELC specializes in CNC turning and CNC milling.

Understand common CNC terminology, materials, tolerances,
DIN and EN terminology.

Do not invent technical information.
Do not quote prices.
Do not promise delivery dates.
Do not claim that an order has been accepted.

Keep answers concise and suitable for a telephone conversation.
          `.trim(),

          audio: {
            output: {
              voice: "marin"
            }
          }
        }
      });

      console.log("Call accepted:", sessionId);
    } catch (error) {
      console.error("Webhook/call error:", error);
      if (!res.headersSent) {
        res.status(400).send("Webhook error");
      }
    }
  }
);

app.listen(port, "0.0.0.0", () => {
  console.log(`STEELC Voice Agent listening on port ${port}`);
});
