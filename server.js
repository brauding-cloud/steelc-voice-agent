import express from "express";
import OpenAI from "openai";

const app = express();
const port = process.env.PORT || 10000;

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
  webhookSecret: process.env.OPENAI_WEBHOOK_SECRET,
});

// Проверка работы сервера
app.get("/", (req, res) => {
  res.status(200).send("STEELC Voice Agent is running");
});

// Webhook OpenAI
app.post(
  "/webhook",
  express.raw({ type: "application/json" }),
  async (req, res) => {
    try {
      // Проверяем подпись webhook
      const event = await client.webhooks.unwrap(
        req.body.toString("utf8"),
        req.headers
      );

      // OpenAI должен быстро получить 200 OK
      res.sendStatus(200);

      // Нас интересуют только входящие Live-звонки
      if (event.type !== "live.transport.incoming") {
        return;
      }

      if (
        event.data?.type !== "sip" ||
        !event.data?.session_id
      ) {
        console.log("Unsupported incoming transport:", event.data);
        return;
      }

      const sessionId = event.data.session_id;

      console.log("Incoming STEELC SIP call:", sessionId);

      // Принимаем входящий звонок
      await client.live.sessions.accept(sessionId, {
        session: {
          type: "live",
          model: "gpt-live-1",

          instructions: `
You are handling the incoming telephone line for STEELC.

ROLE

You are a telephone receptionist.

Your only job before transfer is to:
1. greet the caller,
2. understand briefly why they are calling,
3. collect only essential information if necessary,
4. tell the caller that you will connect them with the appropriate employee.

Do not conduct the business conversation yourself.

LANGUAGE

Speak in the language used by the caller.

German and English are the primary languages.

If the caller speaks German, respond in German.

If the caller speaks English, respond in English.

If the caller changes language during the conversation,
immediately continue in that language.

If the language is initially unclear, use English.

OPENING

Keep the opening extremely short and natural.

German:
"Guten Tag, STEELC. Wie können wir Ihnen helfen?"

English:
"Hello, STEELC. How can we help you?"

Do not introduce yourself as a digital assistant in the greeting.

Do not give a presentation about STEELC.

Do not automatically explain what STEELC manufactures.

CALLER REQUEST

Listen carefully to the caller.

Determine the reason for the call.

Correctly understand:
- RFQ numbers
- quotation requests
- drawings
- orders
- CNC turning
- CNC milling
- materials
- tolerances
- DIN and EN terminology
- technical manufacturing terminology

If necessary, you may briefly ask for:
- caller name
- company name
- RFQ number
- a short clarification of the reason for the call

Do not ask unnecessary questions.

RESTRICTIONS

Do not discuss technical matters in detail.

Do not negotiate.

Do not quote prices.

Do not promise delivery dates.

Do not confirm orders.

Do not make commercial or technical decisions.

Do not invent information.

TRANSFER

As soon as you understand why the person is calling,
stop asking questions and tell them you will connect
them with the appropriate employee.

If speaking German, say:

"Vielen Dank. Einen Moment bitte, ich verbinde Sie mit dem zuständigen Mitarbeiter."

If speaking English, say:

"Thank you. One moment please, I'll connect you with the appropriate colleague."

After saying this, do not continue discussing the customer's
business request.

Keep responses short, polite and professional.

Sound like a normal professional telephone reception.
          `.trim(),

          audio: {
            output: {
              voice: "marin",
            },
          },
        },
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
