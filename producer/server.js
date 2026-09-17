const express = require("express");
const amqp = require("amqplib");

const app = express();

app.use(express.json());

const QUEUE = "task_queue";

let channel;

// Connect to RabbitMQ
async function connectRabbitMQ() {
    try {
        const connection = await amqp.connect("amqp://localhost");

        channel = await connection.createChannel();

        await channel.assertQueue(QUEUE, {
            durable: true
        });

        console.log("Connected to RabbitMQ");
    } catch (error) {
        console.error("RabbitMQ connection failed:", error.message);

        setTimeout(connectRabbitMQ, 5000);
    }
}


// Producer API
app.post("/send", async (req, res) => {

    try {

        if (!channel) {
            return res.status(503).json({
                message: "RabbitMQ is not connected"
            });
        }

        const message = {
            id: Date.now(),
            data: req.body,
            createdAt: new Date().toISOString()
        };


        channel.sendToQueue(
            QUEUE,
            Buffer.from(JSON.stringify(message)),
            {
                persistent: true
            }
        );


        console.log("Message sent:", message);


        res.status(200).json({
            success: true,
            message: "Message sent to queue",
            data: message
        });

    } catch (error) {

        console.error(error);

        res.status(500).json({
            success: false,
            message: "Failed to send message"
        });
    }

});


// Health endpoint
app.get("/", (req, res) => {

    res.json({
        message: "Node.js Producer is running"
    });

});


const PORT = 3000;


connectRabbitMQ().then(() => {

    app.listen(PORT, () => {

        console.log(
            `Producer running at http://localhost:${PORT}`
        );

    });

});