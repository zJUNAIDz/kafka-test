import { kafka } from "./client.js";
import readline from "readline"

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});




async function init() {
  try {
    const producer = kafka.producer();
    console.log("Connecting Producer");
    await producer.connect();
    console.log("Connected to producer");
    rl.setPrompt("> ");
    rl.prompt();
    rl.on("line", async (line) => {
      const [riderName, location] = line.split(' ');
      await producer.send({
        topic: "rider-updates",
        messages: [
          {
            partition: location.toLowerCase() === 'north' ? 0 : 1,
            key: 'location-updates',
            value: JSON.stringify({ name: riderName, location: location    })
          }
        ],
        // partition: 2  ,
      });
    }).on("close", async () => await producer.disconnect())
    console.log("")
    // producer.disconnect();
  } catch (err) {
    console.error(err)
  }
}
init();