import { kafka } from "./client.js";


const group = process.argv[2];
async function init() {
  try {
    console.log("Connecting consumer")
    const consumer = kafka.consumer({ groupId: "user-1" });
    await consumer.connect();
    console.log("Connected to consumer")
    console.log("subscribing to consumer")
    await consumer.subscribe({ topics: ["rider-updates"], fromBeginning: true })
    console.log("subscribed to consumer")
    console.log("running consumer..")
    await consumer.run({
      eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
        console.log(`[${group}] : [${topic}] : PART:[${partition}] : ${message.value.toString()}`)
      }
    });

    console.log("consumer disconnecting ")
    // await consumer.disconnect();
    // console.log("consumer disconnected")
  } catch (err) {
    console.log(err);
  }

}
init();