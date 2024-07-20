import { kafka } from "./client.js"

async function init() {
  const admin = kafka.admin();
  console.log("Admin connecting...");

  await admin.connect();
  console.log("Admin connected successfully");

  console.log("Creating topic 'rider update'");
  await admin.createTopics({
    topics: [{
      topic: "rider-updates",
      numPartitions: 2,
    }]
  });

  console.log("Created topic 'rider updates' sucessfully");
  console.log("Disconnecting admin...");
  await admin.disconnect();
  console.log("admin disconnected...");
}

init();