import { Kafka } from "kafkajs"

export const kafka = new Kafka({
  clientId: "rider-updates",
  brokers: ["192.168.0.102:9092"],
});