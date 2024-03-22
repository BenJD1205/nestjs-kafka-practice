import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka } from 'kafkajs';
import { IConsumer } from './consumer.interface';
import { KafkaConsumerOptions } from './kafka-consumer-options.interface';
import { KafkaConsumer } from './kafka.consumer';

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
  constructor(private readonly configService: ConfigService) {}
  private readonly kafka = new Kafka({
    brokers: ['localhost:9092'],
  });
  private readonly consumers: IConsumer[] = [];

  async consume({ topic, config, onMessage }: KafkaConsumerOptions) {
    const consumer = new KafkaConsumer(
      topic,
      config,
      this.configService.get('KAFKA_BROKER'),
    );
    await consumer.connect();
    await consumer.consume(onMessage);
    this.consumers.push(consumer);
  }

  async onApplicationShutdown() {
    for (const consumer of this.consumers) {
      consumer.disconnect();
    }
  }
}
