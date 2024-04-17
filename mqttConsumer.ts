import mqtt from 'mqtt';
import axios from 'axios';
import dotenv from 'dotenv';

dotenv.config();

export class MqttConsumer {
    private client: mqtt.MqttClient;
    private apiUrl: string = process.env.API_URL!;

    constructor() {
        this.client = mqtt.connect(process.env.MQTT_URL!, {
            username: process.env.MQTT_USERNAME,
            password: process.env.MQTT_PASSWORD
        });

        this.client.on('connect', () => {
            console.log('Connected to MQTT Broker');
            this.client.subscribe(process.env.MQTT_TOPIC!, (err) => {
                if (!err) {
                    console.log(`Subscribed to the topic ${process.env.MQTT_TOPIC}`);
                } else {
                    console.error('Subscription error:', err);
                }
            });
        });

        this.client.on('message', (topic, message) => {
            console.log(`Received message from ${topic}`);
            this.handleMessage(message);
        });
    }

    private async handleMessage(message: Buffer) {
        console.log('Raw message:', message.toString()); 
        try {
            const sensorData = JSON.parse(message.toString());
            await this.postSensorData(sensorData);
        } catch (error) {
            console.error('Failed to process sensor data:', error);
        }
    }

    private async postSensorData(sensorData: any) {
        try {
            const response = await axios.post(this.apiUrl, sensorData);
            console.log('Sensor data posted successfully:', response.data);
        } catch (error) {
            console.error('Failed to post sensor data:', error);
        }
    }
}

const mqttConsumer = new MqttConsumer();

