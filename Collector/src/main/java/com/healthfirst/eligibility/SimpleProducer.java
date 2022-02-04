package com.healthfirst.eligibility;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimpleProducer {

	KafkaProducer<String, String> producer = null;
	private static final Logger logger = LogManager.getLogger(HandlerMSK.class);
	

	public SimpleProducer() {
		// TODO Auto-generated constructor stub
		logger.info("in Constructor..");
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "b-1.ods-kafka.v06nbd.c12.kafka.us-east-1.amazonaws.com:9098,"
				+ "b-2.ods-kafka.v06nbd.c12.kafka.us-east-1.amazonaws.com:9098,"
				+ "b-3.ods-kafka.v06nbd.c12.kafka.us-east-1.amazonaws.com:9098");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/home/hadoop/eligibility/kafka.client.keystore.jks");
		props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/home/hadoop/eligibility/cacerts");
		props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "kafka123");
		props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,"kafka123");
		producer = new KafkaProducer<String, String>(props);
		logger.info("Complete Constructor..");
		
	}

	public void publish(String topic, String strData) throws InterruptedException {
		ProducerRecord<String, String> data;
		data = new ProducerRecord<String, String>(topic, strData);
		logger.info("Before Send..");
		logger.info(topic);
		logger.info(strData);
		producer.send(data);
		producer.close();
		logger.info("After Send..");
		
	}
}