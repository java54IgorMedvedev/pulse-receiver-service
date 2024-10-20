package telran.monitoring.pulse;

import java.net.*;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import telran.monitoring.pulse.dto.SensorData;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.regions.Region;

public class PulseReceiverAppl {
	private static final String REGION = "us-east-1";
    private static final int PORT = 5000;
    private static final int MAX_BUFFER_SIZE = 1500;
    private static final Logger logger = Logger.getLogger(PulseReceiverAppl.class.getName());   
    private static final String LOGGING_LEVEL = System.getenv().getOrDefault("LOGGING_LEVEL", "INFO");
    private static final int MAX_THRESHOLD_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("MAX_THRESHOLD_PULSE_VALUE", "210"));
    private static final int MIN_THRESHOLD_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("MIN_THRESHOLD_PULSE_VALUE", "40"));
    private static final int WARN_MAX_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("WARN_MAX_PULSE_VALUE", "180"));
    private static final int WARN_MIN_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("WARN_MIN_PULSE_VALUE", "55"));

    private static DatagramSocket socket;
    private static DynamoDbClient dynamoDbClient; 

    public static void main(String[] args) throws Exception {
        configureLogging();
        socket = new DatagramSocket(PORT);
        dynamoDbClient = DynamoDbClient.builder()
                .region(Region.of(REGION))
                .build(); 
        byte[] buffer = new byte[MAX_BUFFER_SIZE];

        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, MAX_BUFFER_SIZE);
            socket.receive(packet);
            processReceivedData(buffer, packet);
        }
    }

    private static void configureLogging() {
        LogManager.getLogManager().reset();
        logger.setLevel(Level.parse(LOGGING_LEVEL));
        Handler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.parse(LOGGING_LEVEL));
        logger.addHandler(consoleHandler);       
        logger.config("Logging level: " + LOGGING_LEVEL);
        logger.config("MAX_THRESHOLD_PULSE_VALUE: " + MAX_THRESHOLD_PULSE_VALUE);
        logger.config("MIN_THRESHOLD_PULSE_VALUE: " + MIN_THRESHOLD_PULSE_VALUE);
        logger.config("WARN_MAX_PULSE_VALUE: " + WARN_MAX_PULSE_VALUE);
        logger.config("WARN_MIN_PULSE_VALUE: " + WARN_MIN_PULSE_VALUE);
    }

    private static void processReceivedData(byte[] buffer, DatagramPacket packet) {
        String json = new String(Arrays.copyOf(buffer, packet.getLength()));
        SensorData data = SensorData.getSensorData(json);
        logger.fine("Received SensorData: " + data);
        
        int pulseValue = data.value();
        if (pulseValue > MAX_THRESHOLD_PULSE_VALUE) {
            logger.severe("Pulse value exceeds MAX_THRESHOLD: " + pulseValue);
        } else if (pulseValue > WARN_MAX_PULSE_VALUE) {
            logger.warning("Pulse value exceeds WARN_MAX: " + pulseValue);
        } else if (pulseValue < MIN_THRESHOLD_PULSE_VALUE) {
            logger.severe("Pulse value below MIN_THRESHOLD: " + pulseValue);
        } else if (pulseValue < WARN_MIN_PULSE_VALUE) {
            logger.warning("Pulse value below WARN_MIN: " + pulseValue);
        }

        saveToDynamoDB(data);
    }

    private static void saveToDynamoDB(SensorData data) {
        Map<String, AttributeValue> item = Map.of(
            "seqNumber", AttributeValue.builder().n(String.valueOf(data.seqNumber())).build(),
            "patientId", AttributeValue.builder().n(String.valueOf(data.patientId())).build(),
            "value", AttributeValue.builder().n(String.valueOf(data.value())).build(),
            "timestamp", AttributeValue.builder().n(String.valueOf(data.timestamp())).build()
        );

        PutItemRequest request = PutItemRequest.builder()
            .tableName("pulse_values") 
            .item(item)
            .build();

        dynamoDbClient.putItem(request);
        logger.finer("Saved SensorData to DynamoDB: " + data);
    }
}
