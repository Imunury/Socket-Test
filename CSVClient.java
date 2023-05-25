import java.io.*;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class CSVClient {
    private static final String SERVER_IP = "192.168.0.66"; // 서버 IP 주소
    private static final int SERVER_PORT = 5501; // 서버 포트 번호
    private static final String CSV_FILE_PATH = "./file.csv"; // 저장할 CSV 파일 경로 및 이름
    private static final int PACKET_SIZE = 1024; // 패킷 크기 (바이트)
    private static final Object lock = new Object();

    public static void main(String[] args) {
        try {
            // 서버에 연결
            Socket socket = new Socket(SERVER_IP, SERVER_PORT);

            // 데이터 전송을 위한 입출력 스트림 생성
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();

            // 데이터 시퀀스 번호 초기화
            int sequenceNumber = 0;

            while (true) {
                // GPS 데이터와 수질 측정 센서 데이터 생성
                String gpsData = generateRandomGPSData();
                String sensorData = generateRandomSensorData();

                // CSV 데이터 생성
                String csvData = gpsData + "," + sensorData;

                // CSV 파일로 저장
                saveToCSV(csvData);

                // CSV 데이터 전송
                synchronized (lock) {
                    sendPacketData(outputStream, csvData.getBytes(), sequenceNumber);
                }

                // 응답 수신
                boolean isAckReceived = receiveAck(inputStream, sequenceNumber);

                // 데이터 전송이 성공적으로 수행되지 않은 경우, 재전송 시도
                if (!isAckReceived) {
                    continue;
                }

                // 시퀀스 번호 증가
                sequenceNumber++;

                // 10초 대기
                Thread.sleep(10 * 1000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String generateRandomGPSData() {
        Random random = new Random();
        double latitude = 37.12345 + random.nextDouble() * 0.1; // 위도 범위: 37.12345 ~ 37.22345
        double longitude = 127.12345 + random.nextDouble() * 0.1; // 경도 범위: 127.12345 ~ 127.22345
        double altitude = random.nextDouble() * 1000; // 고도 범위: 0 ~ 1000

        return latitude + "," + longitude + "," + altitude;
    }

    private static String generateRandomSensorData() {
        Random random = new Random();
        double sensor1 = random.nextDouble() * 100; // 센서1 범위: 0 ~ 100
        double sensor2 = random.nextDouble() * 100; // 센서2 범위: 0 ~ 100
        double sensor3 = random.nextDouble() * 100; // 센서3 범위: 0 ~ 100

        return sensor1 + "," + sensor2 + "," + sensor3;
    }

    private static void saveToCSV(String csvData) {
        try {
            synchronized (lock) {
                BufferedWriter writer = new BufferedWriter(new FileWriter(CSV_FILE_PATH, true));
                writer.write(csvData);
                writer.newLine();
                writer.close();
                System.out.println("데이터가 CSV 파일에 저장되었습니다.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void sendPacketData(OutputStream outputStream, byte[] data, int sequenceNumber) throws IOException {
        int dataSize = data.length;
        int numPackets = (int) Math.ceil((double) dataSize / PACKET_SIZE);

        // 패킷 헤더 생성 (시퀀스 번호 + 패킷 개수)
        byte[] header = new byte[8];
        header[0] = (byte) ((sequenceNumber >> 24) & 0xFF);
        header[1] = (byte) ((sequenceNumber >> 16) & 0xFF);
        header[2] = (byte) ((sequenceNumber >> 8) & 0xFF);
        header[3] = (byte) (sequenceNumber & 0xFF);
        header[4] = (byte) ((numPackets >> 24) & 0xFF);
        header[5] = (byte) ((numPackets >> 16) & 0xFF);
        header[6] = (byte) ((numPackets >> 8) & 0xFF);
        header[7] = (byte) (numPackets & 0xFF);

        // 패킷 전송
        outputStream.write(header);
        for (int i = 0; i < numPackets; i++) {
            int offset = i * PACKET_SIZE;
            int length = Math.min(PACKET_SIZE, dataSize - offset);
            byte[] packet = new byte[length];
            System.arraycopy(data, offset, packet, 0, length);
            outputStream.write(packet);
        }
    }

    private static boolean receiveAck(InputStream inputStream, int expectedSequenceNumber) throws IOException {
        byte[] ackData = new byte[4];
        inputStream.read(ackData);

        int sequenceNumber = ((ackData[0] & 0xFF) << 24)
                | ((ackData[1] & 0xFF) << 16)
                | ((ackData[2] & 0xFF) << 8)
                | (ackData[3] & 0xFF);

        return sequenceNumber == expectedSequenceNumber;
    }
}
