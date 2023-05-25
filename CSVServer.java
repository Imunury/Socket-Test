import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class CSVServer {
    private static final int SERVER_PORT = 5500; // 서버 포트 번호
    private static final String CSV_FILE_PATH = "./file.csv"; // 저장할 CSV 파일 경로 및 이름
    private static final int PACKET_SIZE = 1024; // 패킷 크기 (바이트)
    private static final Object lock = new Object();

    public static void main(String[] args) {
        try {
            // 서버 소켓 생성
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            System.out.println("서버가 시작되었습니다. 클라이언트의 연결을 기다립니다...");

            while (true) {
                // 클라이언트의 연결을 기다림
                Socket clientSocket = serverSocket.accept();
                System.out.println("클라이언트가 연결되었습니다.");

                // 데이터 전송을 위한 입출력 스트림 생성
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();

                // 응답 시퀀스 번호 초기화
                int ackSequenceNumber = 0;

                while (true) {
                    // 패킷 헤더 수신 (시퀀스 번호 + 패킷 개수)
                    byte[] header = new byte[8];
                    inputStream.read(header);

                    int sequenceNumber = ((header[0] & 0xFF) << 24)
                            | ((header[1] & 0xFF) << 16)
                            | ((header[2] & 0xFF) << 8)
                            | (header[3] & 0xFF);

                    int numPackets = ((header[4] & 0xFF) << 24)
                            | ((header[5] & 0xFF) << 16)
                            | ((header[6] & 0xFF) << 8)
                            | (header[7] & 0xFF);

                    // 패킷 데이터 수신 및 데이터 복원
                    byte[] data = receivePacketData(inputStream, numPackets);

                    // 데이터 검증 및 저장
                    boolean isValid = verifyData(data);
                    if (isValid) {
                        String csvData = new String(data);
                        synchronized (lock) {
                            saveToCSV(csvData);
                        }
                        System.out.println("데이터를 성공적으로 수신했습니다.");

                        // 응답 전송 (시퀀스 번호)
                        sendAck(outputStream, sequenceNumber);
                        ackSequenceNumber = sequenceNumber + 1;
                    } else {
                        System.out.println("전송된 데이터에 오류가 있습니다.");

                        // 응답 전송 (이전 시퀀스 번호)
                        sendAck(outputStream, ackSequenceNumber);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static byte[] receivePacketData(InputStream inputStream, int numPackets) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        for (int i = 0; i < numPackets; i++) {
            byte[] packet = new byte[PACKET_SIZE];
            int bytesRead = inputStream.read(packet);
            outputStream.write(packet, 0, bytesRead);
        }

        return outputStream.toByteArray();
    }

    private static void saveToCSV(String csvData) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(CSV_FILE_PATH, true));
            writer.write(csvData);
            writer.newLine();
            writer.close();
            System.out.println("데이터가 CSV 파일에 저장되었습니다.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void sendAck(OutputStream outputStream, int sequenceNumber) throws IOException {
        byte[] ackData = new byte[4];
        ackData[0] = (byte) ((sequenceNumber >> 24) & 0xFF);
        ackData[1] = (byte) ((sequenceNumber >> 16) & 0xFF);
        ackData[2] = (byte) ((sequenceNumber >> 8) & 0xFF);
        ackData[3] = (byte) (sequenceNumber & 0xFF);

        outputStream.write(ackData);
    }

    private static boolean verifyData(byte[] data) {
        // 데이터 검증 알고리즘을 구현해야 함
        // 필요한 경우 데이터의 해싱, 체크섬 등을 사용하여 검증

        // 예시: 데이터 길이가 0보다 큰지 확인하는 단순한 검증 예시
        return data.length > 0;
    }
}