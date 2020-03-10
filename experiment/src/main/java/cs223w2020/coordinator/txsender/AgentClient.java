package cs223w2020.coordinator.txsender;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class AgentClient implements Runnable {
    private String host = "127.0.0.1";
    private int port;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    private BlockingQueue<String> sqlQueue;

    public AgentClient(int port) {
        this.port = port;
        sqlQueue = new LinkedBlockingQueue<String>();
    }

    public void addSqlToSendQueue(String sqlStr) {
        try {
            sqlQueue.put(sqlStr);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void startConnection() throws UnknownHostException, IOException {
        // Hardcoded IP
        String ip = host;

        clientSocket = new Socket(ip, port);

        out = new PrintWriter(clientSocket.getOutputStream(), true);

        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

    }

    public String sendMessage(String msg) {
        out.println(msg);
        // String resp;
        // try {
        // resp = in.readLine();
        // } catch (IOException e) {
        // e.printStackTrace();
        // }
        return "";
    }

    public void run() {
        while (true) {
            // 1. get the transaction from queue
            String sqlStr = "";
            try {
                sqlStr = sqlQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (sqlStr.length() > 1){
                sendMessage(sqlStr);
            }
            else{
                //end mark transaction
                
                
                return;
            }
        }
    } 
} 