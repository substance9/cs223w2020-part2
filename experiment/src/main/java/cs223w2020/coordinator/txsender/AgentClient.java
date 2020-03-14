package cs223w2020.coordinator.txsender;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cs223w2020.model.Message;

class AgentClient implements Runnable {
    private String host = "127.0.0.1";
    private int port;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    private BlockingQueue<Message> msgQueue;

    public AgentClient(int port) {
        this.port = port;
        msgQueue = new LinkedBlockingQueue<Message>();
    }

    public void addMsgToSendQueue(Message msg) {
        try {
            msgQueue.put(msg);
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

    public void sendMessage(Message msg) {
        String msgStr = serializeObj(msg);

        out.println(msgStr);
        // String resp;
        // try {
        // resp = in.readLine();
        // } catch (IOException e) {
        // e.printStackTrace();
        // }
    }

    public Message recvMessageBlocking(){
        String resp = null;
        Message respMsg = null;

        try {
            resp = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        respMsg = deserializeStr(resp);

        return respMsg;
    }

    public String serializeObj(Object obj){
        ObjectMapper mapper = new ObjectMapper();
        String msg = null;
        try {
            msg = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return msg;
    }

    public Message deserializeStr(String str){
        ObjectMapper mapper = new ObjectMapper();
        Message msg = null;
        try {
            msg = mapper.readValue(str, Message.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return msg;
    }

    public void run() {
        while (true) {
            // 1. get the transaction from queue
            Message msg = null;
            try {
                msg = msgQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (msg!=null){
                sendMessage(msg);
            }
            else{
                //end mark transaction
                
                
                return;
            }
        }
    } 
} 