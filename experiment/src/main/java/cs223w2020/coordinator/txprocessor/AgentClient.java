package cs223w2020.coordinator.txprocessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cs223w2020.model.Message;
import cs223w2020.model.Message.MessageType;

class AgentClient implements Runnable {
    private String host = "127.0.0.1";
    private int agentId;
    private int port;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    private TxProcessor txProcessor;

    private BlockingQueue<Message> msgSendQueue;

    public AgentClient(int id, int port, TxProcessor txProcessor) {
        this.agentId = id;
        this.port = port;
        msgSendQueue = new LinkedBlockingQueue<Message>();
        this.txProcessor = txProcessor;
    }

    public void addMsgToSendQueue(Message msg) {
        try {
            msgSendQueue.put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void startConnection() throws UnknownHostException, IOException {
        // Hardcoded IP
        String ip = host;

        clientSocket = new Socket(ip, port);

        clientSocket.setSoTimeout(1);

        out = new PrintWriter(clientSocket.getOutputStream(), true);

        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

    }

    private void sendMessage(Message msg) {
        String msgStr = serializeObj(msg);

        out.println(msgStr);
        // String resp;
        // try {
        // resp = in.readLine();
        // } catch (IOException e) {
        // e.printStackTrace();
        // }
    }

    public Message recvMessageWTimeout() {
        String resp = null;
        Message respMsg = null;

        try {
            resp = in.readLine();
        } catch (SocketTimeoutException e) {
            return null;
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (resp != null) {
            respMsg = deserializeStr(resp);
        }

        return respMsg;
    }

    public String serializeObj(Object obj) {
        ObjectMapper mapper = new ObjectMapper();
        String msg = null;
        try {
            msg = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return msg;
    }

    public Message deserializeStr(String str) {
        ObjectMapper mapper = new ObjectMapper();
        Message msg = null;
        try {
            msg = mapper.readValue(str, Message.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return msg;
    }

    public void routeRecvMsg(Message recvMsg) {
        recvMsg.agentId = this.agentId;
        MessageType msgType = recvMsg.getType();
        if (msgType == MessageType.QUERY) {
            ;
        } else {
            Tx2PCCoordinator tx2PCProcessor = this.txProcessor.syncProcessorMapGet(recvMsg.transactionId);
            tx2PCProcessor.addRecvMessage(recvMsg);
        }
    }

    public void run() {
        while (true) {
            Message sendMsg = null;
            try {
                sendMsg = msgSendQueue.poll(1, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                sendMsg = null;
            }
            if (sendMsg!=null){
                sendMessage(sendMsg);
            }
            
            Message recvMsg = null;
            recvMsg = recvMessageWTimeout();
            
            if(recvMsg != null){
                routeRecvMsg(recvMsg);
            } 
        }

    } 
} 