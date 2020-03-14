package cs223w2020.agent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cs223w2020.model.Message;

public class AgentServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    int app_port;

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

    private void processMsg(Message msg){
        System.out.println(msg);
    }

    public void start(int port) {
        app_port = port;
        System.out.println("Starting AgentServer @port: " + String.valueOf(app_port));

        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        System.out.println("Start Listen and Ready to Accpet");
        try {
            clientSocket = serverSocket.accept();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        System.out.println("\"Client\"(Coordinator) Connected");
        try {
            out = new PrintWriter(clientSocket.getOutputStream(), true);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        try {
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        System.out.println("Start recving messages");
        String recvdStr = null;
        //main loop starts
        while(true){
            try {
                recvdStr = in.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (recvdStr==null){continue;}

            Message recvMsg = deserializeStr(recvdStr);

            processMsg(recvMsg);
        }
        
    }
 
    public void stop() {
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        out.close();
        try {
            clientSocket.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
}