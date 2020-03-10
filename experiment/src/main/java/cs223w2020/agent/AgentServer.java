package cs223w2020.agent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class AgentServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    int app_port;

    public void start(int port) {
        app_port = port;
        System.out.println("Starting AgentServer @port: " + String.valueOf(app_port));

        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        System.out.println("Start Listen and Ready to Accpet");
        try {
            clientSocket = serverSocket.accept();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        System.out.println("\"Client\"(Coordinator) Connected");
        try {
            out = new PrintWriter(clientSocket.getOutputStream(), true);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        try {
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        System.out.println("Start recving messages");
        String recvdStr = "";
        while(true){
            try {
                recvdStr = in.readLine();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            System.out.println(recvdStr);
        }
        
    }
 
    public void stop() {
        try {
            in.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
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