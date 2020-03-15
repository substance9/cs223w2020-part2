package cs223w2020.agent;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.sql.Connection;

import cs223w2020.model.Operation;
import cs223w2020.model.Transaction;
import cs223w2020.model.Message.MessageType;
import cs223w2020.model.Message;
import cs223w2020.util.ResultFileLogger;

public class Tx2PCCohort implements Runnable 
{ 
    private int transactionId;
    private int agentId;
    private Connection logDbConnection;
    private AgentServer agentServer;

    private BlockingQueue<Message> recvMessageQueue;

    private String resultLogFileName;
    private ResultFileLogger resultLogger;

    public Tx2PCCohort(int agentId, int tid, Connection dbCon, AgentServer aServer, String resultDir){
        this.agentId = agentId;
        this.logDbConnection = dbCon;
        this.transactionId = tid;
        this.recvMessageQueue = new LinkedBlockingQueue<Message>();
        this.agentServer = aServer;

        this.resultLogFileName = resultDir + "/transactions/" + String.valueOf(this.transactionId) + "_a_" + String.valueOf(agentId) + ".txt";
        
    }



    public void processTxIn2PC(){
        resultLogger = new ResultFileLogger(this.resultLogFileName);
        resultLogger.writeln("-------------------------------------------------");
        resultLogger.writeln("Started Processing New Transaction in 2PC Protocol" + " | Tx ID: " + String.valueOf(transactionId));

        Message recvMsg = takeRecvMessage();

        while(recvMsg.type==MessageType.STATEMENT){
            resultLogger.writeln("Receive SQL Statement: " + recvMsg.sql);
            recvMsg = takeRecvMessage();
        }

        if(recvMsg.type==MessageType.PREPARE){
            resultLogger.writeln("Receive PREPARE");
            //TODO:PREPARE work
            resultLogger.writeln("PREPARED, send vote message to Coordinator");
            Message preparedMsg = new Message(MessageType.VOTEPREPARED, transactionId);
            sendMessage(preparedMsg);
        }else{
            resultLogger.writeln("ERROR: Receive Unexpected Message Type When Need PREPARE, Wrong Type: " + String.valueOf(recvMsg.type));
        }

        //wait for decision now
        resultLogger.writeln("Wait for decision now");
        Message decisionMsg = takeRecvMessage();

        if(decisionMsg.type!=MessageType.ACTCOMMIT && decisionMsg.type!=MessageType.ACTABORT){
            resultLogger.writeln("ERROR: Receive Wrong type of Message while waiting for Decision. Wrong Message Type: " + String.valueOf(decisionMsg.type));
        }else{
            Message ackMsg = new Message(MessageType.ACK, transactionId);
            if(recvMsg.type==MessageType.ACTCOMMIT){
                resultLogger.writeln("Coordinator's Decision is COMMIT");
                //TODO:COMMIT Work
                sendMessage(ackMsg);
                resultLogger.writeln("ACK is sent to Coordinator");
            }else{
                resultLogger.writeln("Coordinator's Decision is ABORT");
                sendMessage(ackMsg);
                resultLogger.writeln("ACK is sent to Coordinator");
            }
        }
        
    }

    public void addRecvMessage(Message msg){
        try {
            this.recvMessageQueue.put(msg);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public Message takeRecvMessage(){
        Message msg = null;
        try {
            msg = this.recvMessageQueue.take();
        } catch (Exception e){
            e.printStackTrace();
        }

        return msg;
    }

    public void sendMessage(Message msg){
        agentServer.addMsgToSendQueue(msg);
    }

    public void run() 
    {
        processTxIn2PC();
        resultLogger.close();
    }
} 