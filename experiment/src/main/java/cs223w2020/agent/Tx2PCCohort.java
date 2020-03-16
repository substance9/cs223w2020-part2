package cs223w2020.agent;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import cs223w2020.agent.AgentServer.HashMapSyncOp;
import cs223w2020.agent.CohortTxLogger.CohortTxState;
import cs223w2020.model.Message;
import cs223w2020.model.Message.MessageType;
import cs223w2020.util.ResultFileLogger;

public class Tx2PCCohort implements Runnable 
{ 
    private int transactionId;
    private int agentId;
    private Connection dataDbConnection;
    private Connection logDbConnection;
    private Connection dataDbTxControlCon;
    private AgentServer agentServer;

    private BlockingQueue<Message> recvMessageQueue;

    private String resultLogFileName;
    private ResultFileLogger resultLogger;
    private CohortTxLogger cohortTxLogger;
    private TxExecutor txExecutor;

    private long prepareTimeoutMs = 3000;
    private long decisionTimeoutMs = 5000;

    public Tx2PCCohort(int agentId, int tid, Connection logDbCon, Connection dataDbCon, Connection controlCon, AgentServer aServer, String resultDir){
        this.agentId = agentId;
        this.dataDbConnection = dataDbCon;
        this.dataDbTxControlCon = controlCon;
        this.logDbConnection = logDbCon;
        this.transactionId = tid;
        this.recvMessageQueue = new LinkedBlockingQueue<Message>();
        this.agentServer = aServer;

        this.resultLogFileName = resultDir + "/transactions/" + String.valueOf(this.transactionId) + "_a_" + String.valueOf(agentId) + ".txt";        
    }

    private Message statementPhase(){
        Message recvMsg = takeRecvMessageWTimeout(prepareTimeoutMs);
        if (recvMsg == null){
            return recvMsg;
        }

        while(recvMsg.type==MessageType.STATEMENT){
            resultLogger.writeln("Receive SQL Statement: " + recvMsg.sql);
            int numRowsAffected = txExecutor.executeStatement(recvMsg.sql);
            if (numRowsAffected != 1){
                resultLogger.writeln("Error: Num of affected rows for insertion is not 1, it is " + String.valueOf(numRowsAffected) + " instead");
            }

            recvMsg = takeRecvMessageWTimeout(prepareTimeoutMs);
            if (recvMsg == null){
                return recvMsg;
            }
        }

        return recvMsg;
    }

    private void sendAbortMsg(){
        Message abortMsg = new Message(MessageType.VOTEABORT, transactionId);
        sendMessage(abortMsg);
    }

    private boolean preparePhase(Message prepareMsg){
        if(prepareMsg.type==MessageType.PREPARE){
            resultLogger.writeln("Receive PREPARE");
            //PREPARE work
            if (txExecutor.prepareTransaction() != true){
                resultLogger.writeln("PREPARE Fail, send vote (ABORT) message to Coordinator");
                return false;
            } else {
                resultLogger.writeln("PREPARED, Log PREPARED state first");
                //PREPARED force log
                int numLogsInserted = cohortTxLogger.writeTxLog(CohortTxState.PREPARED);
                if (numLogsInserted != 1){
                    resultLogger.writeln("Log PREPARED write failed");
                    return false;
                }else{
                    resultLogger.writeln("Send vote (PREPARED) message to Coordinator");
                    Message preparedMsg = new Message(MessageType.VOTEPREPARED, transactionId);
                    sendMessage(preparedMsg);
                    return true;
                }
            }
        }else{
            resultLogger.writeln("ERROR: Receive Unexpected Message Type When Need PREPARE, Wrong Type: " + String.valueOf(prepareMsg.type));
            return false;
        }
    }

    private void sendQueryMsg(){
        Message qMsg = new Message(MessageType.QUERY, transactionId);
        sendMessage(qMsg);
    }

    public void waitAndExecuteDecision(){
        //wait for decision now
        resultLogger.writeln("Wait for decision now");
        Message decisionMsg = null;
        while(decisionMsg == null){
            //blocking, waiting for transaction decision, send query if response timeout
            decisionMsg = takeRecvMessageWTimeout(decisionTimeoutMs);
            if (decisionMsg == null){
                //timeout, send query message
                sendQueryMsg();
            }
        }

        if(decisionMsg.type!=MessageType.ACTCOMMIT && decisionMsg.type!=MessageType.ACTABORT){
            resultLogger.writeln("ERROR: Receive Wrong type of Message while waiting for Decision. Wrong Message Type: " + String.valueOf(decisionMsg.type));
        }else{
            Message ackMsg = new Message(MessageType.ACK, transactionId);
            if(decisionMsg.type==MessageType.ACTCOMMIT){
                resultLogger.writeln("Coordinator's Decision is COMMIT");
                //COMMIT Work
                if (txExecutor.commitTransaction() != true){
                    resultLogger.writeln("Commit Fail");
                } else {
                    resultLogger.writeln("Commit Success, Write Completion Log First");
                    int numLogsInserted = cohortTxLogger.writeTxLog(CohortTxState.COMPLETED);
                    if (numLogsInserted == 1){
                        sendMessage(ackMsg);
                        resultLogger.writeln("ACK is sent to Coordinator");
                    } else {
                        resultLogger.writeln("Log PREPARED write failed");
                    }
                }
            }else if(decisionMsg.type==MessageType.ACTABORT) {
                resultLogger.writeln("Coordinator's Decision is ABORT");
                if (txExecutor.rollbackTransaction() != true){
                    resultLogger.writeln("Rollback Fail");
                }else{
                    sendMessage(ackMsg);
                    resultLogger.writeln("ACK is sent to Coordinator");
                }
                
            }else{
                resultLogger.writeln("ERROR: Receive Wrong type of Message while waiting for Decision. Wrong Message Type: " + String.valueOf(decisionMsg.type));
            }
        }        
    }
    

    public void processTxIn2PC(){
        resultLogger = new ResultFileLogger(this.resultLogFileName);
        cohortTxLogger = new CohortTxLogger(transactionId, logDbConnection);
        txExecutor = new TxExecutor(transactionId, dataDbConnection, dataDbTxControlCon);
        resultLogger.writeln("-------------------------------------------------");
        resultLogger.writeln("Started Processing New Transaction in 2PC Protocol" + " | Tx ID: " + String.valueOf(transactionId));

        Message lastRecvMsg = statementPhase();
        if (lastRecvMsg == null){
            // prepare/statement timeout happens
            // abort
            sendAbortMsg();
            return;
        } else{
            boolean isPrepared = preparePhase(lastRecvMsg);
            if (isPrepared == false){
                sendAbortMsg();
                return;
            } else{
                waitAndExecuteDecision();
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

    public Message takeRecvMessageWTimeout(long timeoutMs) {
        Message msg = null;
        try {
            msg = this.recvMessageQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
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

        try {
            dataDbConnection.close();
            logDbConnection.close();
            dataDbTxControlCon.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        agentServer.syncChangeToTxCohortMap(HashMapSyncOp.REMOVE, transactionId, null);
    }
} 