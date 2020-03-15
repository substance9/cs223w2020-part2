package cs223w2020.coordinator.txprocessor;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import cs223w2020.coordinator.txprocessor.ProtocolDbTxEntry.CohortsState;
import cs223w2020.coordinator.txprocessor.ProtocolDbTxEntry.CoordinatorState;
import cs223w2020.coordinator.txprocessor.ProtocolDbTxEntry.Decision;
import cs223w2020.model.Message;
import cs223w2020.model.Message.MessageType;
import cs223w2020.model.Operation;
import cs223w2020.model.Transaction;
import cs223w2020.util.ResultFileLogger;

public class Tx2PCCoordinator implements Runnable 
{ 
    private Transaction transaction;
    private int numAgents;
    private ArrayList<AgentClient> agentClientList;
    private Connection logDbConnection;

    public ProtocolDbTxEntry protocolDbTxEntry;

    private BlockingQueue<Message> recvMessageQueue;

    private String resultLogFileName;
    private ResultFileLogger resultLogger;

    public Tx2PCCoordinator(Transaction transaction, ProtocolDbTxEntry txEntry, int numAgents, ArrayList<AgentClient> agentCList, Connection dbCon, String resultDir){
        this.numAgents=numAgents;
        this.agentClientList = agentCList;
        this.transaction = transaction;
        this.logDbConnection = dbCon;

        this.recvMessageQueue = new LinkedBlockingQueue<Message>();

        this.protocolDbTxEntry = txEntry;

        this.resultLogFileName = resultDir + "/transactions/" + String.valueOf(transaction.transactionId) + "_c.txt";
    }


    public int getCorrespondAgentId(Operation op){
        int opHashCode = op.sensorId.hashCode() + op.timestamp.hashCode();
        if (opHashCode < 0){
            opHashCode = 0 - opHashCode;
        }
        return (opHashCode % numAgents);
    }

    public void processTxIn2PC(Transaction tx){
        resultLogger = new ResultFileLogger(this.resultLogFileName);
        resultLogger.writeln("-------------------------------------------------");
        resultLogger.writeln("Started Processing New Transaction in 2PC Protocol" + " | Tx ID: " + String.valueOf(tx.transactionId));
        resultLogger.writeln(tx.toString());

        Message startMsg = new Message(Message.MessageType.START, tx.transactionId);

        for (int i = 0; i < tx.operations.size(); i ++){
            Operation op = tx.operations.get(i);
            int agentId = getCorrespondAgentId(op);
            AgentClient aclient = agentClientList.get(agentId);
            if(!protocolDbTxEntry.CohortsStateMap.containsKey(agentId)){
                protocolDbTxEntry.CohortsStateMap.put(agentId, CohortsState.INITIATED);
                protocolDbTxEntry.numOfCohorts = protocolDbTxEntry.numOfCohorts + 1;
                aclient.addMsgToSendQueue(startMsg);
            }

            Message stnMsg = new Message(Message.MessageType.STATEMENT, tx.transactionId);
            stnMsg.setSql(op.sqlStr);
            aclient.addMsgToSendQueue(stnMsg);
            resultLogger.writeln("SQL Statement Sent to Agent "+ String.valueOf(agentId) + ": " + op.sqlStr);
        }

        //Operations are all sent, Start Preparing
        resultLogger.writeln("SQL Statements All Sent to Agent, Start Preparing");
        resultLogger.writeln("There are " + String.valueOf(protocolDbTxEntry.numOfCohorts) + " involved in this transaction: ");
        String participantsList = "";
        for (Integer agId : protocolDbTxEntry.CohortsStateMap.keySet()){
            participantsList = participantsList + String.valueOf(agId) + " | ";
        }
        resultLogger.writeln(participantsList);
        protocolDbTxEntry.coordinatorState = CoordinatorState.PREPARING;

        Message prepareMsg = new Message(Message.MessageType.PREPARE, tx.transactionId);
        for (Integer agId : protocolDbTxEntry.CohortsStateMap.keySet()){
            AgentClient aclient = agentClientList.get(agId);
            aclient.addMsgToSendQueue(prepareMsg);
            resultLogger.writeln("PREPARE Sent to Agent "+ String.valueOf(agId) );
        }

        while (protocolDbTxEntry.numOfVoted < protocolDbTxEntry.numOfCohorts){
            resultLogger.writeln("Wait for Voting, " + String.valueOf(protocolDbTxEntry.numOfVoted) + " out of " + String.valueOf(protocolDbTxEntry.numOfCohorts) + " Voted");
            Message recvMsg = takeRecvMessage();
            int cohortId = recvMsg.agentId;
            if (recvMsg.type!=MessageType.VOTEPREPARED && recvMsg.type!=MessageType.VOTEABORT){
                ;
                //Something Wrong happen
                resultLogger.writeln("ERROR: Receive Wrong type of Message while waiting for Voting. Wrong Message Type: " + String.valueOf(recvMsg.type));
            }
            else{
                protocolDbTxEntry.numOfVoted = protocolDbTxEntry.numOfVoted + 1;
                if(recvMsg.type==MessageType.VOTEABORT){
                    //by default the decision is commit, if someone votes for abort, then the decision is changed to abort
                    resultLogger.writeln("Agent "+ String.valueOf(cohortId) + " Voted for ABORT");
                    protocolDbTxEntry.decision = Decision.ABORT;
                    //protocolDbTxEntry.CohortsStateMap.replace(cohortId,CohortsState.PREPARED);
                }
                else if (recvMsg.type==MessageType.VOTEPREPARED){
                    resultLogger.writeln("Agent "+ String.valueOf(cohortId) + " Voted for PREPARED(COMMIT)");
                    protocolDbTxEntry.CohortsStateMap.replace(cohortId,CohortsState.PREPARED);
                }
            }
        }



        if(protocolDbTxEntry.decision == Decision.COMMIT){
            resultLogger.writeln("Voting Complete, The decision is: COMMIT");
            protocolDbTxEntry.coordinatorState = CoordinatorState.COMMITED;
            // TODO: Write Log
            Message commitMsg = new Message(Message.MessageType.ACTCOMMIT, tx.transactionId);
            for (Integer agId : protocolDbTxEntry.CohortsStateMap.keySet()){
                AgentClient aclient = agentClientList.get(agId);
                aclient.addMsgToSendQueue(commitMsg);
                resultLogger.writeln("COMMIT Sent to Agent "+ String.valueOf(agId) );
            }
        }
        else{
            //Abort
            resultLogger.writeln("Voting Complete, The decision is: ABORT");
            protocolDbTxEntry.coordinatorState = CoordinatorState.ABORTED;
            Message abortMsg = new Message(Message.MessageType.ACTABORT, tx.transactionId);
            for (Integer agId : protocolDbTxEntry.CohortsStateMap.keySet()){
                AgentClient aclient = agentClientList.get(agId);
                aclient.addMsgToSendQueue(abortMsg);
                resultLogger.writeln("ABORT Sent to Agent "+ String.valueOf(agId) );
            }
        }

        while (protocolDbTxEntry.numOfAcked < protocolDbTxEntry.numOfCohorts){
            resultLogger.writeln("Wait for ACKs, " + String.valueOf(protocolDbTxEntry.numOfAcked) + " out of " + String.valueOf(protocolDbTxEntry.numOfCohorts) + " Acked");
            Message recvMsg = takeRecvMessage();
            int cohortId = recvMsg.agentId;
            if (recvMsg.type!=MessageType.ACK){
                ;
                //Something Wrong happen
            }
            else{
                resultLogger.writeln("Agent "+ String.valueOf(cohortId) + " Acked");
                protocolDbTxEntry.numOfAcked = protocolDbTxEntry.numOfAcked + 1;
                if(protocolDbTxEntry.decision == Decision.COMMIT){
                    protocolDbTxEntry.CohortsStateMap.replace(cohortId,CohortsState.COMMITED);
                }else{
                    protocolDbTxEntry.CohortsStateMap.replace(cohortId,CohortsState.ABORTED);
                }
            }
        }

        // TODO: write complete log
        resultLogger.writeln("Transaction Completed ");
        

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

    public void run() 
    {
        processTxIn2PC(this.transaction);
        resultLogger.close();
    }
} 