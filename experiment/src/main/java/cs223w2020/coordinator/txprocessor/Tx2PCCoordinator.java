package cs223w2020.coordinator.txprocessor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import cs223w2020.coordinator.txprocessor.CoordinatorTxLogger.CoordinatorTxState;
import cs223w2020.coordinator.txprocessor.ProtocolDbTxEntry.CohortsState;
import cs223w2020.coordinator.txprocessor.ProtocolDbTxEntry.CoordinatorState;
import cs223w2020.coordinator.txprocessor.ProtocolDbTxEntry.Decision;
import cs223w2020.model.Message;
import cs223w2020.model.Message.MessageType;
import cs223w2020.model.Operation;
import cs223w2020.model.Transaction;
import cs223w2020.util.ResultFileLogger;

public class Tx2PCCoordinator implements Runnable {
    private Transaction transaction;
    private int numAgents;
    private ArrayList<AgentClient> agentClientList;
    private Connection logDbConnection;

    private boolean isRecoveryTx = false;

    public ProtocolDbTxEntry protocolDbTxEntry;

    private BlockingQueue<Message> recvMessageQueue;

    private String resultLogFileName;
    private ResultFileLogger resultLogger;
    private CoordinatorTxLogger coordinatorTxLogger;

    private TxProcessor txProcessor;

    private long voteTimeoutMs = 3000;
    private long ackTimeoutMs = 3000;

    public Tx2PCCoordinator(Transaction transaction, ProtocolDbTxEntry txEntry, int numAgents,
            ArrayList<AgentClient> agentCList, Connection dbCon, TxProcessor txProcessor, String resultDir) {
        this.numAgents = numAgents;
        this.agentClientList = agentCList;
        this.transaction = transaction;
        this.logDbConnection = dbCon;
        this.txProcessor = txProcessor;

        this.recvMessageQueue = new LinkedBlockingQueue<Message>();

        this.protocolDbTxEntry = txEntry;

        this.resultLogFileName = resultDir + "/transactions/" + String.valueOf(transaction.transactionId) + "_c.txt";
    }

    public Tx2PCCoordinator(int transactionId, ProtocolDbTxEntry txEntry, int numAgents,
            ArrayList<AgentClient> agentCList, Connection dbCon, TxProcessor txProcessor, String resultDir, boolean isRecovery) {
        this.numAgents = numAgents;
        this.agentClientList = agentCList;
        this.transaction = new Transaction();
        this.transaction.transactionId = transactionId;
        this.logDbConnection = dbCon;
        this.txProcessor = txProcessor;
        this.isRecoveryTx = isRecovery;

        this.recvMessageQueue = new LinkedBlockingQueue<Message>();

        this.protocolDbTxEntry = txEntry;

        this.resultLogFileName = resultDir + "/transactions/" + String.valueOf(transaction.transactionId) + "_c_recovery.txt";
    }

    public int getCorrespondAgentId(Operation op) {
        int opHashCode = op.sensorId.hashCode() + op.timestamp.hashCode();
        if (opHashCode < 0) {
            opHashCode = 0 - opHashCode;
        }
        return (opHashCode % numAgents);
    }

    private void statementPhase(){
        Message startMsg = new Message(Message.MessageType.START, transaction.transactionId);

        for (int i = 0; i < transaction.operations.size(); i++) {
            Operation op = transaction.operations.get(i);
            int agentId = getCorrespondAgentId(op);
            AgentClient aclient = agentClientList.get(agentId);
            if (!protocolDbTxEntry.CohortsStateMap.containsKey(agentId)) {
                protocolDbTxEntry.CohortsStateMap.put(agentId, CohortsState.INITIATED);
                protocolDbTxEntry.numOfCohorts = protocolDbTxEntry.numOfCohorts + 1;
                aclient.addMsgToSendQueue(startMsg);
            }

            Message stnMsg = new Message(Message.MessageType.STATEMENT, transaction.transactionId);
            stnMsg.setSql(op.sqlStr);
            aclient.addMsgToSendQueue(stnMsg);
            resultLogger.writeln("SQL Statement Sent to Agent " + String.valueOf(agentId) + ": " + op.sqlStr);
        }

        // Operations are all sent, Start Preparing
        resultLogger.writeln("SQL Statements All Sent to Agent, Start Preparing");
        resultLogger.writeln(
                "There are " + String.valueOf(protocolDbTxEntry.numOfCohorts) + " involved in this transaction: ");
        String participantsList = "";
        for (Integer agId : protocolDbTxEntry.CohortsStateMap.keySet()) {
            participantsList = participantsList + String.valueOf(agId) + " | ";
        }
        resultLogger.writeln(participantsList);
    }

    private Decision sendPrepareAndWaitPhase(){
        protocolDbTxEntry.coordinatorState = CoordinatorState.PREPARING;

        Message prepareMsg = new Message(Message.MessageType.PREPARE, transaction.transactionId);
        for (Integer agId : protocolDbTxEntry.CohortsStateMap.keySet()) {
            AgentClient aclient = agentClientList.get(agId);
            aclient.addMsgToSendQueue(prepareMsg);
            resultLogger.writeln("PREPARE Sent to Agent " + String.valueOf(agId));
        }

        while (protocolDbTxEntry.numOfVoted < protocolDbTxEntry.numOfCohorts) {
            resultLogger.writeln("Wait for Voting, " + String.valueOf(protocolDbTxEntry.numOfVoted) + " out of "
                    + String.valueOf(protocolDbTxEntry.numOfCohorts) + " Voted");
            Message recvMsg = takeRecvMessageWTimeout(voteTimeoutMs);

            if(recvMsg == null){
                protocolDbTxEntry.decision = Decision.ABORT;
                return Decision.ABORT;
            }

            int cohortId = recvMsg.agentId;
            if (recvMsg.type != MessageType.VOTEPREPARED && recvMsg.type != MessageType.VOTEABORT) {
                ;
                // Something Wrong happen
                resultLogger
                        .writeln("ERROR: Receive Wrong type of Message while waiting for Voting. Wrong Message Type: "
                                + String.valueOf(recvMsg.type));
            } else {
                protocolDbTxEntry.numOfVoted = protocolDbTxEntry.numOfVoted + 1;
                if (recvMsg.type == MessageType.VOTEABORT) {
                    // by default the decision is commit, if someone votes for abort, then the
                    // decision is changed to abort
                    resultLogger.writeln("Agent " + String.valueOf(cohortId) + " Voted for ABORT");
                    protocolDbTxEntry.decision = Decision.ABORT;
                    return Decision.ABORT;
                    // protocolDbTxEntry.CohortsStateMap.replace(cohortId,CohortsState.PREPARED);
                } else if (recvMsg.type == MessageType.VOTEPREPARED) {
                    resultLogger.writeln("Agent " + String.valueOf(cohortId) + " Voted for PREPARED(COMMIT)");
                    protocolDbTxEntry.CohortsStateMap.replace(cohortId, CohortsState.PREPARED);
                }
            }
        }
        return Decision.COMMIT;
    }

    private Message processWithCommit(){
        resultLogger.writeln("Voting Complete, The decision is: COMMIT");
        protocolDbTxEntry.coordinatorState = CoordinatorState.COMMITED;
        // Write Log
        coordinatorTxLogger.writeTxLog(CoordinatorTxState.COMMIT, protocolDbTxEntry.numOfCohorts);
        Message commitMsg = new Message(Message.MessageType.ACTCOMMIT, transaction.transactionId);
        for (Integer agId : protocolDbTxEntry.CohortsStateMap.keySet()) {
            AgentClient aclient = agentClientList.get(agId);
            aclient.addMsgToSendQueue(commitMsg);
            resultLogger.writeln("COMMIT Sent to Agent " + String.valueOf(agId));
        }

        return commitMsg;
    }

    private Message processWithAbort(){
        resultLogger.writeln("Voting Complete, The decision is: ABORT");
        protocolDbTxEntry.coordinatorState = CoordinatorState.ABORTED;
        Message abortMsg = new Message(Message.MessageType.ACTABORT, transaction.transactionId);
        for (Integer agId : protocolDbTxEntry.CohortsStateMap.keySet()) {
            AgentClient aclient = agentClientList.get(agId);
            aclient.addMsgToSendQueue(abortMsg);
            resultLogger.writeln("ABORT Sent to Agent " + String.valueOf(agId));
        }

        return abortMsg;
    }

    private void waitAckPhase(Message msg){
        while (protocolDbTxEntry.numOfAcked < protocolDbTxEntry.numOfCohorts) {
            resultLogger.writeln("Wait for ACKs, " + String.valueOf(protocolDbTxEntry.numOfAcked) + " out of "
                    + String.valueOf(protocolDbTxEntry.numOfCohorts) + " Acked");
            Message recvMsg = takeRecvMessageWTimeout(ackTimeoutMs);

            while(recvMsg == null){
                for (Integer agId : protocolDbTxEntry.CohortsStateMap.keySet()) {
                    CohortsState coState = protocolDbTxEntry.CohortsStateMap.get(agId);
                    if (coState != CohortsState.COMMITED && coState != CohortsState.ABORTED ){
                        AgentClient aClient = agentClientList.get(agId);
                        aClient.addMsgToSendQueue(msg);
                    }
                }
                recvMsg = takeRecvMessageWTimeout(ackTimeoutMs);
            }

            int cohortId = recvMsg.agentId;
            if (recvMsg.type != MessageType.ACK) {
                ;
                // Something Wrong happen
            } else {
                resultLogger.writeln("Agent " + String.valueOf(cohortId) + " Acked");
                protocolDbTxEntry.numOfAcked = protocolDbTxEntry.numOfAcked + 1;
                if (protocolDbTxEntry.decision == Decision.COMMIT) {
                    protocolDbTxEntry.CohortsStateMap.replace(cohortId, CohortsState.COMMITED);
                } else {
                    protocolDbTxEntry.CohortsStateMap.replace(cohortId, CohortsState.ABORTED);
                }
            }
        }
    }

    public void processTxIn2PC() {
        resultLogger = new ResultFileLogger(this.resultLogFileName);
        coordinatorTxLogger = new CoordinatorTxLogger(transaction.transactionId, logDbConnection);
        resultLogger.writeln("-------------------------------------------------");
        resultLogger.writeln(
                "Started Processing New Transaction in 2PC Protocol" + " | Tx ID: " + String.valueOf(transaction.transactionId));
        resultLogger.writeln(transaction.toString());

        statementPhase();

        Decision decision = sendPrepareAndWaitPhase();
        // the protocolDbTxEntry.decision need to be set properly in preparePhase()
        Message msg = null;

        if (decision == Decision.COMMIT){
            msg = processWithCommit();
        } else{
            // Abort
            msg = processWithAbort();
        }

        waitAckPhase(msg);

        // Write complete log
        coordinatorTxLogger.writeTxLog(CoordinatorTxState.COMPLETED,0); //num of cohorts is a hack for recovery
        resultLogger.writeln("Transaction Completed ");

    }

    public void processTxIn2PCRecovery() {
        resultLogger = new ResultFileLogger(this.resultLogFileName);
        coordinatorTxLogger = new CoordinatorTxLogger(transaction.transactionId, logDbConnection);
        resultLogger.writeln("-------------------------------------------------");
        resultLogger.writeln(
                "Started Processing Recovery Transaction in 2PC Protocol" + " | Tx ID: " + String.valueOf(transaction.transactionId));
        resultLogger.writeln(transaction.toString());

        Message commitMsg = new Message(Message.MessageType.ACTCOMMIT, transaction.transactionId);
        //TODO: improve by only send to participants, this requires improvement of reocvery process
        for (int agId =0; agId < agentClientList.size(); agId++) {
            AgentClient aclient = agentClientList.get(agId);
            aclient.addMsgToSendQueue(commitMsg);
            resultLogger.writeln("Resend COMMIT to Agent (all) " + String.valueOf(agId));
        }

        waitAckPhase(commitMsg);
        // Write complete log
        coordinatorTxLogger.writeTxLog(CoordinatorTxState.COMPLETED,0); //num of cohorts is a hack for recovery
        resultLogger.writeln("Transaction (recovered) Completed ");
    }

    public void addRecvMessage(Message msg) {
        try {
            this.recvMessageQueue.put(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Message takeRecvMessage() {
        Message msg = null;
        try {
            msg = this.recvMessageQueue.take();
        } catch (Exception e) {
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

    public void run() {
        if(!isRecoveryTx){
            processTxIn2PC();
        } else {
            processTxIn2PCRecovery();
        }


        resultLogger.close();

        try {
            logDbConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        txProcessor.syncProtocolDBRemove(transaction.transactionId);
        txProcessor.syncProcessorMapRemove(transaction.transactionId);
    }
} 