package cs223w2020.coordinator.txprocessor;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import cs223w2020.coordinator.txprocessor.ProtocolDbTxEntry.Decision;
import cs223w2020.model.Message;
import cs223w2020.model.Message.MessageType;

class TxStateQuerier implements Runnable {
    private BlockingQueue<Message> qMsgRecvQueue;
    public TxProcessor txProcessor;
    public ArrayList<AgentClient> agentCList;

    public TxStateQuerier(TxProcessor txProcessor, ArrayList<AgentClient> agentCList) {
        this.txProcessor = txProcessor;
        this.qMsgRecvQueue = new LinkedBlockingQueue<Message>();
        this.agentCList = agentCList;
    }

    private void answerQueryWithActMsg(Message qMesg) {
        int transactionId = qMesg.transactionId;
        Decision txDecision = txProcessor.syncGetTxDecisionProtocolDB(transactionId);
        int agentId = qMesg.agentId;
        AgentClient aClient = agentCList.get(agentId);
        if (txDecision == Decision.COMMIT) {
            Message commitMsg = new Message(MessageType.ACTCOMMIT, transactionId);
            aClient.addMsgToSendQueue(commitMsg);
        } else if (txDecision == Decision.ABORT) {
            Message abortMsg = new Message(MessageType.ACTABORT, transactionId);
            aClient.addMsgToSendQueue(abortMsg);
        } else {
            // something wrong with protocolDB, do nothing
            return;
        }
    }

    public void addToRecvQueue(Message qMsg) {
        try {
            qMsgRecvQueue.put(qMsg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void run() {
        Message qMsg = null;
        while(true){
            try {
                qMsg = qMsgRecvQueue.take();
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (qMsg!=null){
                answerQueryWithActMsg(qMsg);
            }
        }
    }
}