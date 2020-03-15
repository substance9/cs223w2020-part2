package cs223w2020.coordinator.txprocessor;

import java.util.HashMap;

class ProtocolDbTxEntry{
    public enum CoordinatorState{
        INITIATED, PREPARING, COMMITED, ABORTED
    }

    public enum CohortsState{
        INITIATED, PREPARED, COMMITED, ABORTED
    }

    public enum Decision{
        COMMIT, ABORT
    }
    
    public int transactionId;
    public CoordinatorState coordinatorState;
    HashMap<Integer, CohortsState> CohortsStateMap;
    public int numOfCohorts;
    public int numOfVoted;
    public Decision decision;
    public int numOfAcked;

    public ProtocolDbTxEntry(int tid){
        this.transactionId = tid;
        coordinatorState = CoordinatorState.INITIATED;
        CohortsStateMap = new HashMap<Integer, CohortsState>();
        numOfCohorts = 0;
        numOfVoted = 0;
        numOfAcked = 0;
        decision = Decision.COMMIT;
    }
}