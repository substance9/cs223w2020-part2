package cs223w2020.coordinator.txprocessor;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class CoordinatorTxLogger {

    public enum CoordinatorTxState{
        COMMIT, COMPLETED
    }

    private Connection dbCon;
    private int transactionId;
    public int numCohorts = 0;

    public CoordinatorTxLogger(int transactionId, Connection dbCon){
        this.transactionId = transactionId;
        this.dbCon = dbCon;
    }

    public CoordinatorTxLogger(Connection dbCon){
        this.dbCon = dbCon;
        this.transactionId = 0;
    }

    public int writeTxLog(CoordinatorTxState txState, int numCohorts){
        return writeTxLog(transactionId, txState, numCohorts);
    }

    public int writeTxLog(int txId, CoordinatorTxState txState, int numCohorts){
        PreparedStatement pst = null;
        int numRowsAffected = 0;

        String sqlStatement = "INSERT INTO COORDINATOR_TX_LOG VALUES (?,?,?)";
        try{
            pst = dbCon.prepareStatement(sqlStatement);

            pst.setInt(1,txId);//1 specifies the first parameter in the query  

            if(txState == CoordinatorTxState.COMMIT){
                pst.setInt(2,1);  
            } else {
                pst.setInt(2,2);  
            }
            pst.setInt(3,numCohorts);

            numRowsAffected = pst.executeUpdate();
        } catch(Exception e){
            e.printStackTrace();
        }
        return numRowsAffected;
    }
}