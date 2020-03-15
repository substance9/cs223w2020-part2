package cs223w2020.coordinator.txprocessor;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class CoordinatorTxLogger {

    public enum CoordinatorTxState{
        COMMIT, COMPLETED
    }

    private Connection dbCon;
    private int transactionId;

    public CoordinatorTxLogger(int transactionId, Connection dbCon){
        this.transactionId = transactionId;
        this.dbCon = dbCon;
    }

    public CoordinatorTxLogger(Connection dbCon){
        this.dbCon = dbCon;
        this.transactionId = 0;
    }

    public int writeTxLog(CoordinatorTxState txState){
        return writeTxLog(transactionId, txState);
    }

    public int writeTxLog(int txId, CoordinatorTxState txState){
        PreparedStatement pst = null;
        int numRowsAffected = 0;

        String sqlStatement = "INSERT INTO COORDINATOR_TX_LOG VALUES (?,?)";
        try{
            pst = dbCon.prepareStatement(sqlStatement);

            pst.setInt(1,txId);//1 specifies the first parameter in the query  

            if(txState == CoordinatorTxState.COMMIT){
                pst.setInt(2,1);  
            } else {
                pst.setInt(2,2);  
            }

            numRowsAffected = pst.executeUpdate();
        } catch(Exception e){
            e.printStackTrace();
        }
        return numRowsAffected;
    }
}