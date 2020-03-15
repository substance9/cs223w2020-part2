package cs223w2020.agent;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class CohortTxLogger {

    public enum CohortTxState{
        PREPARED, COMPLETED
    }

    private Connection dbCon;
    private int transactionId;

    public CohortTxLogger(int transactionId, Connection dbCon){
        this.transactionId = transactionId;
        this.dbCon = dbCon;
    }

    public CohortTxLogger(Connection dbCon){
        this.dbCon = dbCon;
        this.transactionId = 0;
    }

    public int writeTxLog(CohortTxState txState){
        return writeTxLog(transactionId, txState);
    }

    public int writeTxLog(int txId, CohortTxState txState){
        PreparedStatement pst = null;
        int numRowsAffected = 0;

        String sqlStatement = "INSERT INTO COHORT_TX_LOG VALUES (?,?)";
        try{
            pst = dbCon.prepareStatement(sqlStatement);

            pst.setInt(1,txId);//1 specifies the first parameter in the query  

            if(txState == CohortTxState.PREPARED){
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