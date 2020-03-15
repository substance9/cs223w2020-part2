package cs223w2020.agent;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class TxExecutor
{ 
    private int transactionId;
    private String fullTransactionId;
    private Connection dbCon;
    private Connection txControlCon;

    public TxExecutor(int transactionId, Connection dbCon, Connection controlCon){
        this.transactionId = transactionId;
        this.dbCon = dbCon;
        this.txControlCon = controlCon;

        try {
            this.dbCon.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            this.txControlCon.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        fullTransactionId = "tx" + String.valueOf(transactionId);
    }

    public int executeStatement(String sqlStr){
        PreparedStatement pst = null;
        int numRowsAffected = 0;
        ResultSet rs = null;

        try {
            pst = dbCon.prepareStatement(sqlStr);
            numRowsAffected = pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return numRowsAffected;
    }

    public boolean commitTransaction(){
        boolean result = true;
        String commitTxStatement = "COMMIT PREPARED '"+ fullTransactionId +"';";
        PreparedStatement pst = null;
        try {
            pst = txControlCon.prepareStatement(commitTxStatement);
            pst.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            result = false;
        }

        return result;
    }

    public boolean rollbackTransaction(){
        boolean result = true;
        String rollbackTxStatement = "ROLLBACK PREPARED '"+ fullTransactionId +"';";
        PreparedStatement pst = null;
        try {
            pst = txControlCon.prepareStatement(rollbackTxStatement);
            pst.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            result = false;
        }

        return result;
    }

    public boolean prepareTransaction(){
        boolean result = true;
        String prepareTxStatement = "PREPARE TRANSACTION '"+ fullTransactionId +"';";
        PreparedStatement pst = null;
        try {
            pst = dbCon.prepareStatement(prepareTxStatement);
            pst.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            result = false;
        }

        return result;
    }

    public void closeTransaction(){
        try {
            dbCon.close();
            txControlCon.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    
} 