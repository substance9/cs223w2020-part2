package main.java.cs223w2020.txprocessor;

import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.ExecutorService; 
import java.util.concurrent.Executors; 
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import main.java.cs223w2020.TransactionQueue;
import main.java.cs223w2020.model.Operation;
import main.java.cs223w2020.model.Transaction;

public class TxExecutor implements Runnable 
{ 
    private Transaction tx;
    private HikariDataSource  connectionPool;
    private int isolationLevel;
    private TransactionQueue resQueue;

    public TxExecutor(Transaction tx, HikariDataSource connectionPool, int isolationLevel, TransactionQueue resQueue){
        this.tx = tx;
        this.resQueue = resQueue;
        this.connectionPool = connectionPool;
        this.isolationLevel = isolationLevel;
    }

    public void run() 
    {
        Connection con = null;
        Operation op = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        String sqlStatement = null;
        int numRowsAffected = 0;
        try{
            //System.out.println("try executing");
            con = connectionPool.getConnection();
            con.setTransactionIsolation(isolationLevel);
            con.setAutoCommit(false);

            tx.setBeginTimeToNow();

            for(int i = 0; i < tx.operations.size(); i++){
                op = tx.operations.get(i);
                
                if(op.operationStr.equals("SELECT")){
                    pst = con.prepareStatement(op.sqlStr);
                    rs = pst.executeQuery();
                }
                else if(op.operationStr.equals("INSERT")){
                    pst = con.prepareStatement(op.sqlStr);
                    numRowsAffected = pst.executeUpdate();
                    ;
                }
                else{
                    System.out.println("ERROR: Operation Type " + op.operationStr + " not supported");
                }
            }

            con.commit();
            tx.setEndTimeToNow();
            if(op.operationStr.equals("SELECT")){
                while (rs.next()) {
                    ;
                }
            }
            
        } catch (SQLException ex){
            System.out.println(tx.operations.get(0).sqlStr);
            ex.printStackTrace();
        }finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pst != null) {
                    pst.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        resQueue.put(tx);
    }

    
} 