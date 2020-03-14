package cs223w2020.agent;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService; 
import java.util.concurrent.Executors; 
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.concurrent.TimeUnit;

import cs223w2020.coordinator.OperationQueue;
import cs223w2020.coordinator.TransactionQueue;
import cs223w2020.model.Operation;
import cs223w2020.model.Transaction;

public class TxProcessor implements Runnable 
{ 
    private TransactionQueue txQueue;
    private TransactionQueue resQueue;
    private String dbName;
    private String datasetConcurrency;
    private int mpl;
    private String concurrency;
    private int isolationLevel;

    private ExecutorService threadPool;
    private HikariDataSource  connectionPool;

    private Thread resultAggregator;

    public TxProcessor(String dbName, String datasetConcurrency, int mpl, int isolationLevel, TransactionQueue txQueue, String resOutputDir){
        this.dbName = dbName;
        this.mpl = mpl;
        this.txQueue = txQueue;
        this.datasetConcurrency = datasetConcurrency;
        this.isolationLevel = isolationLevel;

        Properties prop = getHikariDbProperties(dbName);
        if (dbName.equals("mysql")){
            try{
                Class.forName("com.mysql.jdbc.Driver").getDeclaredConstructor().newInstance();
            }
            catch(Exception ex){
                ex.printStackTrace();
            }
        }
        String jdbcUrlBase = prop.getProperty("jdbcUrl");
        String jdbcUrl = jdbcUrlBase + "cs223w2020_"+ datasetConcurrency + "_concurrency";

        HikariConfig cfg = new HikariConfig(prop);
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setMaximumPoolSize(mpl);
        //cfg.setTransactionIsolation(isolationLevel);
        cfg.setAutoCommit(false);
        connectionPool = new HikariDataSource(cfg);

        threadPool = Executors.newFixedThreadPool(mpl); 

        resQueue = new TransactionQueue();
        //resultAggregator = new Thread(new ResultAggregator(resQueue, resOutputDir)); 
        //resultAggregator.start(); 
    }

    public Properties getHikariDbProperties(String dbName){
        Properties prop = null;
        try (InputStream input = TxProcessor.class.getClassLoader().getResourceAsStream(dbName + ".properties")) {
            prop = new Properties();
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    public void run() 
    {
        TxExecutor txexecutor = null;
        Transaction tx = null;
        while(true){
            //1. get the transaction from queue
            tx = txQueue.take();
            if (tx.operations.size()>0){
                //2. Construct TxExecutor with the (1)transaction, (2)connectionPool (3)result (transaction) queue (4)set isolation level for each transaction
                txexecutor = new TxExecutor(tx, connectionPool, isolationLevel, resQueue);
                //3. Get a thread from pool and execute
                threadPool.execute(txexecutor);
            }
            else{
                //end mark transaction
                while(txQueue.getSize()>0){
                    try{
                        Thread.sleep(1500);
                    }catch(Exception ex) 
                    { 
                        System.out.println("Exception has been" + " caught" + ex); 
                    }
                }
                threadPool.shutdown();
                try {
                    threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                resQueue.put(tx);

                try
                { 
                    resultAggregator.join(); 
                } 
                catch(Exception ex) 
                { 
                    System.out.println("Exception has been" + " caught" + ex); 
                }

                try{
                    Thread.sleep(1500);
                }catch(Exception ex) 
                { 
                    System.out.println("Exception has been" + " caught" + ex); 
                }

                connectionPool.close();
                return;
            }
        }
    }
} 