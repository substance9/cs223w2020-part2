package cs223w2020.coordinator.txprocessor;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.concurrent.TimeUnit;

import cs223w2020.coordinator.TransactionQueue;
import cs223w2020.model.Transaction;

public class TxProcessor implements Runnable {
    private TransactionQueue txQueue;
    private int numAgents;
    private int mpl;
    private int dbPort;
    private int agentsPortsStartAt;
    private String resultDir;

    public ArrayList<AgentClient> agentClientList;
    public ArrayList<Thread> agentClientThreadList;

    public HashMap<Integer, Tx2PCCoordinator> tx2PCProcessorMap;
    static Semaphore tx2PCProcessorMapSem = new Semaphore(1);
    public HashMap<Integer, ProtocolDbTxEntry> protocolDB;
    static Semaphore ProtocolDBSem = new Semaphore(1);

    public ExecutorService tx2PCCoordinatorThreadPool;
    public HikariDataSource dBConnectionPool;

    public TxProcessor(TransactionQueue txQueue, int mpl, int dbPort, int numAgents, int agentsPortsStartAt,
            String resOutputDir) {
        this.txQueue = txQueue;
        this.numAgents = numAgents;
        this.agentsPortsStartAt = agentsPortsStartAt;
        this.mpl = mpl;
        this.dbPort = dbPort;
        this.resultDir = resOutputDir;

        this.protocolDB = new HashMap<Integer, ProtocolDbTxEntry>();

        agentClientList = new ArrayList<AgentClient>();
        agentClientThreadList = new ArrayList<Thread>();

        tx2PCProcessorMap = new HashMap<Integer, Tx2PCCoordinator>();

        for (int i = 0; i < numAgents; i++) {
            AgentClient aclient = new AgentClient(i, agentsPortsStartAt + i, this);
            agentClientList.add(aclient);
        }

        Properties prop = getHikariDbProperties("postgres");

        String jdbcUrlBase = prop.getProperty("jdbcUrl");
        String jdbcUrl = jdbcUrlBase + ":" + String.valueOf(dbPort) + "/cs223w2020_coordinator_log";

        System.out.println("Preparing to connect to DB for log storage: " + jdbcUrl);

        // DB COnnection
        HikariConfig cfg = new HikariConfig(prop);
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setMaximumPoolSize(mpl + 3);
        cfg.setAutoCommit(true);
        dBConnectionPool = new HikariDataSource(cfg);

        tx2PCCoordinatorThreadPool = Executors.newFixedThreadPool(mpl);
    }

    public Properties getHikariDbProperties(String dbName) {
        Properties prop = null;
        try (InputStream input = TxProcessor.class.getClassLoader().getResourceAsStream(dbName + ".properties")) {
            prop = new Properties();
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    public int connectToAgents() {
        for (int i = 0; i < numAgents; i++) {
            AgentClient aclient = agentClientList.get(i);
            try {
                aclient.startConnection();
            } catch (IOException e) {
                e.printStackTrace();
                return -1;
            }
        }
        return 0;
    }

    public void syncProtocolDBPut(int tid, ProtocolDbTxEntry dbTxEntry) {
        try {
            ProtocolDBSem.acquire();

            try {
                protocolDB.put(tid, dbTxEntry);
            } finally {
                ProtocolDBSem.release();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void syncProtocolDBRemove(int tid) {
        try {
            ProtocolDBSem.acquire();

            try {
                protocolDB.remove(tid);
            } finally {
                ProtocolDBSem.release();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ProtocolDbTxEntry syncProtocolDBGet(int tid) {
        ProtocolDbTxEntry entry = null;
        try {
            ProtocolDBSem.acquire();

            try {
                entry = protocolDB.get(tid);
            } finally {
                ProtocolDBSem.release();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return entry;
    }

    public int syncProtocolDBCount() {
        int count = 0;
        try {
            ProtocolDBSem.acquire();

            try {
                count = protocolDB.size();
            } finally {
                ProtocolDBSem.release();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return count;
    }

    public void syncProcessorMapPut(int tid, Tx2PCCoordinator tx2PCProcessor) {
        try {
            tx2PCProcessorMapSem.acquire();

            try {
                tx2PCProcessorMap.put(tid, tx2PCProcessor);
            } finally {
                tx2PCProcessorMapSem.release();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void syncProcessorMapRemove(int tid) {
        try {
            tx2PCProcessorMapSem.acquire();

            try {
                tx2PCProcessorMap.remove(tid);
            } finally {
                tx2PCProcessorMapSem.release();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Tx2PCCoordinator syncProcessorMapGet(int tid) {
        Tx2PCCoordinator tx2PCProcessor = null;
        try {
            tx2PCProcessorMapSem.acquire();

            try {
                tx2PCProcessor = tx2PCProcessorMap.get(tid);
            } finally {
                tx2PCProcessorMapSem.release();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return tx2PCProcessor;
    }

    public void run() {
        for (int i = 0; i < numAgents; i++) {
            AgentClient aclient = agentClientList.get(i);
            Thread acThread = new Thread(aclient);
            agentClientThreadList.add(acThread);
            acThread.start();
        }

        Transaction tx = null;
        while (true) {
            if (syncProtocolDBCount() > mpl + 1) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            //1. get the transaction from queue
            tx = txQueue.take();
            if (tx.operations.size()>0){
                System.out.println("Got Transaction " + String.valueOf(tx.transactionId) + "from Queue");
                Connection logDbCon = null;
                try{
                    logDbCon = dBConnectionPool.getConnection();
                    logDbCon.setAutoCommit(true);
                }catch (SQLException ex){
                    ex.printStackTrace();
                }
                
                //2. Construct TxExecutor with the (1)transaction, (2)connectionPool (3)result (transaction) queue (4)set isolation level for each transaction
                ProtocolDbTxEntry protocolDbTxEntry = new ProtocolDbTxEntry(tx.transactionId);
                syncProtocolDBPut(tx.transactionId, protocolDbTxEntry);
                Tx2PCCoordinator tx2PCProcessor = new Tx2PCCoordinator(tx, protocolDbTxEntry, numAgents, agentClientList, logDbCon, this, resultDir);

                syncProcessorMapPut(tx.transactionId, tx2PCProcessor);
                //3. Get a thread from pool and execute
                tx2PCCoordinatorThreadPool.execute(tx2PCProcessor);
                System.out.println("Put Transaction " + String.valueOf(tx.transactionId) + "to execution Queue");
            }
            else{
                //end mark transaction
                
                
                return;
            }
        }
    }
} 