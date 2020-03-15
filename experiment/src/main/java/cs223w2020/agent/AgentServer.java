package cs223w2020.agent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import cs223w2020.model.Message;
import cs223w2020.model.Message.MessageType;

public class AgentServer {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    private BlockingQueue<Message> msgSendQueue;

    private int agentId;
    private int appPort;
    private int mpl;
    private int dbPort;
    private String resultDir;

    public ExecutorService tx2PCCohortThreadPool;
    public HikariDataSource logDBConnectionPool;
    public HikariDataSource dataDBConnectionPool;

    public HashMap<Integer, Tx2PCCohort> txCohortMap;
    public enum HashMapSyncOp{
        PUT, REMOVE
    }

    public AgentServer(int agentId, int mpl, int appPort, int dbPort, String resultDir) {
        this.agentId = agentId;
        this.appPort = appPort;
        this.mpl = mpl;
        this.dbPort = dbPort;
        this.resultDir = resultDir;

        txCohortMap = new HashMap<Integer, Tx2PCCohort>();
        msgSendQueue = new LinkedBlockingQueue<Message>();

        Properties prop = getHikariDbProperties("postgres");

        String jdbcUrlBase = prop.getProperty("jdbcUrl");
        String dataJdbcUrl = jdbcUrlBase + ":" + String.valueOf(dbPort) + "/cs223w2020_low_concurrency";

        System.out.println("Preparing to connect to DB for data storage: " + dataJdbcUrl);

        // Data DB COnnection
        HikariConfig dataDbCfg = new HikariConfig(prop);
        dataDbCfg.setJdbcUrl(dataJdbcUrl);
        dataDbCfg.setMaximumPoolSize(mpl*2+1);
        dataDbCfg.setAutoCommit(false);
        dataDBConnectionPool = new HikariDataSource(dataDbCfg);

        // Data DB COnnection
        String logJdbcUrl = jdbcUrlBase + ":" + String.valueOf(dbPort) + "/cs223w2020_cohort_log";

        HikariConfig logDbCfg = new HikariConfig(prop);
        logDbCfg.setJdbcUrl(logJdbcUrl);
        logDbCfg.setMaximumPoolSize(mpl+1);
        logDbCfg.setAutoCommit(true);
        logDBConnectionPool = new HikariDataSource(logDbCfg);

        tx2PCCohortThreadPool = Executors.newFixedThreadPool(mpl);
    }

    public Properties getHikariDbProperties(String dbName) {
        Properties prop = null;
        try (InputStream input = AgentServer.class.getClassLoader().getResourceAsStream(dbName + ".properties")) {
            prop = new Properties();
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    public String serializeObj(Object obj) {
        ObjectMapper mapper = new ObjectMapper();
        String msg = null;
        try {
            msg = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return msg;
    }

    public Message deserializeStr(String str) {
        ObjectMapper mapper = new ObjectMapper();
        Message msg = null;
        try {
            msg = mapper.readValue(str, Message.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return msg;
    }

    public void sendMessage(Message msg) {
        String msgStr = serializeObj(msg);

        out.println(msgStr);
        // String resp;
        // try {
        // resp = in.readLine();
        // } catch (IOException e) {
        // e.printStackTrace();
        // }
    }

    public Message recvMessageWTimeout() {
        String resp = null;
        Message respMsg = null;

        try {
            resp = in.readLine();
        } catch (SocketTimeoutException e) {
            return null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (resp != null) {
            respMsg = deserializeStr(resp);
        }

        return respMsg;
    }

    public void addMsgToSendQueue(Message msg) {
        try {
            msgSendQueue.put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        System.out.println("Starting AgentServer @port: " + String.valueOf(appPort));

        try {
            serverSocket = new ServerSocket(appPort);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        System.out.println("Start Listen and Ready to Accpet");
        try {
            clientSocket = serverSocket.accept();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        try {
            clientSocket.setSoTimeout(1);
        } catch (SocketException e2) {
            e2.printStackTrace();
        }

        System.out.println("\"Client\"(Coordinator) Connected");
        try {
            out = new PrintWriter(clientSocket.getOutputStream(), true);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        try {
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        System.out.println("Start recving messages");
        String recvdStr = null;
        // main loop starts
        while (true) {
            Message sendMsg = null;
            try {
                sendMsg = msgSendQueue.poll(1, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                sendMsg = null;
            }
            if (sendMsg != null) {
                sendMessage(sendMsg);
            }

            Message recvMsg = null;
            recvMsg = recvMessageWTimeout();

            if (recvMsg != null) {
                processMsg(recvMsg);
            }
        }

    }

    public void stop() {
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        out.close();
        try {
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void syncChangeToTxCohortMap(HashMapSyncOp op, int tid, Tx2PCCohort txCohort){
        if(op == HashMapSyncOp.PUT){
            txCohortMap.put(tid, txCohort);
        }
        if(op == HashMapSyncOp.REMOVE){
            txCohortMap.remove(tid);
        }
    }

    private void processMsg(Message msg) {
        // System.out.println(msg);

        if (msg.type == MessageType.START) {
            Connection logDbCon = null;
            Connection dataDbCon = null;
            Connection dataDbTxControlCon = null;
            try {
                dataDbCon = dataDBConnectionPool.getConnection();
                dataDbTxControlCon = dataDBConnectionPool.getConnection();
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }
            int newTxId = msg.transactionId;
                //  Log DB COnnection
            try{
                //System.out.println("try executing");
                logDbCon = logDBConnectionPool.getConnection();
                logDbCon.setAutoCommit(true);
            }catch (SQLException ex){
                ex.printStackTrace();
            }
            Tx2PCCohort txCohort = new Tx2PCCohort(agentId, newTxId, logDbCon, dataDbCon, dataDbTxControlCon, this, resultDir);
            syncChangeToTxCohortMap(HashMapSyncOp.PUT, newTxId, txCohort);
            tx2PCCohortThreadPool.execute(txCohort);
            System.out.println("Create new 2PC Cohort Thread for transaction" + String.valueOf(newTxId));
        }

        else{
            int txId = msg.transactionId;
            Tx2PCCohort cohort = txCohortMap.get(txId);
            cohort.addRecvMessage(msg);
        }
    }
    
}