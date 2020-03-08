package main.java.cs223w2020.replayer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.sql.Timestamp;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.sql.Timestamp;

import main.java.cs223w2020.OperationQueue;
import main.java.cs223w2020.model.Operation;

public class Replayer implements Runnable 
{
    private ArrayList<Operation>opList;
    private String obsFilePath;
    private String semObsFilePath;
    private String queryFilePath;
    private InsertionSqlFileLoader obsSqlFileLoader;
    private InsertionSqlFileLoader semObsSqlFileLoader;
    private QueriesFileLoader queriesFileLoader;

    private int expDurationMinutes;
    private OperationQueue opQueue;
    private Float sFactor;
    private int opCounter;

    public Replayer(String inputDirStr, String dbName, String concurrencyStr, int expDurationMinutes, OperationQueue opQueue){
        opList = new ArrayList<Operation>(); 
        obsFilePath = inputDirStr+"data/"+ concurrencyStr + "_concurrency/" + "observation_" + concurrencyStr + "_concurrency.sql";
        semObsFilePath = inputDirStr+"data/"+ concurrencyStr + "_concurrency/" + "semantic_observation_" + concurrencyStr + "_concurrency.sql";
        if(dbName.equals("mysql")){
            queryFilePath = inputDirStr+"queries/"+ concurrencyStr + "_concurrency/" + "queries_mysql.txt";
        }
        else{
            queryFilePath = inputDirStr+"queries/"+ concurrencyStr + "_concurrency/" + "queries.txt";
        }

        obsSqlFileLoader = new InsertionSqlFileLoader(obsFilePath);
        semObsSqlFileLoader = new InsertionSqlFileLoader(semObsFilePath);
        queriesFileLoader = new QueriesFileLoader(queryFilePath); 

        this.expDurationMinutes = expDurationMinutes;
        this.opQueue = opQueue;

        opCounter = 0;
        init();
    }

    public void init(){
        //pre-load all operations(insertion/query) from file to a list
        loadObsSqlFileToList(opList);
        loadSemObsSqlFileToList(opList);
        loadQueryFileToList(opList);

        System.out.println("Loaded " + String.valueOf(opList.size()) + " operations");

        //sort operation list by timestamp
        Collections.sort(opList, new SortbyTime()); 

        Timestamp minTs = opList.get(0).timestamp;
        Timestamp maxTs = opList.get(opList.size()-1).timestamp;

        String minTsStr = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(minTs);
        String maxTsStr = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(maxTs);

        System.out.println("Min Timestamp: " + minTsStr + " getTime(): "+ String.valueOf(minTs.getTime()));
        System.out.println("Max Timestamp: " + maxTsStr + " getTime(): "+ String.valueOf(maxTs.getTime()));
        //calculating time span shrink factor
        Float expDurationSeconds = (float)expDurationMinutes*60;
        Float actualDurationSeconds = (float)((maxTs.getTime() - minTs.getTime()) / 1000);
        sFactor = expDurationSeconds / actualDurationSeconds;
        System.out.println("sFactor = " + String.valueOf(sFactor));
        // System.out.println("2 minutes in " + String.valueOf(sFactor * 120) + "s");

        //Add end mark
        Operation endMarkOp = new Operation("END", null, null, null);
        opList.add(endMarkOp);
    }

    public void run() 
    { 
        //...........
        Date date = new Date();
        long startTime = date.getTime();
        long duration = 2 * 60 * 1000;
        long endTime = 0;
        //...........


        // Start Ingesting
        long lastOpTime = 0;
        long currOpTime = 0;
        long currTimeStampExeTime = 0;
        long lastTimeStampExeTime = 0;
        long exeTimeDelta = 0;
        long opTimestampDelta = 0;
        long expectedTimeDelta = 0;
        
        for(int i = 0; i < opList.size(); i++){
            //...............
            // date = new Date();
            // endTime = date.getTime();
            // if(endTime >= startTime + duration){
            //     Operation endMarkOp = new Operation("END", null, null, null);
            //     sendOperation(endMarkOp);
            //     System.exit(0);
            //     return;
            // }
            
            //...............


            Operation op = opList.get(i);
            if (i == 0){
                lastOpTime = op.timestamp.getTime();
            }
            if (op.operationStr.equals("END")){
                sendOperation(op);
                return;
            }
            currOpTime = op.timestamp.getTime();

            opCounter = opCounter + 1;
            if (opCounter%10000 == 0){
                System.out.println("Sent " + String.valueOf(opCounter) + " operations");
            }

            if (lastOpTime == currOpTime){
                sendOperation(op);
                continue;
            }else{
                currTimeStampExeTime = System.currentTimeMillis();
                exeTimeDelta = currTimeStampExeTime - lastTimeStampExeTime;
                opTimestampDelta = currOpTime - lastOpTime;
                expectedTimeDelta = (long)(opTimestampDelta * sFactor);
                if (exeTimeDelta < expectedTimeDelta){
                    try{
                        Thread.sleep(expectedTimeDelta - exeTimeDelta);
                    }catch (Exception e) {
                        System.out.println(e);
                    }
                }

                sendOperation(op);
                lastTimeStampExeTime = currTimeStampExeTime;
            }
            lastOpTime = currOpTime;
            
        }

        System.out.println("\n\nAll Files have been Loaded!\n\n");
    } 

    private void loadObsSqlFileToList(ArrayList<Operation> l){
        System.out.println("Loading Observation SQL File: " + obsFilePath); 
        obsSqlFileLoader.load(l);
        //System.out.println(l.size());
    }

    private void loadSemObsSqlFileToList(ArrayList<Operation> l){
        System.out.println("Loading Semantic Observation SQL File: " + semObsFilePath); 
        semObsSqlFileLoader.load(l);
        //System.out.println(l.size());
    }

    private void loadQueryFileToList(ArrayList<Operation> l){
        System.out.println("Loading Queries File: " + queryFilePath); 
        queriesFileLoader.load(l);
        //System.out.println(l.size());
    }

    private void sendOperation(Operation op){
        op.setActualArrivalTimeToNow();
        opQueue.put(op);
    }
} 

class SortbyTime implements Comparator<Operation> 
{
    // Used for sorting in ascending order of timestamp
    public int compare(Operation a, Operation b)
    {
        Long t1 = a.timestamp.getTime();
        Long t2 = b.timestamp.getTime();
        if(t1 > t2)
            return 1;
        else if(t2 > t1)
            return -1;
        else
            return 0;
    }
}