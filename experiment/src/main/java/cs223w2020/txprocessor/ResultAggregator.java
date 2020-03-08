package main.java.cs223w2020.txprocessor;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.io.File;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import main.java.cs223w2020.OperationQueue;
import main.java.cs223w2020.TransactionQueue;
import main.java.cs223w2020.model.Operation;
import main.java.cs223w2020.model.Transaction;

public class ResultAggregator implements Runnable {

    private TransactionQueue resQueue;
    private String outputDirStr;
    private int TxCount;
    private Timestamp expStartTime;

    public ResultAggregator(TransactionQueue resQueue, String resOutputDir){
        this.resQueue = resQueue;
        this.outputDirStr = resOutputDir;

        File dir = new File(resOutputDir); 
        if (dir.mkdir()) { 
            System.out.println("Output Directory is created"); 
        } 
        else { 
            System.out.println("ERROR: Output Directory cannot be created"); 
        } 

        generateExperimentSetting();
        TxCount = 0;
        expStartTime = getNowTs();
    }

    public void run() 
    {
        try {
            Writer txResultWriter = Files.newBufferedWriter(Paths.get(outputDirStr +"/transaction_results.csv"));
            CSVPrinter txResultCsvPrinter = new CSVPrinter(txResultWriter, CSVFormat.DEFAULT.withHeader("TxNumber", "Type", "NumOfOps", "ConstructTime", "BeginTime", "EndTime", "AvgOpResponseTime"));

            Transaction restx = null;
            String txType = null;
            long totalOpResponseTimeInTx = 0;
            long avgOpResponseTimeInTx = 0;
            long opResponseTime = 0;
            while(true){
                restx = resQueue.take();
                if (restx.operations.size()>0){
                    //Do something to update or output the results
                    //System.out.println(restx);
                    if (restx.operations.size()==1 && restx.operations.get(0).operationStr.equals("SELECT")){
                        txType = "QUERY";
                    }
                    else{
                        txType = "INSERT";
                    }
                    int numOps = restx.operations.size();
                    for(int j = 0; j < restx.operations.size(); j++){
                        opResponseTime = restx.endTime.getTime() - restx.operations.get(j).actualArrivalTime.getTime();
                        totalOpResponseTimeInTx = totalOpResponseTimeInTx + opResponseTime;
                    }
                    avgOpResponseTimeInTx = totalOpResponseTimeInTx / numOps;
                    TxCount = TxCount + 1;
                    txResultCsvPrinter.printRecord(TxCount,txType,numOps,restx.constructTime.getTime(),restx.beginTime.getTime(),restx.endTime.getTime(),avgOpResponseTimeInTx);
                    if (TxCount%1000 == 0){
                        System.out.println("Processed " + String.valueOf(TxCount) + " transactions");
                    }

                    totalOpResponseTimeInTx = 0;
                    avgOpResponseTimeInTx = 0;
                    opResponseTime = 0;
                }
                else{
                    //Experiment is end
                    txResultCsvPrinter.flush();
                    return;
                    
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private void generateExperimentSetting(){
        Properties prop = null;
        PrintWriter configWriter = null;
        String configResultFileName = outputDirStr + "/exp_settings.txt";
        try (InputStream input = ResultAggregator.class.getClassLoader().getResourceAsStream("experiment.properties")) {
            FileWriter fileWriter = new FileWriter(configResultFileName);
            configWriter = new PrintWriter(fileWriter);
            prop = new Properties();

            // load a properties file
            prop.load(input);

            configWriter.println( "Experiment Parameters:" );
            // get the property value and print it out
            configWriter.println("--replayer.inputs_directory:\t"+prop.getProperty("replayer.inputs_directory"));
            configWriter.println("--replayer.concurrency:\t\t"+prop.getProperty("replayer.concurrency"));
            configWriter.println("--replayer.experiment_duration:\t"+prop.getProperty("replayer.experiment_duration"));
            configWriter.println("--simulator.policy:\t\t"+prop.getProperty("simulator.policy"));
            configWriter.println("--processor.db:\t\t\t"+prop.getProperty("processor.db"));
            configWriter.println("--processor.mpl:\t\t"+prop.getProperty("processor.mpl"));
            configWriter.println("--processor.tx_isolation_level:\t\t"+prop.getProperty("processor.tx_isolation_level"));

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        configWriter.close();
    }

    public Timestamp getNowTs(){
        Date date= new Date();
        long time = date.getTime();
        Timestamp ts = new Timestamp(time);
        return ts;
    }
} 