package main.java.cs223w2020.replayer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Hashtable;
import java.util.ArrayList;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import main.java.cs223w2020.model.Operation;

public class QueriesFileLoader 
{   
    private String filePathStr;
    SimpleDateFormat dateFormat;

    public QueriesFileLoader(String fStr){
        filePathStr = fStr;
        dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
    }

    public ArrayList<Operation> load(ArrayList<Operation> opList){
        BufferedReader reader;
        StringBuilder realLine = new StringBuilder();
        int state = 0;
        try {
            reader = new BufferedReader(new FileReader(filePathStr));
            String line = reader.readLine();
            while (line != null) {
                if (line.length() < 1){
                    continue;
                }
                //System.out.println(line);

                if (line.length() > 1){
                    if (state == 0){
                        state = 1;
                    }
                    realLine.append(" ");
                    realLine.append(line);
                }
                else if (line.length() == 1){
                    if (state == 0){
                        //something is wrong
                        continue;
                    }
                    else if (state == 1){
                        realLine.append(line);
                        Operation op = parse(realLine.toString());
                        if (op != null){
                            opList.add(op);
                        }
                        realLine = new StringBuilder();
                        state = 0;
                    }
                    
                }
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return opList;
    }

    private Operation parse(String line){
        String[] arrLine = line.split(",",2); //2017-11-08T00:08:00Z,"SELECT ci.INFRASTRUCTURE_ID FROM SENSOR sen, COVERAGE_INFRASTRUCTURE ci WHERE sen.id=ci.SENSOR_ID AND sen.id='a48b9428_7661_49f1_b920_153ba738b664'"
        String timeStampStr = arrLine[0];
        Timestamp ts = null;

        try {
            Date parsedDate = dateFormat.parse(timeStampStr);
            ts = new Timestamp(parsedDate.getTime());
        } catch(Exception e) { 
            System.out.println("Exception has been" + " caught" + e); 
        }

        String sqlStr = arrLine[1].substring(1, arrLine[1].length()-1);

        //System.out.println(sensorIdStr);
        Operation op = new Operation("SELECT", ts, null, sqlStr);
        //System.out.println(op);
        return op;
    }
} 