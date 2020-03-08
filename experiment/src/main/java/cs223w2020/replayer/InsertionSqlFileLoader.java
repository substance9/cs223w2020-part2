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

public class InsertionSqlFileLoader 
{   
    private String filePathStr;
    private Hashtable<String, Integer> timestampColInTableDict;
    SimpleDateFormat dateFormat;

    public InsertionSqlFileLoader(String fStr){
        filePathStr = fStr;
        dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        timestampColInTableDict = 
                      new Hashtable<String, Integer>(); 
        timestampColInTableDict.put("thermometerobservation", 2);
        timestampColInTableDict.put("wemoobservation", 3);
        timestampColInTableDict.put("wifiapobservation", 2);
        timestampColInTableDict.put("occupancy", 3);
        timestampColInTableDict.put("presence", 3);
    }

    public ArrayList<Operation> load(ArrayList<Operation> opList){
        BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(filePathStr));
			String line = reader.readLine();
			while (line != null) {
                //System.out.println(line);
                Operation op = parse(line);
                if (op != null){
                    opList.add(op);
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
        if (line.startsWith("--") || line.startsWith("SET") || line.length() < 6){
            return null;
        }

        String[] arrLine = line.split(" ",5); //"INSERT INTO [TABLENAME] VALUE (.....)"
        String tableName = arrLine[2];
        String valueString = arrLine[4];

        String[] arrValue = valueString.split(",");
        String timeStampRawStr = arrValue[timestampColInTableDict.get(tableName)];
        String timeStampStr = timeStampRawStr.substring(2, timeStampRawStr.length()-1);
        Timestamp ts = null;

        try {
            Date parsedDate = dateFormat.parse(timeStampStr);
            ts = new Timestamp(parsedDate.getTime());
        } catch(Exception e) { 
            System.out.println("Exception has been" + " caught" + e); 
        }

        // This is a hack. In current insertion data, sensor id is the next attribute after timestamp, so we directly use timestamp col id + 1 to get sensor id col id
        String sensorIdRawStr = arrValue[timestampColInTableDict.get(tableName)+1];
        String sensorIdStr = sensorIdRawStr.substring(2, sensorIdRawStr.length()-3);
        //System.out.println(sensorIdStr);
        Operation op = new Operation("INSERT", ts, sensorIdStr, line);
        //System.out.println(op);
        return op;
    }
} 