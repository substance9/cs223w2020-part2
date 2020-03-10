package cs223w2020.model;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

public class Message {
    public String type;
    //STATEMENTS
    //PREPARE
    //VOTE:PREPARED/ABORT
    //COMMIT
    //ABORT
    //ACK
    
    public int transactionId;
    public ArrayList<String> sqlList;
    
    public Message(String typeStr, int transactionId){
        this.type = typeStr;
        this.transactionId = transactionId;
        sqlList = new ArrayList<String>();
    }

    // @Override
    // public String toString()
    // {
    //      return "Operation: " + operationStr + " - Timestamp: " + timestamp.toLocalDateTime() + " - SensorID: "  + sensorId + " - SQLStr: "  + sqlStr;
    // }

    public void appendSql(String sql){
        sqlList.add(sql);
    }

}

