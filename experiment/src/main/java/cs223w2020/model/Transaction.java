package cs223w2020.model;

import java.sql.Timestamp;
import java.util.Date;
import java.util.ArrayList;

public class Transaction {
    public int transactionId;
    public Timestamp constructTime;
    public Timestamp beginTime;
    public Timestamp endTime;
    public ArrayList<Operation> operations;

    public Transaction(){
        constructTime = null;
        beginTime = null;
        endTime = null;
        operations = new ArrayList<Operation>();
    }

    public void appendOperation(Operation op){
        operations.add(op);
    }

    public void setConstructTimeToNow(){
        this.constructTime = getNowTs();
    }

    public void setBeginTimeToNow(){
        this.beginTime = getNowTs();
    }

    public void setEndTimeToNow(){
        this.endTime = getNowTs();
    }

    public Timestamp getNowTs(){
        Date date= new Date();
        long time = date.getTime();
        Timestamp ts = new Timestamp(time);
        return ts;
    }

    @Override
    public String toString()
    {
         return "Transaction: " + operations;
    }
}

