package cs223w2020.model;

public class Message {
    public enum MessageType {
        START, STATEMENT, PREPARE, VOTEPREPARED, VOTEABORT,
        ACTCOMMIT, ACTABORT, ACK, QUERY 
    }
        //c-a:START (first message to init everything)
        //c->a:STATEMENT
        //c->a:PREPARE
        //a->c:VOTE:PREPARED/ABORT
        //c->a:COMMIT
        //c->a:ABORT
        //a->c:ACK

    public MessageType type;
    
    public int transactionId;
    public String sql;
    public int agentId;
    
    public Message(MessageType t, int transactionId){
        this.type = t;
        this.transactionId = transactionId;
    }

    public Message(){
        
    }
    
    public void setSql(String sqlStr){
        this.sql = sqlStr;
    }

    @Override
    public String toString(){
        if (type == MessageType.STATEMENT){
            return("Message for Transaction <"+String.valueOf(transactionId)+"> | Message Type: " + type.toString() + " | SQL: " + sql);
        }
        else{
            return("Message for Transaction <"+String.valueOf(transactionId)+"> | Message Type: " + type.toString());
        }
        
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public String getSql() {
        return sql;
    }

}

