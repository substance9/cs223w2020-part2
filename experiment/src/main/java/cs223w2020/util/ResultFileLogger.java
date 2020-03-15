package cs223w2020.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class ResultFileLogger {
    private String fileName;
    private BufferedWriter writer;

    public ResultFileLogger(String fName) {
        this.fileName = fName;
        try {
             writer= new BufferedWriter(new FileWriter(fileName, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeln(String str){
        try {
            writer.write(str + "\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    public void close(){
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}