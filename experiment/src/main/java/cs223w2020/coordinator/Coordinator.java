package cs223w2020.coordinator;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import cs223w2020.coordinator.OperationQueue;
import cs223w2020.coordinator.TransactionQueue;
import cs223w2020.coordinator.replayer.*;
import cs223w2020.coordinator.txprocessor.TxProcessor;
import cs223w2020.coordinator.txsimulator.*;


public class Coordinator {
    public static void main( String[] args ){
        

        System.out.println( "CS223 Part 2 Coordinator Initiating:" );

        Properties prop = readConfig(args);



        OperationQueue oQueue = new OperationQueue();
        TransactionQueue tQueue = new TransactionQueue();

        // //Start the replayer to read and send data
        TxProcessor txProcessor = new TxProcessor(tQueue,
                                            Integer.parseInt(prop.getProperty("mpl")), 
                                            Integer.parseInt(prop.getProperty("coordinator_db_port")), 
                                            Integer.parseInt(prop.getProperty("num_agents")), 
                                            Integer.parseInt(prop.getProperty("agent_ports_starts")),
                                            prop.getProperty("result.output_dir"));

        txProcessor.connectToAgents();
        Thread txSenderThread = new Thread(txProcessor); 

        txSenderThread.start();

        //Start Transaction Simulator according to the specified policy
        String txSimulationPolicy = prop.getProperty("simulator.policy");
        Thread txSimulator = null;
        if (txSimulationPolicy.equals("single")){
            txSimulator = new Thread(new SingleTxSimulator(oQueue, tQueue)); 
        }
        else if (txSimulationPolicy.equals("batch")){
            txSimulator = new Thread(new BatchTxSimulator(oQueue, tQueue)); 
        }
        else if (txSimulationPolicy.equals("simplebatch")){
            txSimulator = new Thread(new SimpleBatchTxSimulator(oQueue, tQueue)); 
        }
        else{
            System.out.println("ERROR: Transaction simulation policy " + txSimulationPolicy + " not supported");
            return;
        }
        txSimulator.start(); 

        // //Start the replayer to read and send data
        Thread replayer = new Thread(new Replayer(prop.getProperty("replayer.inputs_directory"), 
                                                    "postgres",
                                                    prop.getProperty("replayer.concurrency"),
                                                    Integer.parseInt(prop.getProperty("replayer.experiment_duration")),
                                                    oQueue)); 
        
        System.out.println("Coordinator Starting");
        //long expStartTime = System.currentTimeMillis();
        replayer.start(); 

        // //reaping all threads, ending the experiment
        try
        { 
            replayer.join(); 
        } 
        catch(Exception ex) 
        { 
            System.out.println("Exception has been" + " caught" + ex); 
        } 

        try
        { 
            txSimulator.join(); 
        } 
        catch(Exception ex) 
        { 
            System.out.println("Exception has been" + " caught" + ex); 
        } 


        //long expEndTime = System.currentTimeMillis();
        //System.out.println("Experiment took " + String.valueOf(expEndTime-expStartTime) + "ms to finish");
    }

    private static Properties readConfig( String[] args ){
        Properties prop = null;
        try (InputStream input = Coordinator.class.getClassLoader().getResourceAsStream("experiment.properties")) {

            prop = new Properties();

            // load a properties file
            prop.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        ArgumentParser parser = ArgumentParsers.newFor("Coordinator").build().defaultHelp(true)
                .description("CS223 Project Part 2 Experiment - Coordinator");
        parser.addArgument("-m", "--mpl").required(true).setDefault("2").help("Multiple Processing Level");
		parser.addArgument("-w", "--workload").choices("low","high")
				.setDefault("high").help("Dataset Workload");
        parser.addArgument("-p", "--policy").choices("single","batch","simplebatch")
                .setDefault("simplebatch").help("Tx construction policy");
        parser.addArgument("-d", "--dbport").required(true).help("DB Port for coordinator");
        parser.addArgument("-n", "--num_agents").required(true).help("The Number of agents");
        parser.addArgument("-s", "--agent_ports_starts").required(true).help("AGENT_APP_PORTS_STARTS_AT");
        parser.addArgument("-i", "--id").required(true).help("Experiment ID");
        parser.addArgument("-e", "--error").required(true).help("Simulated Error ID");
        parser.addArgument("-t", "--transaction").required(true).help("Transaction ID for the simulated error");
		Namespace ns = null;

		try {
			ns = parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
        }
        
        if (args.length >= 2){
            //read config from command line args
            prop.setProperty("mpl", ns.get("mpl"));
            prop.setProperty("replayer.concurrency", ns.get("workload"));
            prop.setProperty("simulator.policy", ns.get("policy"));
            prop.setProperty("agent_ports_starts", ns.get("agent_ports_starts"));
            prop.setProperty("num_agents", ns.get("num_agents"));
            prop.setProperty("coordinator_db_port", ns.get("dbport"));
            prop.setProperty("experiment_id", ns.get("id"));
            prop.setProperty("simulated_error_id", ns.get("error"));
            prop.setProperty("error_transaction_id", ns.get("transaction"));
        }
            
        String resultDir = prop.getProperty("result.output_path")
                                                                +"errID_"+prop.getProperty("simulated_error_id")
                                                                +"|transID_"+prop.getProperty("error_transaction_id")
                                                                +"|expID_"+prop.getProperty("experiment_id");
        prop.setProperty("result.output_dir", resultDir);
        System.out.println("--result.output_dir:\t"+resultDir);

        System.out.println( "Experiment Parameters:" );
            // get the property value and print it out
            System.out.println("--replayer.inputs_directory:\t"+prop.getProperty("replayer.inputs_directory"));
            System.out.println("--replayer.concurrency:\t\t"+prop.getProperty("replayer.concurrency"));
            System.out.println("--replayer.experiment_duration:\t"+prop.getProperty("replayer.experiment_duration"));
            System.out.println("--simulator.policy:\t\t"+prop.getProperty("simulator.policy"));
            System.out.println("--agent_ports_starts:\t\t"+prop.getProperty("agent_ports_starts"));
            System.out.println("--mpl:\t\t\t"+prop.getProperty("mpl"));
            System.out.println("--num_agents:\t\t"+prop.getProperty("num_agents"));
            System.out.println("--coordinator_db_port:\t\t"+prop.getProperty("coordinator_db_port"));
            System.out.println("--experiment_id:\t\t"+prop.getProperty("experiment_id"));
            System.out.println("--simulated_error_id:\t\t"+prop.getProperty("simulated_error_id"));
            System.out.println("--error_transaction_id:\t\t"+prop.getProperty("error_transaction_id"));

        
        return prop;
    }
 }