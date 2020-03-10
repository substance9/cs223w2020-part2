package cs223w2020.agent;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Agent {
    public static void main( String[] args ){

        System.out.println( "CS223 Part 2 Agent Initiating:" );

        Properties prop = readConfig(args);

        AgentServer server=new AgentServer();
        server.start(Integer.parseInt(prop.getProperty("agent_app_port")));
    }

    private static Properties readConfig( String[] args ){
        Properties prop = null;
        try (InputStream input = Agent.class.getClassLoader().getResourceAsStream("experiment.properties")) {

            prop = new Properties();

            // load a properties file
            prop.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        ArgumentParser parser = ArgumentParsers.newFor("Agent").build().defaultHelp(true)
				.description("CS223 Project Part 2 Experiment - Agent");
        parser.addArgument("-d", "--dbport").required(true).help("DB Port for agent");
        parser.addArgument("-p", "--port").required(true).help("App port");
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
            prop.setProperty("agent_db_port", ns.get("dbport"));
            prop.setProperty("agent_app_port", ns.get("port"));
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
            System.out.println("--agent_db_port:\t"+prop.getProperty("agent_db_port"));
            System.out.println("--agent_app_port:\t\t"+prop.getProperty("agent_app_port"));
            System.out.println("--experiment_id:\t"+prop.getProperty("experiment_id"));
            System.out.println("--simulated_error_id:\t\t"+prop.getProperty("simulated_error_id"));
            System.out.println("--error_transaction_id:\t\t"+prop.getProperty("error_transaction_id"));

        return prop;
    }
 }