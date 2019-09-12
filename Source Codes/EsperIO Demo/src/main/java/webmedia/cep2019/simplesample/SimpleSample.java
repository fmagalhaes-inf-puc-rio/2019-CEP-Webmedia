package webmedia.cep2019.simplesample;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.dataflow.core.EPDataFlowInstance;
import com.espertech.esper.common.client.dataflow.core.EPDataFlowInstantiationOptions;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esperio.file.FileSinkForge;

import com.espertech.esper.runtime.client.*;
import com.espertech.esperio.file.FileSourceForge;
import org.apache.log4j.varia.NullAppender;
import org.reflections.Reflections;
import webmedia.cep2019.simplesample.event.*;

import javax.imageio.IIOException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.server.ExportException;
import java.util.Random;
import java.util.Set;

public class SimpleSample {

    Configuration configuration;
    EPCompiler epCompiler;
    CompilerArguments compilerArguments;
    UpdateListener printListener;
    EPRuntime runtime;

    String currentDir;

    /**
     * Perform initial configurations of the Esper Engine
     */
    private void init(){
        //Get the current directory
        currentDir = System.getProperty("user.dir");

        //Log configuration
        org.apache.log4j.BasicConfigurator.configure(new NullAppender()); //This just remove the Warnings
        //org.apache.log4j.BasicConfigurator.configure(); //This prints the logs on the console

        //Get the EPCompiler
        epCompiler = EPCompilerProvider.getCompiler();

        //The configuration is used to configure the Esper engine before the processing starts
        configuration = new Configuration();

        //Add the SensorUpdate event
        configuration.getCommon().addEventType(SensorUpdate.class);
        //Add a new event type to each event on the event package
        Reflections reflections = new Reflections("webmedia.cep2019.simplesample.event");

        for(Class eventClass : reflections.getSubTypesOf(SensorUpdate.class)){
            configuration.getCommon().addEventType(eventClass);
        }


        configuration.getCommon().addImport("com.espertech.esperio.file.*");

        //Get the runtime environment
        runtime = EPRuntimeProvider.getDefaultRuntime(configuration);

        //Compiler Arguments based on the configuration
        compilerArguments = new CompilerArguments(configuration);

        //Create an update listener that just prints the event information
        printListener = new UpdateListener() {
            public void update(EventBean[] newData, EventBean[] oldData, EPStatement epStatement, EPRuntime epRuntime) {
                for (int i = 0; i < newData.length; i++) {
                    EventBean event = newData[i];
                    //Print the name of the event type (e.g.: SensorUpdate)
                    System.out.print("{" + event.getEventType().getName() + ": ");

                    //Get the list of event properties
                    String[] propertyNames = event.getEventType().getPropertyNames();

                    //Print the properties and respective values
                    for (String propertyName : propertyNames){
                        System.out.print(propertyName + "=" + event.get(propertyName) + ", ");
                    }
                    System.out.println("}");
                }
            }
        };
    }

    /**
     * Compile and deploy an EPL rule
     * @param label a label for the rule
     * @param epl the EPL rule
     * @param addPrintListener if a PrintListener should be added to the generated EPLStatement
     * @return a String with the deployementId of the rule
     */
    private String compileAndDeploy(String label, String epl, boolean addPrintListener){
        EPCompiled compiledRule = null;
        try{ //Compile the rule to java bytecode
            compiledRule = epCompiler.compile("@name('" + label + "') " + epl, compilerArguments);
        }catch (EPCompileException ex){
            ex.printStackTrace();
        }

        EPDeployment deployment = null;
        try{//Deploy the compiled rule
            deployment = runtime.getDeploymentService().deploy(compiledRule);
        }catch (EPDeployException ex){
            ex.printStackTrace();
        }

        //The statement is a rule already deployed to the runtime environment
        EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), label);


        //Add the printListener to the created statement
        if(addPrintListener & (statement != null))
            statement.addListener(printListener);


        return deployment.getDeploymentId();
    }

    /**
     * Generate a new input file with random values
     * @param directory the directory to create the file
     */
    private void generateInput(String directory){
        File file = new File(directory, "input.txt");
        System.out.println("No input file was found, generating a new one.\nJust a few seconds...");
        try {
            file.createNewFile();
            FileOutputStream fos = new FileOutputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            Random random = new Random();
            for (int i = 0; i < 15; i++) {
                double temp = random.nextInt(50) + random.nextDouble();
                double hum = random.nextDouble();
                int room = i % 3;
                long timestamp = System.currentTimeMillis();
                String csvLine = "" + temp + "," + hum + "," + room + "," + timestamp;
                try {
                    Thread.sleep(100);
                }catch (Exception ex){
                    ex.printStackTrace();
                }
                bw.write(csvLine);
                bw.newLine();
            }
            bw.close();
            fos.close();
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * Create and instantiate a dataflow that will read sensor events from an input file
     */
    private void readCSVInput(){
        try {
            //Check if there is an input file, creates one if there is not
            File inputFile = new File(currentDir, "input.txt");
            if (!inputFile.exists()) {
                File toCopy = new File(Paths.get(currentDir).getParent().toString(), "input.txt");
                if (toCopy.exists()) {
                    //input_file.createNewFile();
                    Files.copy(toCopy.toPath(), inputFile.toPath());
                }
                else{
                    generateInput(currentDir);
                }
            }
            //Create the input DataFlow the dataflow is created in a EPL statement
            String createFileSourceEpl =
                    "create dataflow SensorCSVInput\n" +
                            //FileSource is the type of datasource we are creating
                            //we are generating a stream of SensorUpdate events
                        "FileSource -> sensorstream<SensorUpdate> {\n" +
                            //file attribute specifies the file path
                            "file: 'input.txt', \n" +
                            //propertyNames is the order in wich each event property appears in each csv line
                            "propertyNames: ['temperature','humidity','roomId', 'timestamp'], \n" +
                            //repeat the events once after reaching the end of the file
                            "numLoops: 1\n" +
                        "}\n" +
                    //Tells the Runtime to consume the generated stream as input
                    "EventBusSink(sensorstream){}";
            String deploymentId = compileAndDeploy("SensorCSVInput", createFileSourceEpl, false);
            //Using the deploymentId of the EPLStatement, we create an instance of the Dataflow and run it
            EPDataFlowInstance instance = runtime.getDataFlowService().instantiate(deploymentId, "SensorCSVInput");
            instance.run();

        }catch (Exception iex){
            iex.printStackTrace();
        }
    }

    /**
     * Outputs events of a given type to a csv file
     * @param eventName the name of the event to be outputed
     * @param fileName the name of the file to save the events
     */
    private void outputEventsToFile(String eventName, String fileName){
        String dataflowName = eventName+"CSVOutput";

        //Delete any previous  file
        File testOutputFile = new File(currentDir, fileName);
        if (testOutputFile.exists()) {
            testOutputFile.delete();
        }

        String createFileSinkEPL =
                "create dataflow "+dataflowName+ " \n" +
                        //We take the outstream of the given event type
                        "EventBusSource -> outstream<"+eventName+"> {} \n" +
                        //And send it to a FileSink dataflow that will save the event
                        "FileSink(outstream) { \n" +
                        //file attribute specifies the file path
                        "file: '"+fileName+"', \n" +
                        //If the file already existed would append the new values to the end
                        "append: false \n" +
                        "}";
        String deploymentId = compileAndDeploy(dataflowName, createFileSinkEPL, false);
        EPDataFlowInstance instance = runtime.getDataFlowService().instantiate(deploymentId, dataflowName);
        instance.start();
    }

    /**
     * Generate the CEP rules
     */
    private void generateRules(){
        //Creates rules that print each new event
        compileAndDeploy("select-SensorUpdate", "select * from SensorUpdate", true);

        //Auto generate LowHumidity and HighTemperature events
        compileAndDeploy("insert-LowHumidity", "insert into LowHumidity \n" +
                "select s.temperature as temperature, s.humidity as humidity, s.roomId as roomId, s.timestamp as timestamp \n" +
                "from pattern [every-distinct(s.timestamp) s=SensorUpdate(humidity<0.35)]", true);

        compileAndDeploy("insert-HighTemperature", "insert into HighTemperature \n" +
                "select s.temperature as temperature, s.humidity as humidity, s.roomId as roomId, s.timestamp as timestamp \n" +
                "from pattern [every-distinct(s.timestamp) s=SensorUpdate(temperature>35)]", true);
        /* -------- Note -------
        Notice we used the every-distinct pattern operator on the previous rules, that is because since LowHumidity
        and HighTemperature extend SensorUpdate every LowHumidity and HighTemperature would activate SensorUpdate rules
        we use the every-distinct to avoid lops that would constantly create new LowHumidity and HighTemperature events
        */
    }

    /**
     * Log special events to csv files
     */
    private void logSpecialEvents(){
        outputEventsToFile("LowHumidity", "LowHumidityLog.txt");
        outputEventsToFile("HighTemperature", "HighTemperatureLog.txt");
    }

    /**
     * Run this demo
     */
    private void runDemo(){
        init();

        //Generate the CEP rules
        generateRules();

        //Log special events to csv files
        logSpecialEvents();

        //Reads events from the input file
        readCSVInput();
    }

    public static void main(String[] args) {
        SimpleSample simpleSample = new SimpleSample();
        simpleSample.runDemo();
    }


}
