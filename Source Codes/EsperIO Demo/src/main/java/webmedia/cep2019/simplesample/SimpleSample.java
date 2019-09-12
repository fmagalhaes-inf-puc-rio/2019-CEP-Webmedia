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
public class SimpleSample {

    Configuration configuration;
    EPCompiler epCompiler;
    CompilerArguments compilerArguments;
    UpdateListener printListener;
    EPRuntime runtime;

    /**
     * Perform initial configurations of the Esper Engine
     */
    private void init(){
        //Log configuration
        org.apache.log4j.BasicConfigurator.configure(new NullAppender()); //This just remove the Warnings
        //org.apache.log4j.BasicConfigurator.configure(); //This prints the logs on the console

        //Get the EPCompiler
        epCompiler = EPCompilerProvider.getCompiler();

        //The configuration is used to configure the Esper engine before the processing starts
        configuration = new Configuration();

        //Add a new event type using a java class
        configuration.getCommon().addEventType(SensorUpdate.class);

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
        EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "select-all");


        //Add the printListener to the created statement
        if(addPrintListener & (statement != null))
            statement.addListener(printListener);


        return (deployment != null) ?deployment.getDeploymentId() :null;
    }

    public void generateInput(String directory){
        File file = new File(directory, "input.txt");
        try {
            file.createNewFile();
            FileOutputStream fos = new FileOutputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            Random random = new Random();
            for (int i = 0; i < 15; i++) {
                double temp = random.nextInt(50) + random.nextDouble();
                double hum = random.nextDouble();
                int room = i % 3;
                String csvLine = "" + temp + "," + hum + "," + room;
                bw.write(csvLine);
                bw.newLine();
            }
            bw.close();
            fos.close();
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public void readCSVInput(){
        try {
            //Check if there is an input file, creates one if there is not
            String directory = System.getProperty("user.dir");
            File inputFile = new File(directory, "input.txt");
            if (!inputFile.exists()) {
                File toCopy = new File(Paths.get(directory).getParent().toString(), "input.txt");
                if (toCopy.exists()) {
                    //input_file.createNewFile();
                    Files.copy(toCopy.toPath(), inputFile.toPath());
                }
                else{
                    generateInput(directory);
                }
            }
            //Create the input DataFlow
            String inputLocation = inputFile.getAbsolutePath();
            //The dataflow is created in a EPL statement
            String createFSEpl =
                    "create dataflow SensorCSVFlow\n" +
                            //FileSource is the type of datasource we are creating
                            //we are generating a stream of SensorUpdate events
                        "FileSource -> sensorstream<SensorUpdate> {\n" +
                            //file attribute specifies the file path
                            "file: 'input.txt', \n" +
                            //propertyNames is the order in wich each event property appears in each csv line
                            "propertyNames: ['temperature','humidity','roomId'], \n" +
                            //repeat the events once after reaching the end of the file
                            "numLoops: 1\n" +
                        "}\n" +
                    //Tells the Runtime to consume the generated stream as input
                    "EventBusSink(sensorstream){}";
            String deploymentId = compileAndDeploy("SensorCSVFlow", createFSEpl, false);
            EPDataFlowInstance instance = runtime.getDataFlowService().instantiate(deploymentId, "SensorCSVFlow");
            instance.run();

        }catch (Exception iex){
            iex.printStackTrace();
        }
    }

    public void generateRules(){
        //Creates new Event Streams for low humidity and High temperature
        compileAndDeploy("createLowHumidity", "create schema LowHumidity () copyfrom SensorUpdate", false);
        compileAndDeploy("createHighTemperature", "create schema HighTemperature () copyfrom SensorUpdate", false);

        compileAndDeploy("insert-LowHumidity", "insert into LowHumidity \n" +
                                                        "select * from SensorUpdate(humidity<0.35)", true);

        compileAndDeploy("insert-LowHumidity", "insert into HighTemperature \n" +
                                                        "select * from SensorUpdate(temperature>35)", true);

        //Creates rules that print each new event
        compileAndDeploy("select-SensorUpdate", "select * from SensorUpdate", true);
        compileAndDeploy("select-LowHumidity", "select * from LowHumidity", true);
        compileAndDeploy("select-HighTemperature", "select * from HighTemperature", true);
    }

    /**
     * Run this demo
     */
    public void runDemo(){
        init();

        generateRules();
        //Send a new event
        readCSVInput();
        runtime.getEventService().sendEventBean(new SensorUpdate(25.6, 0.65, 1), "SensorUpdate");
    }

    public static void main(String[] args) {
        SimpleSample simpleSample = new SimpleSample();
        simpleSample.runDemo();
    }


}
