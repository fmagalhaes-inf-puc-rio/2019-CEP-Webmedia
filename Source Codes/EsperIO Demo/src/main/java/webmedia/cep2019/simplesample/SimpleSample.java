package webmedia.cep2019.simplesample;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;

import com.espertech.esper.runtime.client.*;
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
     */
    private void compileAndDeploy(String label, String epl){
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
        statement.addListener(printListener);
    }

    public void generateInput(String directory){
        File file = new File(directory, "input.txt");
        try {
            file.createNewFile();
            FileOutputStream fos = new FileOutputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            Random random = new Random();
            for (int i = 0; i < 15; i++) {
                double temp = random.nextInt(100) + random.nextDouble();
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

    /**
     * Run this demo
     */
    public void runDemo(){
        this.init();
        this.compileAndDeploy("select-all", "select * from SensorUpdate");
        try {
            String directory = System.getProperty("user.dir");
            File input_file = new File(directory, "input.txt");
            if (!input_file.exists()) {
                File toCopy = new File(Paths.get(directory).getParent().toString(), "input.txt");
                if (toCopy.exists()) {
                    input_file.createNewFile();
                    Files.copy(toCopy.toPath(), input_file.toPath());
                }
                else{
                    generateInput(directory);
                }
            }

        }catch (Exception iex){
            iex.printStackTrace();
        }
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        //Send a new event
        runtime.getEventService().sendEventBean(new SensorUpdate(25.6, 0.65, 1), "SensorUpdate");
    }

    public static void main(String[] args) {
        SimpleSample simpleSample = new SimpleSample();
        simpleSample.runDemo();
    }


}
