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

    /**
     * Run this demo
     */
    public void runDemo(){
        this.init();
        this.compileAndDeploy("select-all", "select * from SensorUpdate");
        //Send a new event
        runtime.getEventService().sendEventBean(new SensorUpdate(25.6, 0.65, 1), "SensorUpdate");
    }

    public static void main(String[] args) {
        SimpleSample simpleSample = new SimpleSample();
        simpleSample.runDemo();
    }
}
