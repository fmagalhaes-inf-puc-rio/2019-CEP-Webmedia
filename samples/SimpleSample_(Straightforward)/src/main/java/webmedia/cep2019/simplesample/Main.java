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

public class Main {

    public static void main(String[] args) {

        //Log configuration
        org.apache.log4j.BasicConfigurator.configure(new NullAppender()); //This just remove the Warnings
        //org.apache.log4j.BasicConfigurator.configure(); //This prints the logs on the console

        //Get the EPCompiler
        EPCompiler epCompiler = EPCompilerProvider.getCompiler();

        //The configuration is used to configure the Esper engine before the processing starts
        Configuration configuration = new Configuration();
        //Add a new event type using a java class
        configuration.getCommon().addEventType(SensorUpdate.class);

        //Compiler Arguments based on the configuration
        CompilerArguments compilerArguments = new CompilerArguments(configuration);

        EPCompiled compiledRule = null;
        try{ //Compile the rule to java bytecode
            compiledRule = epCompiler.compile("@name('select-all') select * from SensorUpdate", compilerArguments);
        }catch (EPCompileException ex){
            ex.printStackTrace();
        }

        //Get the runtime environment
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);

        EPDeployment deployment = null;
        try{//Deploy the compiled rule
            deployment = runtime.getDeploymentService().deploy(compiledRule);
        }catch (EPDeployException ex){
            ex.printStackTrace();
        }

        EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "select-all");

        //Create an update listener that prints the event properties
        UpdateListener printListener = new UpdateListener() {
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

        //Add the printListener to the created statement
        statement.addListener(printListener);

        //Send a new event
        runtime.getEventService().sendEventBean(new SensorUpdate(25.6, 0.65, 1), "SensorUpdate");
    }
}
