/**
 * Put your copyright and license info here.
 */
package com.datatorrent.setupbug;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    randomGenerator.setNumTuples(500);
    SetupOperator setupOperator = dag.addOperator("SetupOperator", SetupOperator.class);

    dag.addStream("randomData", randomGenerator.out, setupOperator.inputPort);
  }

  public static class SetupOperator implements Operator
  {
    private transient boolean setupCalled = false;
    private transient long windowCount = 0;

    public final DefaultInputPort<Double> inputPort = new DefaultInputPort<Double>() {
      @Override
      public void process(Double t)
      {
        if(!setupCalled) {
          LOG.info("Setup called.");
        }
      }
    };

    public SetupOperator()
    {
    }

    @Override
    public void setup(OperatorContext cntxt)
    {
      setupCalled = true;
    }

    @Override
    public void beginWindow(long l)
    {
      if(windowCount > 10) {
        throw new RuntimeException("End");
      }
    }

    @Override
    public void endWindow()
    {
      windowCount++;
    }

    @Override
    public void teardown()
    {
    }

    private static final Logger LOG = LoggerFactory.getLogger(SetupOperator.class);
  }
}
