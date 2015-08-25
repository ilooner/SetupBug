/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.setupbug;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetupOperator implements Operator
{
  private transient boolean setupCalled = false;
  private transient long windowCount = 0;

  public final DefaultInputPort<Double> inputPort = new DefaultInputPort<Double>()
  {
    @Override
    public void process(Double t)
    {
      if (!setupCalled) {
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
    if (windowCount > 10) {
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
