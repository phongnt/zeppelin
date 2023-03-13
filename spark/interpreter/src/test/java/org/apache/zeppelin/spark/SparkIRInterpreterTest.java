
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.spark;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResultMessage;
import org.apache.zeppelin.interpreter.LazyOpenInterpreter;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterEventClient;
import org.apache.zeppelin.r.IRInterpreterTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SparkIRInterpreterTest extends IRInterpreterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkIRInterpreterTest.class);

  private final RemoteInterpreterEventClient mockRemoteIntpEventClient = mock(RemoteInterpreterEventClient.class);

  @Override
  protected Interpreter createInterpreter(Properties properties) {
    return new SparkIRInterpreter(properties);
  }

  @Override
  @BeforeEach
  public void setUp() throws InterpreterException {
    LOGGER.debug("Start setupUp");
    Properties properties = new Properties();
    properties.setProperty(SparkStringConstants.MASTER_PROP_NAME, "local");
    properties.setProperty(SparkStringConstants.APP_NAME_PROP_NAME, "test");
    properties.setProperty("zeppelin.spark.maxResult", "100");
    properties.setProperty("spark.r.backendConnectionTimeout", "10");
    properties.setProperty("zeppelin.spark.deprecatedMsg.show", "false");
    properties.setProperty("spark.sql.execution.arrow.sparkr.enabled", "false");

    InterpreterContext context = getInterpreterContext();
    InterpreterContext.set(context);
    interpreter = createInterpreter(properties);
    LOGGER.debug("Interpreter created");
    InterpreterGroup interpreterGroup = new InterpreterGroup();
    interpreterGroup.addInterpreterToSession(new LazyOpenInterpreter(interpreter), "session_1");
    interpreter.setInterpreterGroup(interpreterGroup);

    SparkInterpreter sparkInterpreter = new SparkInterpreter(properties);
    interpreterGroup.addInterpreterToSession(new LazyOpenInterpreter(sparkInterpreter), "session_1");
    sparkInterpreter.setInterpreterGroup(interpreterGroup);
    LOGGER.debug("Spark Interpreter created");
    interpreter.open();
    LOGGER.debug("End setupUp");
  }


  @Test
  public void testSparkRInterpreter() throws InterpreterException, InterruptedException, IOException {
    InterpreterContext context = getInterpreterContext();
    InterpreterResult result = interpreter.interpret("1+1", context);
    LOGGER.debug("1+1=" + result.toJson());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    List<InterpreterResultMessage> interpreterResultMessages = context.out.toInterpreterResultMessage();
    LOGGER.debug("1+1= context out " + context.out.toString());
    assertTrue(interpreterResultMessages.get(0).getData().contains("2"));

    context = getInterpreterContext();
    result = interpreter.interpret("sparkR.version()", context);
    LOGGER.debug("sparkR.version() is " + result.toJson());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    LOGGER.debug("sparkR.version() context out " + context.out.toString());
    if (interpreterResultMessages.get(0).getData().contains("2.2")) {
      ENABLE_GOOGLEVIS_TEST = false;
    }
    context = getInterpreterContext();
    result = interpreter.interpret("df <- as.DataFrame(faithful)\nhead(df)", context);
    LOGGER.debug("head(df) is " + result.toJson());
    interpreterResultMessages = context.out.toInterpreterResultMessage();
    LOGGER.debug("head(df) context out " + context.out.toString());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code(), context.out.toString());
    assertTrue(interpreterResultMessages.get(0).getData().contains(">eruptions</th>"));
    // spark job url is sent
    verify(mockRemoteIntpEventClient, atLeastOnce()).onParaInfosReceived(any(Map.class));

    // cancel
    final InterpreterContext context2 = getInterpreterContext();
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          LOGGER.debug("Start running");
          InterpreterResult result = interpreter.interpret("ldf <- dapplyCollect(\n" +
                  "         df,\n" +
                  "         function(x) {\n" +
                  "           Sys.sleep(3)\n" +
                  "           x <- cbind(x, \"waiting_secs\" = x$waiting * 60)\n" +
                  "         })\n" +
                  "head(ldf, 3)", context2);
          LOGGER.debug("context2 out is " + context2.out.toString());
          LOGGER.debug("Interpreter cancelled then result: " + result.toJson());
          assertTrue(result.message().get(0).getData().contains("cancelled"));
        } catch (InterpreterException e) {
          fail("Should not throw InterpreterException");
        }
      }
    };
    LOGGER.debug("Start Cancel-Thread");
    thread.setName("Cancel-Thread");
    thread.start();
    Thread.sleep(100);
    LOGGER.debug("Cancel context");
    interpreter.cancel(context2);
  }

  @Override
  protected InterpreterContext getInterpreterContext() {
    return InterpreterContext.builder()
            .setNoteId("note_1")
            .setParagraphId("paragraph_1")
            .setInterpreterOut(new InterpreterOutput())
            .setLocalProperties(new HashMap<>())
            .setIntpEventClient(mockRemoteIntpEventClient)
            .build();
  }
}
