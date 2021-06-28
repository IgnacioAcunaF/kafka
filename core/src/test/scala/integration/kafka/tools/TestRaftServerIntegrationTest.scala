/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.tools

import scala.collection.Seq
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import kafka.server.KafkaConfig
import kafka.tools.TestRaftServer
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.Exit
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.BeforeEach
import kafka.integration.KafkaServerTestHarness

class TestRaftServerIntegrationTest extends KafkaServerTestHarness {

  override def generateConfigs: Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(1, zkConnect).map(KafkaConfig.fromProps(_, new Properties()))

  val exited = new AtomicBoolean(false)

  @BeforeEach
  override def setUp(): Unit = {
    Exit.setExitProcedure((_, _) => exited.set(true))
    super.setUp()
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    try {
      assertFalse(exited.get())
    } finally {
      Exit.resetExitProcedure()
    }
  }

  @Test
  def testOneSingleQuorumTestRaftServer(): Unit = {
    val serverProps =  new Properties()
    serverProps.put(KafkaConfig.ProcessRolesProp, "controller")
    serverProps.put("node.id", "0")
    serverProps.put("listeners", "PLAINTEXT://localhost:9092")
    serverProps.put("controller.listener.names", "PLAINTEXT")
    serverProps.put("controller.quorum.voters", "0@localhost:9092")
    serverProps.put("log.dirs", "/tmp/kraft-logs")
    
    val throughput = 5000
    val recordSize = 256
    val config = KafkaConfig.fromProps(serverProps, doLog = false)
    val server = new TestRaftServer(config, throughput, recordSize)

    server.startup()
    try {
      TestUtils.waitUntilTrue(() => {
          println(server.workloadGenerator.totalRecordCount())
          server.workloadGenerator.totalRecordCount() > 300000
      }, "Test Raft Server")
    } finally server.shutdown()
  }
}
