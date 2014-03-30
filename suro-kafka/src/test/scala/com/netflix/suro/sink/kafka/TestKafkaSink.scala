package com.netflix.suro.sink.kafka

import kafka.utils._
import com.netflix.suro.message._
import com.netflix.suro.jackson.DefaultObjectMapper
import com.netflix.suro.sink.Sink
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.commons.io.FileUtils
import java.io.File
import org.mockito.Mockito.mock
import org.junit.Assert._
import com.netflix.suro.thrift.TMessageSet
import java.net.ServerSocket
import java.util.{Random, Properties}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import com.netflix.suro.ClientConfig
import org.scalatest.junit.JUnit3Suite
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.consumer.SimpleConsumer
import org.I0Itec.zkclient.{IDefaultNameSpace, ZkClient, ZkServer}
import kafka.api.FetchRequestBuilder
import com.fasterxml.jackson.databind.jsontype.NamedType
import scala.Some
import kafka.common.TopicAndPartition
import kafka.message.MessageAndOffset
import kafka.admin.TopicCommand.TopicCommandOptions

class TestKafkaSink extends JUnit3Suite {
  private val brokerId1 = 0
  private val brokerId2 = 1
  private val ports = choosePorts(2)
  private val (port1, port2) = (ports(0), ports(1))
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null
  private var zkServer: ZkServer = null
  private var zkClient: ZkClient = null

  private var servers = List.empty[KafkaServer]

  private val props1 = createBrokerConfig(brokerId1, port1)
  private val config1 = new KafkaConfig(props1) {
    override val hostName = "localhost"
    override val numPartitions = 1
  }
  private val props2 = createBrokerConfig(brokerId2, port2)
  private val config2 = new KafkaConfig(props2) {
    override val hostName = "localhost"
    override val numPartitions = 1
  }

  override def setUp() {
    zkServer = startZkServer("TestKafkaSink", 4711)
    zkClient = new ZkClient("localhost:4711", 20000, 20000, ZKStringSerializer)
    // set up 2 brokers with 4 partitions each
    server1 = createServer(config1)
    server2 = createServer(config2)
    servers = List(server1,server2)

    consumer1 = new SimpleConsumer("localhost", port1, 1000000, 64*1024, "")
    consumer2 = new SimpleConsumer("localhost", port2, 1000000, 64*1024, "")
  }

  override def tearDown() {
    server1.shutdown
    server1.awaitShutdown()
    server2.shutdown
    server2.awaitShutdown()
    Utils.rm(server1.config.logDirs)
    Utils.rm(server2.config.logDirs)
    zkServer.shutdown()
  }

  def test() {
    val topic = "routingKey"
    // create topic with 1 partition and await leadership
    kafka.admin.TopicCommand.createTopic(zkClient,
      new TopicCommandOptions(Array[String]("--zookeeper", "localhost:4711", "create", "--topic", topic)))
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)

    val description = "{\n" +
      "    \"type\": \"kafka\",\n" +
      "    \"client.id\": \"kafkasink\",\n" +
      "    \"metadata.broker.list\": \"" + getBrokerListStrFromConfigs(Seq(config1, config2)) + "\",\n" +
      "    \"request.required.acks\": 1\n" +
      "}"

    val jsonMapper = new DefaultObjectMapper()
    jsonMapper.registerSubtypes(new NamedType(classOf[KafkaSink], "kafka"))
    val sink: KafkaSink = jsonMapper.readValue(description, new TypeReference[Sink](){})
    sink.open()
    val i = new MessageSetReader(createMessageSet(2)).iterator()
    while (i.hasNext) {
      sink.writeTo(new StringMessage(i.next))
    }
    sink.close()
    println(sink.getStat())

    // get the leader
    val leaderOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isDefined)
    val leader = leaderOpt.get


    val messageSet = if(leader == server1.config.brokerId) {
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 100000).build())
      response1.messageSet(topic, 0).iterator.toBuffer
    }else {
      val response2 = consumer2.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 100000).build())
      response2.messageSet(topic, 0).iterator.toBuffer
    }
    assertEquals("Should have fetched 2 messages", 2, messageSet.size)

    assertEquals(new String(createMsg(messageSet, 0)),"testMessage" + 0)
    assertEquals(new String(createMsg(messageSet, 1)), "testMessage" + 1)
  }

  def createMsg(messageSet: mutable.Buffer[MessageAndOffset], offset: Int): Array[Byte] = {
    val bb = messageSet(offset).message.payload
    val bytes = new Array[Byte](bb.remaining())
    bb.get(bytes, 0, bytes.length)
    bytes
  }

  def startZkServer(testName: String, port: Int): ZkServer = {
    val dataPath = "./build/test/" + testName + "/data"
    val logPath = "./build/test/" + testName + "/log"
    FileUtils.deleteDirectory(new File(dataPath))
    FileUtils.deleteDirectory(new File(logPath))
    val zkServer = new ZkServer(dataPath, logPath, mock(classOf[IDefaultNameSpace]), port, ZkServer.DEFAULT_TICK_TIME, 100)
    zkServer.start()
    return zkServer
  }

  def createMessageSet(numMsgs: Int): TMessageSet = {
    val builder = new MessageSetBuilder(new ClientConfig)
      .withCompression(Compression.LZF)

    for(i <- 0 to numMsgs - 1) {
      builder.withMessage(
        "routingKey",
        ("testMessage" +i).getBytes())
    }

    return builder.build()
  }

  def choosePorts(count: Int): List[Int] = {
    val sockets =
      for(i <- 0 until count)
      yield new ServerSocket(0)
    val socketList = sockets.toList
    val ports = socketList.map(_.getLocalPort)
    socketList.map(_.close)
    ports
  }

  /**
   * Create a test config for the given node id
   */
  def createBrokerConfigs(numConfigs: Int): List[Properties] = {
    for((port, node) <- choosePorts(numConfigs).zipWithIndex)
    yield createBrokerConfig(node, port)
  }

  def getBrokerListStrFromConfigs(configs: Seq[KafkaConfig]): String = {
    configs.map(c => c.hostName + ":" + c.port).mkString(",")
  }

  /**
   * Create a test config for the given node id
   */
  def createBrokerConfig(nodeId: Int, port: Int): Properties = {
    val props = new Properties
    props.put("broker.id", nodeId.toString)
    props.put("host.name", "localhost")
    props.put("port", port.toString)
    props.put("log.dir", tempDir().getAbsolutePath)
    props.put("log.flush.interval.messages", "1")
    props.put("zookeeper.connect", "localhost:4711")
    props.put("replica.socket.timeout.ms", "1500")
    props
  }

  /**
   * Create a test config for a consumer
   */
  def createConsumerProperties(zkConnect: String, groupId: String, consumerId: String,
                               consumerTimeout: Long = -1): Properties = {
    val props = new Properties
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("consumer.id", consumerId)
    props.put("consumer.timeout.ms", consumerTimeout.toString)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("rebalance.max.retries", "4")
    props.put("auto.offset.reset", "smallest")

    props
  }

  /**
   * Create a kafka server instance with appropriate test settings
   * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
   * @param config The configuration of the server
   */
  def createServer(config: KafkaConfig, time: Time = SystemTime): KafkaServer = {
    val server = new KafkaServer(config, time)
    server.startup()
    server
  }

  def tempDir(): File = {
    val ioDir = System.getProperty("java.io.tmpdir")
    val f = new File(ioDir, "kafka-" + new Random().nextInt(1000000))
    f.mkdirs()
    f.deleteOnExit()
    f
  }

  def waitUntilLeaderIsElectedOrChanged(zkClient: ZkClient, topic: String, partition: Int, timeoutMs: Long, oldLeaderOpt: Option[Int] = None): Option[Int] = {
    val leaderLock = new ReentrantLock()
    val leaderExistsOrChanged = leaderLock.newCondition()

    if(oldLeaderOpt == None)
      println("Waiting for leader to be elected for partition [%s,%d]".format(topic, partition))
    else
      println("Waiting for leader for partition [%s,%d] to be changed from old leader %d".format(topic, partition, oldLeaderOpt.get))

    leaderLock.lock()
    try {
      zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), new LeaderExistsOrChangedListener(topic, partition, leaderLock, leaderExistsOrChanged, oldLeaderOpt, zkClient))
      leaderExistsOrChanged.await(timeoutMs, TimeUnit.MILLISECONDS)
      // check if leader is elected
      val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
      leader match {
        case Some(l) =>
          if(oldLeaderOpt == None)
            println("Leader %d is elected for partition [%s,%d]".format(l, topic, partition))
          else
            println("Leader for partition [%s,%d] is changed from %d to %d".format(topic, partition, oldLeaderOpt.get, l))
        case None => error("Timing out after %d ms since leader is not elected for partition [%s,%d]"
          .format(timeoutMs, topic, partition))
      }
      leader
    } finally {
      leaderLock.unlock()
    }
  }

  /**
   * Wait until the given condition is true or the given wait time ellapses
   */
  def waitUntilTrue(condition: () => Boolean, waitTime: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    while (true) {
      if (condition())
        return true
      if (System.currentTimeMillis() > startTime + waitTime)
        return false
      Thread.sleep(waitTime.min(100L))
    }
    // should never hit here
    throw new RuntimeException("unexpected error")
  }
}
