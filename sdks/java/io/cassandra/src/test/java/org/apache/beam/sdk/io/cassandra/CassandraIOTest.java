/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.cassandra.service.StorageServiceMBean;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;

/** Tests of {@link CassandraIO}. */
@RunWith(JUnit4.class)
public class CassandraIOTest implements Serializable {

  private static final long NUM_ROWS = 1000L;
  private static final String CASSANDRA_KEYSPACE = "beam_ks";
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final String CASSANDRA_TABLE = "scientist";
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraIOTest.class);
  private static final String STORAGE_SERVICE_MBEAN = "org.apache.cassandra.db:type=StorageService";
  private static final String JMX_PORT = "7199";
  private transient Cluster cluster;
  private transient Session session;
  private CassandraIO.ConnectionConfiguration connectionConfiguration =
      CassandraIO.ConnectionConfiguration.create(
          Arrays.asList(CASSANDRA_HOST), CASSANDRA_KEYSPACE, 9042);

  @Before
  public void startCassandra() throws Exception {
    System.setProperty("cassandra.jmx.local.port", JMX_PORT);
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(
        "/cassandra.yaml", "target/cassandra", 30000);

    cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).withClusterName("beam").build();
    session = cluster.connect();

    LOGGER.info("Creating the Cassandra keyspace");
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + CASSANDRA_KEYSPACE
            + " WITH REPLICATION = "
            + "{'class':'SimpleStrategy', 'replication_factor':3};");
    LOGGER.info(CASSANDRA_KEYSPACE + " keyspace created");

    LOGGER.info("Use the Cassandra keyspace");
    session.execute("USE " + CASSANDRA_KEYSPACE);

    LOGGER.info("Create Cassandra tables");
    // table person is used for writing
    session.execute(
        "CREATE TABLE IF NOT EXISTS person(person_id int, person_name text, PRIMARY KEY"
            + "(person_id));");
    // table scientist is used for reading
    session.execute(
        "CREATE TABLE IF NOT EXISTS scientist(person_id int, person_name text, PRIMARY KEY"
            + "(person_id));");

    LOGGER.info("Insert records");
    String[] scientists = {
      "Einstein",
      "Darwin",
      "Copernicus",
      "Pasteur",
      "Curie",
      "Faraday",
      "Newton",
      "Bohr",
      "Galilei",
      "Maxwell"
    };
    for (int i = 0; i < NUM_ROWS; i++) {
      int index = i % scientists.length;
      session.execute(
          "INSERT INTO scientist(person_id, person_name) values("
              + i
              + ", '"
              + scientists[index]
              + "');");
    }
    flushMemTables();
  }

  /**
   * Force the flush of cassandra memTables to SSTables to update size_estimates. (only JMX is
   * available) https://wiki.apache.org/cassandra/MemtableSSTable
   * https://wiki.apache.org/cassandra/JmxInterface
   */
  private void flushMemTables() throws Exception {
    JMXServiceURL url =
        new JMXServiceURL(
            String.format("service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi", CASSANDRA_HOST, JMX_PORT));
    JMXConnector jmxConnector = JMXConnectorFactory.connect(url, null);
    MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
    ObjectName objectName = new ObjectName(STORAGE_SERVICE_MBEAN);
    StorageServiceMBean mBeanProxy =
        JMX.newMBeanProxy(mBeanServerConnection, objectName, StorageServiceMBean.class);
    mBeanProxy.forceKeyspaceFlush(CASSANDRA_KEYSPACE, CASSANDRA_TABLE);
    jmxConnector.close();
    // did not find any better way to make this call synchronous
    Thread.sleep(30000);
  }

  @After
  public void stopCassandra() throws Exception {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  public void testEstimatedSize() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CassandraIO.Read read =
        CassandraIO.read()
            .withConnectionConfiguration(connectionConfiguration)
            .withTable(CASSANDRA_TABLE);
    CassandraIO.BoundedCassandraSource initialSource =
        new CassandraIO.BoundedCassandraSource(read, null);
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    LOGGER.info("Estimated size: {}", estimatedSize);
    assertEquals("Wrong estimated size", 36864L, estimatedSize);
  }

  /* TODO fails because, right now split query is ignored, so when the splitIntoBundles creates
  n sources, it duplicates dataset by n
   */
  @Test
  public void testSplitIntoBundles() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CassandraIO.Read read =
        CassandraIO.<Scientist>read()
            .withConnectionConfiguration(connectionConfiguration)
            .withTable(CASSANDRA_TABLE)
            .withQuery(String.format("select * from %s.%s", CASSANDRA_KEYSPACE, CASSANDRA_TABLE))
            .withEntityName(Scientist.class)
            .withRowKey("person_id")
            .withCoder(SerializableCoder.of(Scientist.class));

    CassandraIO.BoundedCassandraSource initialSource =
        new CassandraIO.BoundedCassandraSource(read, null);
    int desiredBundleSizeBytes = 100000;
    List<? extends BoundedSource<String>> splits =
        initialSource.splitIntoBundles(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    int expectedNumSplits = 3;
    assertEquals(expectedNumSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertEquals("Wrong number of empty splits", expectedNumSplits, nonEmptySplits);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testRead() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Scientist> output =
        pipeline.apply(
            CassandraIO.<Scientist>read()
                .withConnectionConfiguration(connectionConfiguration)
                //TODO should be extracted from withQuery()
                .withTable(CASSANDRA_TABLE)
                .withEntityName(Scientist.class)
                .withRowKey("person_id")
                .withQuery("select * from " + CASSANDRA_KEYSPACE + ".scientist")
                .withCoder(SerializableCoder.of(Scientist.class)));

    PAssert.thatSingleton(output.apply("Count scientists", Count.<Scientist>globally()))
        .isEqualTo(NUM_ROWS);

    PCollection<KV<String, Integer>> mapped =
        output.apply(
            MapElements.via(
                new SimpleFunction<Scientist, KV<String, Integer>>() {
                  public KV<String, Integer> apply(Scientist input) {
                    KV<String, Integer> kv = KV.of(input.getName(), input.getId());
                    return kv;
                  }
                }));

    PAssert.that(mapped.apply("Count Scientist", Count.<String, Integer>perKey()))
        .satisfies(
            new SerializableFunction<Iterable<KV<String, Long>>, Void>() {
              @Override
              public Void apply(Iterable<KV<String, Long>> input) {
                for (KV<String, Long> element : input) {
                  assertEquals(element.getKey(), NUM_ROWS / 10, element.getValue().longValue());
                }
                return null;
              }
            });

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWrite() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    ArrayList<Person> data = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      Person person = new Person();
      person.setId(i);
      person.setName("Beam Test");
      data.add(person);
    }

    pipeline
        .apply(Create.of(data))
        .apply(CassandraIO.<Person>write().withConnectionConfiguration(connectionConfiguration));
    // table to write to is specified in the entity in @Table annotation (in that cas person)
    pipeline.run();

    ResultSet result =
        session.execute("select person_id,person_name from " + CASSANDRA_KEYSPACE + ".person");
    List<Row> results = result.all();
    assertEquals(NUM_ROWS, results.size());
    for (Row row : results) {
      assertEquals("Beam Test", row.getString("person_name"));
    }
  }

  /** Simple Cassandra entity used in test. */
  @Table(name = "person", keyspace = CASSANDRA_KEYSPACE)
  public static class Person implements Serializable {

    @Column(name = "person_name")
    private String name;

    @Column(name = "person_id")
    private int id;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String toString() {
      return id + ":" + name;
    }
  }

  /** Another simple Cassandra entity on a different table. */
  @Table(name = "scientist", keyspace = CASSANDRA_KEYSPACE)
  public static class Scientist extends Person {}
}
