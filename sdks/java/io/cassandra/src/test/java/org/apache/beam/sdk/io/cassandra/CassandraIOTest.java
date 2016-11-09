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
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.CassandraDaemon;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests of {@link CassandraIO}.
 */
@RunWith(JUnit4.class)
public class CassandraIOTest {

  private static final String CASSANDRA_KEYSPACE = "beam";
  private static final int CASSANDRA_PORT = 9142;
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final String CASSANDRA_TABLE = "beam";

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraIOTest.class);

  private CassandraDaemon cassandraDaemon;
  private Cluster cluster;
  private Session session;

  @Before
  public void startCassandra() throws Exception {

    System.setProperty("cassandra-foreground", "false");
    System.setProperty("cassandra.boot_without_jna", "true");

    cassandraDaemon = new CassandraDaemon(true);
    LOGGER.info("Starting Apache Cassandra daemon");
    cassandraDaemon.init(null);
    cassandraDaemon.start();

    LOGGER.info("Apache Cassandra up & running");

    Thread.sleep(10000);

    LOGGER.info("Init Cassandra client");
    cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).withPort(CASSANDRA_PORT)
        .withClusterName("beam-cluster").build();
    session = cluster.connect();

    try {
      LOGGER.info("Creating the Cassandra keyspace");
      session.execute("CREATE KEYSPACE " + CASSANDRA_KEYSPACE + " WITH REPLICATION = "
          + "{'class':'SimpleStrategy', 'replication_factor':3};");
      LOGGER.info(CASSANDRA_KEYSPACE + " keyspace created");
    } catch (AlreadyExistsException e) {
      // nothing to do
    }

    LOGGER.info("Use the Cassandra keyspace");
    session.execute("USE " + CASSANDRA_KEYSPACE);

    try {
      LOGGER.info("Create Cassandra table");
      session.execute("CREATE TABLE person(person_id int, person_name text, PRIMARY KEY"
          + "(person_id));");
    } catch (AlreadyExistsException e) {
      // nothing to do
    }

    LOGGER.info("Insert records");
    session.execute("INSERT INTO person(person_id, person_name) values(0, 'John Foo');");
    session.execute("INSERT INTO person(person_id, person_name) values(1, 'David Bar');");
  }

  @After
  public void stopCassandra() throws Exception {
    if (session != null) {
      session.close();
    }
    if (cluster != null) {
      cluster.close();
    }

    Schema.instance.clear();
    if (cassandraDaemon != null) {
      LOGGER.info("Stopping Apache Cassandra");
      cassandraDaemon.stop();
      LOGGER.info("Destroying Apache Cassandra daemon");
      cassandraDaemon.destroy();
      cassandraDaemon = null;
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    // read from Cassandra
    PCollection<Person> read = pipeline.apply(CassandraIO.<Person>read()
        .withConnectionConfiguration(
            CassandraIO.ConnectionConfiguration.create(
                Arrays.asList(CASSANDRA_HOST),
                CASSANDRA_KEYSPACE,
                CASSANDRA_PORT))
        .withTable(CASSANDRA_TABLE)
        .withEntityName(Person.class)
        .withRowKey("person_id")
        .withQuery("select * from " + CASSANDRA_KEYSPACE + ".person"));

    PAssert.thatSingleton(read.apply("Count", Count.<Person>globally())).isEqualTo(2L);

    pipeline.run();
  }


  /**
   * This test works when executed alone, but fails when mixed with testRead().
   * I'm investigating.
   *
   * @throws Exception
   */
  @Test
  @Ignore
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    Person person = new Person();
    person.setName("Beam Test");

    pipeline.apply(Create.of(person)).apply(
        CassandraIO.<Person>write()
            .withConnectionConfiguration(CassandraIO.ConnectionConfiguration.create(
                Arrays.asList(CASSANDRA_HOST),
                CASSANDRA_KEYSPACE,
                CASSANDRA_PORT
            )));

    ResultSet result = session.execute("select * from " + CASSANDRA_KEYSPACE + ".person where "
        + "person_name = 'Beam Test' ALLOW FILTERING");
    for (Row row : result.all()) {
      Assert.assertEquals("Beam Test", row.getString("person_name"));
    }
  }

  /**
   * Simple Cassandra entity used in test.
   */
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

  }

}
