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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.CassandraDaemon;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Tests of {@link CassandraIO}.
 */
@RunWith(JUnit4.class)
public class CassandraIOTest {

  private static final String CASSANDRA_KEYSPACE = "beam";
  private static final int CASSANDRA_PORT = 9042;
  private static final String CASSANDRA_HOST = "localhost";
  private static final String CASSANDRA_TABLE = "beam";

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraIOTest.class);

  private CassandraDaemon cassandraDaemon;

  @Before
  public void startCassandra() throws Exception {

    System.setProperty("cassandra-foreground", "false");
    System.setProperty("cassandra.boot_without_jna", "true");
    System.setProperty("cassandra.storagedir", "/tmp");

    LOGGER.info("Starting Apache Cassandra");
    cassandraDaemon = new CassandraDaemon(true);
    cassandraDaemon.init(null);
    cassandraDaemon.start();
  }

  @After
  public void stopCassandra() throws Exception {
    Schema.instance.clear();
    cassandraDaemon.stop();
    cassandraDaemon.destroy();
  }

  @Test
  @Ignore
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    LOGGER.info("Insert data in Cassandra");

    Cluster cluster = Cluster.builder().addContactPoints(new String[]{ CASSANDRA_HOST })
        .withPort(CASSANDRA_PORT).withoutMetrics().withoutJMXReporting().build();
    Session session = cluster.connect();

    try {
      session.execute("CREATE KEYSPACE " + CASSANDRA_KEYSPACE + " WITH REPLICATION = "
          + "{'class':'SimpleStrategy', 'replication_factory':3};");
      LOGGER.info(CASSANDRA_KEYSPACE + " keyspace created");
    } catch (AlreadyExistsException e) {
      // nothing to do
    }

    session.execute("USE " + CASSANDRA_KEYSPACE);

    try {
      session.execute("CREATE TABLE person(person_id int PRIMARY_KEY, person_name text);");
    } catch (AlreadyExistsException e) {
      // nothing to do
    }

    LOGGER.info("Insert records");
    session.execute("INSERT INTO person(person_id, person_name) values(0, 'John Foo');");
    session.execute("INSERT INTO person(person_id, person_name) values(1, 'David Bar');");

    session.close();
    cluster.close();

    Pipeline pipeline = TestPipeline.create();

    // read from Cassandra
    PCollection read = pipeline.apply(CassandraIO.read()
        .withHosts(new String[]{ CASSANDRA_HOST })
        .withPort(CASSANDRA_PORT)
        .withEntityName(Person.class)
        .withQuery("select * from person"));

    pipeline.run();
  }

  /**
   * Simple Cassandra entity used in test.
   */
  @Table(name = "person")
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
