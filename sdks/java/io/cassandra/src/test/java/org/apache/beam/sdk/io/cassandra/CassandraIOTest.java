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

import static org.junit.Assert.assertEquals;

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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
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
public class CassandraIOTest implements Serializable {

  private static final String CASSANDRA_KEYSPACE = "beam_ks";
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final String CASSANDRA_TABLE = "beam";

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraIOTest.class);

  private transient Cluster cluster;
  private transient Session session;

  @Before
  public void startCassandra() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml",
        "target/cassandra", 30000);

    cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).withClusterName("beam").build();
    session = cluster.connect();

    try {
      LOGGER.info("Creating the Cassandra keyspace");
      session.execute("CREATE KEYSPACE " + CASSANDRA_KEYSPACE + " WITH REPLICATION = "
          + "{'class':'SimpleStrategy', 'replication_factor':3};");
      LOGGER.info(CASSANDRA_KEYSPACE + " keyspace created");
    } catch (Exception e) {
      // nothing to do
    }

    LOGGER.info("Use the Cassandra keyspace");
    session.execute("USE " + CASSANDRA_KEYSPACE);

    try {
      LOGGER.info("Create Cassandra tables");
      session.execute("CREATE TABLE person(person_id int, person_name text, PRIMARY KEY"
          + "(person_id));");
      session.execute("CREATE TABLE scientist(person_id int, person_name text, PRIMARY KEY"
          + "(person_id));");
    } catch (Exception e) {
      // nothing to do
    }

    LOGGER.info("Insert records");
    String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
        "Newton", "Bohr", "Galilei", "Maxwell"};
    for (int i = 0; i < 1000; i++) {
      int index = i % scientists.length;
      session.execute("INSERT INTO scientist(person_id, person_name) values(" + i + ", '"
          + scientists[index] + "');");
    }
  }

  @After
  public void stopCassandra() throws Exception {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    // read from Cassandra
    PCollection<Scientist> output = pipeline.apply(CassandraIO.<Scientist>read()
        .withConnectionConfiguration(
            CassandraIO.ConnectionConfiguration.create(
                Arrays.asList(CASSANDRA_HOST),
                CASSANDRA_KEYSPACE,
                9042))
        .withTable(CASSANDRA_TABLE)
        .withEntityName(Scientist.class)
        .withRowKey("person_id")
        .withQuery("select * from " + CASSANDRA_KEYSPACE + ".scientist")
        .withCoder(SerializableCoder.of(Scientist.class)));

    PAssert.thatSingleton(output.apply("Count All", Count.<Scientist>globally())).isEqualTo(1000L);

    PCollection<KV<String, Integer>> mapped =
        output.apply(MapElements.via(new SimpleFunction<Scientist, KV<String, Integer>>() {
          public KV<String, Integer> apply(Scientist input) {
            KV<String, Integer> kv = KV.of(input.getName(), input.getId());
            return kv;
          }
        }));

    PAssert.that(mapped
        .apply("Count Scientist", Count.<String, Integer>perKey())
    ).satisfies(new SerializableFunction<Iterable<KV<String, Long>>, Void>() {
      @Override
      public Void apply(Iterable<KV<String, Long>> input) {
        for (KV<String, Long> element : input) {
          assertEquals(element.getKey(), 100L, element.getValue().longValue());
        }
        return null;
      }
    });

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    ArrayList<Person> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      Person person = new Person();
      person.setId(i);
      person.setName("Beam Test");
      data.add(person);
    }

    pipeline.apply(Create.of(data)).apply(
        CassandraIO.<Person>write()
            .withConnectionConfiguration(CassandraIO.ConnectionConfiguration.create(
                Arrays.asList(CASSANDRA_HOST),
                CASSANDRA_KEYSPACE,
                9042
            )));
    pipeline.run();

    ResultSet result = session.execute("select person_id,person_name from "
        + CASSANDRA_KEYSPACE + ".person");
    List<Row> results = result.all();
    assertEquals(1000, results.size());
    for (Row row : results) {
      assertEquals("Beam Test", row.getString("person_name"));
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

    public String toString() {
      return id + ":" + name;
    }

  }

  /**
   * Another simple Cassandra entity on a different table.
   */
  @Table(name = "scientist", keyspace = CASSANDRA_KEYSPACE)
  public static class Scientist extends Person {

  }

}
