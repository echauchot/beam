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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to read and write on Apache Cassandra.
 *
 * <h3>Reading from Apache Cassandra</h3>
 *
 * <p>CassandraIO provides a source to read and returns a bounded collection of entities as {@code
 * PCollection<Entity>}. An entity is built by Cassandra mapper based on a POJO containing
 * annotations.
 *
 * <p>To configure a Cassandra source, you have to provide the hosts, port, and keyspace of the
 * Cassandra instance, wrapped as a {@link ConnectionConfiguration}. The following example
 * illustrate various options for configuring the IO:
 *
 * <pre>{@code
 * pipeline.apply(CassandraIO.<Person>read()
 *     .withConnectionConfiguration(CassandraIO.ConnectionConfiguration.create(
 *        Arrays.asList("host1", "host2"),
 *        9042,
 *        "beam"))
 *     .withQuery("select * from Person")
 *     .withEntityName(Person.class)
 *     // above options are the minimum set, returns PCollection<Person>
 *
 * }</pre>
 *
 * <h3>Writing to Apache Cassandra</h3>
 *
 * <p>CassandraIO write provides a sink to write to Apache Cassandra. It expects a {@code
 * PCollection} of entities that will be mapped and written in Cassandra.
 *
 * <p>To configure the write, you have to specify hosts, port, keyspace wrapped as a {@link
 * ConnectionConfiguration} and the entity:
 *
 * <pre>{@code
 * pipeline
 *     .apply(...) // provides PCollection<Person>
 *     .apply(CassandraIO.<Person>write()
 *        .withConnectionConfiguration(CassandraIO.ConnectionConfiguration.create(
 *           Arrays.asList("host1", "host2"),
 *           9042,
 *           "beam"))
 *        .withEntityName(Person.class);
 *
 * }</pre>
 */
public class CassandraIO {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraIO.class);

  private CassandraIO() {}

  public static <T> Read<T> read() {
    return new AutoValue_CassandraIO_Read.Builder<T>().build();
  }

  public static <T> Write<T> write() {
    return new AutoValue_CassandraIO_Write.Builder<T>().build();
  }

  /** POJO describing a connection to Apache Cassandra database. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {
    public static ConnectionConfiguration create(List<String> hosts, String keyspace, int port) {
      checkArgument(
          hosts != null,
          "ConnectionConfiguration.create(hosts, keyspace, port) " + "called with null hosts");
      checkArgument(
          hosts.size() >= 1,
          "ConnectionConfiguration.create(hosts, keyspace, port) "
              + "called with an empty hosts list");
      checkArgument(
          keyspace != null,
          "ConnectionConfiguration.create(hosts, keyspace, port) " + "called with null keyspace");
      checkArgument(
          port > 0,
          "ConnectionConfiguration.create(hosts, keyspace, port) called "
              + "with invalid port number");
      return new AutoValue_CassandraIO_ConnectionConfiguration.Builder()
          .setHosts(hosts)
          .setKeyspace(keyspace)
          .setPort(port)
          .build();
    }

    abstract List<String> getHosts();

    abstract String getKeyspace();

    abstract int getPort();

    abstract Builder builder();

    public void populateDisplayData(DisplayData.Builder builder) {
      builder.addIfNotNull(DisplayData.item("hosts", getHosts().toString()));
      builder.addIfNotNull(DisplayData.item("keyspace", getKeyspace()));
      builder.add(DisplayData.item("port", getPort()));
    }

    Cluster getCluster() {
      LOG.debug("Connecting to Cassandra cluster");
      return Cluster.builder()
          .addContactPoints(getHosts().toArray(new String[0]))
          .withPort(getPort())
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHosts(List<String> hosts);

      abstract Builder setKeyspace(String keyspace);

      abstract Builder setPort(int port);

      abstract ConnectionConfiguration build();
    }
  }

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link CassandraIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    abstract String getQuery();

    @Nullable
    abstract String getTable();

    @Nullable
    abstract String getRowKey();

    @Nullable
    abstract Class<T> getEntityName();

    @Nullable
    abstract Coder<T> getCoder();

    abstract Builder<T> builder();

    public Read<T> withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(
          connectionConfiguration != null,
          "CassandraIO.read()"
              + ".withConnectionConfiguration(configuration) called with null configuration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    public Read<T> withQuery(String query) {
      checkArgument(query != null, "CassandraIO.read().withQuery(query) called with null query");
      return builder().setQuery(query).build();
    }

    //TODO should be extracted from withQuery()
    public Read<T> withTable(String table) {
      checkArgument(table != null, "CassandraIO.read().withTable(table) called with null table");
      return builder().setTable(table).build();
    }

    public Read<T> withRowKey(String rowKey) {
      checkArgument(
          rowKey != null, "CassandraIO.read().withRowKey(rowKey) called with null " + "rowKey");
      return builder().setRowKey(rowKey).build();
    }

    public Read<T> withEntityName(Class<T> entityName) {
      checkArgument(
          entityName != null,
          "CassandraIO.read().withEntityName(entityName) called " + "with null entityName");
      return builder().setEntityName(entityName).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "CassandraIO.read().withCoder(coder) called with null coder");
      return builder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(createSource()));
    }

    @VisibleForTesting
    BoundedSource<T> createSource() {
      return new BoundedCassandraSource(this, null);
    }

    @Override
    public void validate(PBegin input) {
      checkState(
          getQuery() != null,
          "CassandraIO.read() requires a query to be set via " + "withQuery(query)");
      checkState(
          getRowKey() != null,
          "CassandraIO.read() requires a rowKey to be set via " + "withRowkey(rowKey)");
      checkState(
          getEntityName() != null,
          "CassandraIO.read() requires an entity name to be set "
              + "via withEntityName(entityName)");
      checkState(
          getTable() != null,
          "CassandraIO.read() requires a table to be set via " + "withTable(table)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      getConnectionConfiguration().populateDisplayData(builder);

      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      builder.addIfNotNull(DisplayData.item("entityName", getEntityName().getName()));
      builder.addIfNotNull(DisplayData.item("rowKey", getRowKey()));
      builder.addIfNotNull(DisplayData.item("table", getTable()));
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionConfiguration(
          ConnectionConfiguration connectionConfiguration);

      abstract Builder<T> setQuery(String query);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setRowKey(String rowKey);

      abstract Builder<T> setEntityName(Class<T> entityName);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Read<T> build();
    }
  }

  @VisibleForTesting
  static class BoundedCassandraSource<T> extends BoundedSource<T> {

    private final Read<T> spec;
    private String splitQuery;

    BoundedCassandraSource(Read<T> spec, String splitQuery) {
      this.spec = spec;
      this.splitQuery = splitQuery;
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return spec.getCoder();
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions pipelineOptions) {
      return new BoundedCassandraReader(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
      return DataSizeEstimates.getEstimatedSize(spec.getConnectionConfiguration(), spec.getTable());
    }

    @Override
    public List<BoundedSource<T>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions pipelineOptions) {
      long numSplits = 1;
      List<BoundedSource<T>> sourceList = new ArrayList<>();
      try {
        if (desiredBundleSizeBytes > 0) {
          numSplits = getEstimatedSizeBytes(pipelineOptions) / desiredBundleSizeBytes;
        }
      } catch (Exception e) {
        // fallback to 1
        numSplits = 1;
      }
      if (numSplits <= 0) {
        numSplits = 1;
      }

      LOG.debug("Number of splits is {}", numSplits);

      if (numSplits == 1) {
        sourceList.add(this);
        return sourceList;
      }

      DataSizeEstimates.Partitioner partitioner =
          new DataSizeEstimates.Partitioner(spec.getConnectionConfiguration().getCluster());

      BigInteger startRange = partitioner.minToken;
      BigInteger endRange = partitioner.maxToken;
      BigInteger startToken, endToken;

      endToken = startRange;
      BigInteger incrementValue =
          (endRange.subtract(startRange)).divide(new BigInteger(String.valueOf(numSplits)));
      String splitQuery;
      for (int splitCount = 1; splitCount <= numSplits; splitCount++) {
        startToken = endToken;
        endToken = startToken.add(incrementValue);
        if (splitCount == numSplits) {
          endToken = endRange;
        }
        splitQuery =
            QueryBuilder.select()
                .from(spec.getConnectionConfiguration().getKeyspace(), spec.getTable())
                .where(QueryBuilder.gte("token(" + spec.getRowKey() + ")", startToken))
                .and(QueryBuilder.lt("token(" + spec.getRowKey() + ")", endToken))
                .toString();
        sourceList.add(new BoundedCassandraSource(spec, splitQuery));
      }
      return sourceList;
    }
  }

  private static class BoundedCassandraReader<T> extends BoundedSource.BoundedReader<T> {

    private final BoundedCassandraSource<T> source;

    private Cluster cluster;
    private Session session;
    private ResultSet resultSet;
    private Iterator<T> iterator;
    private T current;

    private BoundedCassandraReader(BoundedCassandraSource<T> source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      LOG.debug("Starting Cassandra reader");
      Read spec = this.source.spec;
      cluster = spec.getConnectionConfiguration().getCluster();
      session = cluster.connect();
      //TODO mix spec.getQuery() and source.getquery (= split Query)
      resultSet = session.execute(spec.getQuery());
      final MappingManager mappingManager = new MappingManager(session);
      Mapper mapper = mappingManager.mapper(spec.getEntityName());
      iterator = mapper.map(resultSet).iterator();
      return advance();
    }

    @Override
    public boolean advance() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      } else {
        current = null;
        return false;
      }
    }

    @Override
    public void close() {
      LOG.debug("Closing Cassandra reader");
      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    }

    @Override
    public BoundedSource getCurrentSource() {
      return source;
    }

    @Override
    public T getCurrent() {
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() {
      return Instant.now();
    }
  }

  /**
   * A {@link PTransform} to write into Apache Cassandra. See {@link CassandraIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    abstract Builder<T> builder();

    public Write<T> withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(
          connectionConfiguration != null,
          "CassandraIO.write()"
              + ".withConnectionConfiguration(configuration) called with null configuration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input.apply(ParDo.of(new WriteFn<T>(this)));
      return PDone.in(input.getPipeline());
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionConfiguration(
          ConnectionConfiguration connectionConfiguration);

      abstract Write<T> build();
    }
  }

  private static class WriteFn<T> extends DoFn<T, Void> {

    private final Write<T> spec;

    private transient Cluster cluster;
    private transient Session session;
    private transient MappingManager mappingManager;

    public WriteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() throws Exception {
      LOG.debug("Starting Cassandra writer");
      cluster = spec.getConnectionConfiguration().getCluster();
      session = cluster.connect(spec.getConnectionConfiguration().getKeyspace());
      mappingManager = new MappingManager(session);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      T entity = context.element();
      Mapper<T> mapper = (Mapper<T>) mappingManager.mapper(entity.getClass());
      mapper.save(entity);
    }

    @Teardown
    public void teardown() throws Exception {
      LOG.debug("Closing Cassandra writer");
      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    }
  }
}
