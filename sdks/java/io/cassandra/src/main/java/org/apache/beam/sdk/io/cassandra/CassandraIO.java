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
import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;

import org.joda.time.Instant;

/**
 * An IO to read and write on Apache Cassandra.
 *
 * <h3>Reading from Apache Cassandra</h3>
 *
 * <p>CassandraIO provides a source to read and returns a bounded collection of entities as {@code
 * PCollection<Entity>}.
 * An entity is built by Cassandra mapper based on a POJO containing annotations.
 *
 * <p>To configure a Cassandra source, you have to provide the hosts, port, and keyspace of the
 * Cassandra instance, wrapped as a {@link ConnectionConfiguration}.
 * The following example illustrate various options for configuring the IO:
 *
 * <pre>{@code
 *
 * pipeline.apply(CassandraIO.<Person>read()
 *     .withConnectionConfiguration(CassandraIO.ConnectionConfiguration.create(
 *        Arrays.asList("host1", "host1"),
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
 * <p>CassandraIO write provides a sink to write to Apache Cassandra. It expects a
 * {@code PCollection} of entities that will be mapped and written in Cassandra.
 *
 * <p>To configure the write, you have to specify hosts, port, keyspace wrapped as a
 * {@link ConnectionConfiguration} and the entity:
 *
 * <pre>{@code
 *
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

  public static <T> Read<T> read() {
    return new AutoValue_CassandraIO_Read.Builder<T>().build();
  }

  public static <T> Write<T> write() {
    return new AutoValue_CassandraIO_Write.Builder<T>().build();
  }

  private CassandraIO() {}

  /**
   * POJO describing a connection to Apache Cassandra database.
   */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {
    abstract List<String> getHosts();
    abstract String getKeyspace();
    abstract int getPort();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHosts(List<String> hosts);
      abstract Builder setKeyspace(String keyspace);
      abstract Builder setPort(int port);
      abstract ConnectionConfiguration build();
    }

    public static ConnectionConfiguration create(List<String> hosts, String keyspace, int port) {
      checkNotNull(hosts, "hosts");
      checkNotNull(keyspace, "keyspace");
      return new AutoValue_CassandraIO_ConnectionConfiguration.Builder()
          .setHosts(hosts)
          .setKeyspace(keyspace)
          .setPort(port)
          .build();
    }

    public void validate() {
      checkNotNull(getHosts(), "hosts");
      checkArgument(getHosts().size() >= 1, "hosts should contain at least one host");
      checkNotNull(getKeyspace(), "keyspace");
    }

    public void populateDisplayData(DisplayData.Builder builder) {
      builder.addIfNotNull(DisplayData.item("hosts", getHosts().toString()));
      builder.addIfNotNull(DisplayData.item("keyspace", getKeyspace()));
      builder.add(DisplayData.item("port", getPort()));
    }

    Cluster getCluster() {
      return Cluster.builder().addContactPoints(getHosts().toArray(new String[0])).withPort
          (getPort())
          .build();
    }
  }

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link CassandraIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    @Nullable abstract ConnectionConfiguration getConnectionConfiguration();
    @Nullable abstract String getQuery();
    @Nullable abstract String getTable();
    @Nullable abstract String getRowKey();
    @Nullable abstract Class<T> getEntityName();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionConfiguration(ConnectionConfiguration
                                                         connectionConfiguration);
      abstract Builder<T> setQuery(String query);
      abstract Builder<T> setTable(String table);
      abstract Builder<T> setRowKey(String rowKey);
      abstract Builder<T> setEntityName(Class<T> entityName);
      abstract Read<T> build();
    }

    public Read<T> withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkNotNull(connectionConfiguration, "ConnectionConfiguration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    public Read<T> withQuery(String query) {
      checkNotNull(query, "query");
      return builder().setQuery(query).build();
    }

    public Read<T> withTable(String table) {
      checkNotNull(table, "table");
      return builder().setTable(table).build();
    }

    public Read<T> withRowKey(String rowKey) {
      checkNotNull(rowKey, "RowKey");
      return builder().setRowKey(rowKey).build();
    }

    public Read<T> withEntityName(Class<T> entityName) {
      return builder().setEntityName(entityName).build();
    }

    @Override
    public PCollection<T> apply(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(createSource()));
    }

    @VisibleForTesting
    BoundedSource<T> createSource() {
      return new BoundedCassandraSource(this, null);
    }

    @Override
    public void validate(PBegin input) {
      getConnectionConfiguration().validate();
      checkNotNull(getQuery(), "query");
      checkNotNull(getEntityName(), "entityName");
      checkNotNull(getTable(), "table");
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

  }

  private static class BoundedCassandraSource<T> extends BoundedSource<T> {

    private Read spec;
    private String splitQuery;

    BoundedCassandraSource(Read spec, String splitQuery) {
      this.spec = spec;
      this.splitQuery = splitQuery;
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return SerializableCoder.of(spec.getEntityName());
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions pipelineOptions) {
      return false;
    }

    @Override
    public BoundedReader createReader(PipelineOptions pipelineOptions) {
      return new BoundedCassandraReader(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
      try (Cluster cluster = spec.getConnectionConfiguration().getCluster()) {
        try (Session session = cluster.newSession()) {

          ResultSet resultSet = session.execute("SELECT partitions_count, mean_partition_size FROM "
              + "system.size_estimates WHERE keyspace_name = ? AND table_name "
              + "= ?", spec.getConnectionConfiguration().getKeyspace(), spec.getTable());

          Row row = resultSet.one();

          long partitionsCount = row.getLong("partitions_count");
          long meanPartitionSize = row.getLong("mean_partition_size");


          return partitionsCount * meanPartitionSize;
        }
      }
    }

    @Override
    public List<BoundedSource<T>> splitIntoBundles(long desiredBundleSizeBytes,
                                                PipelineOptions pipelineOptions) {
      int exponent = 63;
      long numSplits = 10;
      long startToken, endToken = 0L;
      List<BoundedSource<T>> sourceList = new ArrayList<>();
      try {
        if (desiredBundleSizeBytes > 0) {
          numSplits = getEstimatedSizeBytes(pipelineOptions) / desiredBundleSizeBytes;
        }
      } catch (Exception e) {
        // fallback to 10
        numSplits = 10;
      }

      if (numSplits <= 0) {
        numSplits = 1;
      }

      if (numSplits == 1) {
        sourceList.add(this);
        return sourceList;
      }

      BigInteger startRange = new BigInteger(String.valueOf(-(long) Math.pow(2, exponent)));
      BigInteger endRange = new BigInteger(String.valueOf((long) Math.pow(2, exponent)));
      endToken = startRange.longValue();
      BigInteger incrementValue = (endRange.subtract(startRange)).divide(
          new BigInteger(String.valueOf(numSplits)));
      String splitQuery = null;
      for (int splitCount = 1; splitCount <= numSplits; splitCount++) {
        startToken = endToken;
        endToken = startToken + incrementValue.longValue();
        if (splitCount == numSplits) {
          endToken = (long) Math.pow(2, exponent);
        }
        splitQuery =
            QueryBuilder
                .select()
                .from(spec.getConnectionConfiguration().getKeyspace(), spec.getTable())
                .where(
                    QueryBuilder.gte("token(" + spec.getRowKey() + ")", startToken))
                .and(QueryBuilder.lt("token(" + spec.getRowKey() + ")", endToken))
            .toString();
        sourceList.add(new BoundedCassandraSource(spec, splitQuery));
      }
      return sourceList;
    }

  }

  private static class BoundedCassandraReader<T> extends BoundedSource.BoundedReader<T> {

    private final BoundedCassandraSource source;

    private Cluster cluster;
    private Session session;
    private ResultSet resultSet;
    private Iterator<T> iterator;
    private T current;

    public BoundedCassandraReader(BoundedCassandraSource source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      Read spec = source.spec;
      cluster = spec.getConnectionConfiguration().getCluster();
      session = cluster.connect();
      resultSet = session.execute(spec.getQuery());
      final MappingManager mappingManager = new MappingManager(session);
      Mapper mapper = (Mapper) mappingManager.mapper(spec.getEntityName());
      iterator = mapper.map(resultSet).iterator();
      return advance();
    }

    @Override
    public boolean advance() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void close() {
      session.close();
      cluster.close();
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

    @Nullable abstract ConnectionConfiguration getConnectionConfiguration();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionConfiguration(ConnectionConfiguration
                                                         connectionConfiguration);
      abstract Write<T> build();
    }

    public Write<T> withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkNotNull(connectionConfiguration, "connectionConfiguration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    public PDone apply(PCollection<T> input) {
      Pipeline pipeline = input.getPipeline();
      CassandraWriteOperation<T> operation = new CassandraWriteOperation<T>(this);

      Coder<CassandraWriteOperation<T>> coder =
          (Coder<CassandraWriteOperation<T>>) SerializableCoder.of(operation.getClass());

      PCollection<CassandraWriteOperation<T>> collection =
          pipeline.apply(Create.<CassandraWriteOperation<T>> of(operation).withCoder(coder));

      final PCollectionView<CassandraWriteOperation<T>> view =
          collection.apply(View.<CassandraWriteOperation<T>>asSingleton());

      PCollection<Void> results = input.apply(ParDo.of(new DoFn<T, Void>() {

        private CassandraWriter<T> writer = null;

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
          if (writer == null) {
            CassandraWriteOperation<T> operation = c.sideInput(view);
            writer = operation.createWriter();
          }

          try {
            writer.write(c.element());
          } catch (Exception e) {
            try {
              writer.flush();
            } catch (Exception ec) {
              // nothing to do
            }
            throw e;
          }
        }

        @FinishBundle
        public void finishBundle(Context c) throws Exception {
          if (writer != null) {
            writer.flush();
          }
        }

      }).withSideInputs(view));

      PCollectionView<Iterable<Void>> voidView = results.apply(View.<Void> asIterable());

      collection.apply("Cassandra Write", ParDo.of(new DoFn<CassandraWriteOperation<T>, Void>() {

        @ProcessElement
        public void processElement(ProcessContext c) {
          // TODO could be implemented per bundle directly in the DoFn
          CassandraWriteOperation<T> operation = c.element();
          operation.finalize();
        }

      }).withSideInputs(voidView));

      return PDone.in(pipeline);
    }

  }

  private static class CassandraWriteOperation<T> implements Serializable {

    private Write spec;

    private transient Cluster cluster;
    private transient Session session;
    private transient MappingManager mappingManager;

    private synchronized Session getSession() {
      if (session == null) {
        cluster = spec.getConnectionConfiguration().getCluster();
        session = cluster.connect(spec.getConnectionConfiguration().getKeyspace());
      }
      return session;
    }

    private synchronized MappingManager getMappingManager() {
      if (mappingManager == null) {
        Session session = getSession();
        mappingManager = new MappingManager(session);
      }
      return mappingManager;
    }

    public CassandraWriteOperation(Write<T> spec) {
      this.spec = spec;
    }

    public CassandraWriter<T> createWriter() {
      return new CassandraWriter<T>(this, getMappingManager());
    }

    protected void finalize() {
      session.close();
      cluster.close();
    }

  }

  private static class CassandraWriter<T> {

    private static int batchSize = 20000;
    private final CassandraWriteOperation operation;
    private final MappingManager mappingManager;
    private final List<ListenableFuture<Void>> results = new ArrayList<>();
    private Mapper<T> mapper;

    public CassandraWriter(CassandraWriteOperation operation, MappingManager mappingManager) {
      this.operation = operation;
      this.mappingManager = mappingManager;
    }

    public void flush() {
      for (ListenableFuture<Void> result : results) {
        try {
          Uninterruptibles.getUninterruptibly(result);
        } catch (ExecutionException e) {
          if (e.getCause() instanceof DriverException) {
            throw ((DriverException) e.getCause()).copy();
          } else {
            throw new DriverInternalError("Unexpected exception throws", e.getCause());
          }
        }
      }
      results.clear();
    }

    public CassandraWriteOperation getOperation() {
      return operation;
    }

    public void write(T entity) {
      if (mapper == null) {
        mapper = (Mapper<T>) mappingManager.mapper(entity.getClass());
      }
      if (results.size() >= batchSize) {
        flush();
      }
      results.add(mapper.saveAsync((T) entity));
    }

  }

}
