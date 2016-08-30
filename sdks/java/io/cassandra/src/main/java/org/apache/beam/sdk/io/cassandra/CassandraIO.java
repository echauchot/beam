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
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
import org.apache.beam.sdk.values.PInput;

import org.joda.time.Instant;

/**
 * An IO to read and write on Apache Cassandra.
 *
 * <h3>Reading from Apache Cassandra</h3>
 *
 * <p>CassandraIO provides a source to read and returns a bounded collection of entities as {@code
 * PCollection<Entity>}.
 * An entity is built by Cassandra mapper based on a POJO containing annotations.</p>
 *
 * <p>To configure a Cassandra source, you have to provide the hosts, port, and keyspace of the
 * Cassandra instance. The following example illustrate various options for configuring the IO:</p>
 *
 * <pre>{@code
 *
 * pipeline.apply(CassandraIO.read()
 *     .withHosts(new String[]{ "host1", "host2" })
 *     .withPort(9042)
 *     .withKeyspace("beam")
 *     .withQuery("select * from Person")
 *     .withEntityName(Person.class)
 *     // above options are the minimum set, returns PCollection<Person>
 *
 * }</pre>
 *
 * <h3>Writing to Apache Cassandra</h3>
 *
 * <p>CassandraIO write provides a sink to write to Apache Cassandra. It expects a {@code
 * PCollection}
 * of entities that will be mapped and written in Cassandra.
 * </p>
 *
 * <p>To configure the sink, you have to specify hosts, port, keyspace, and entity class:</p>
 *
 * <pre>{@code
 *
 * pipeline
 *     .apply(...) // returns PCollection<Person>
 *     .apply(CassandraIO.write()
 *        .withHosts(new String[]{ "host1", "host2" })
 *        .withPort(9042)
 *        .withKeyspace("beam")
 *        .withEntityName(Person.class);
 *
 * }</pre>
 */
public class CassandraIO {

  public static Read read() {
    return new Read(new String[]{"localhost"}, null, 9042, null, null, null, null);
  }

  public static Write write() {
    return new Write(new String[]{"localhost"}, null, 9042);
  }

  private CassandraIO() {}

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link CassandraIO} for more
   * information on usage and configuration.
   */
  public static class Read extends PTransform<PBegin, PCollection> {

    public Read withHosts(String[] hosts) {
      return new Read(hosts, keyspace, port, query, table, rowKey, entityName);
    }

    public Read withKeyspace(String keyspace) {
      return new Read(hosts, keyspace, port, query, table, rowKey, entityName);
    }

    public Read withPort(int port) {
      return new Read(hosts, keyspace, port, query, table, rowKey, entityName);
    }

    public Read withQuery(String query) {
      return new Read(hosts, keyspace, port, query, table, rowKey, entityName);
    }

    public Read withTable(String table) {
      return new Read(hosts, keyspace, port, query, table, rowKey, entityName);
    }

    public Read withRowKey(String rowKey) {
      return new Read(hosts, keyspace, port, query, table, rowKey, entityName);
    }

    public Read withEntityName(Class entityName) {
      return new Read(hosts, keyspace, port, query, table, rowKey, entityName);
    }

    protected String[] hosts;
    protected String keyspace;
    protected int port;
    protected String table;
    protected String query;
    protected String rowKey;
    protected Class entityName;

    private Read(
      String[] hosts,
      String keyspace,
      int port,
      String query,
      String table,
      String rowKey,
      Class entityName) {
      super("Cassandra.ReadIO");

      this.hosts = hosts;
      this.port = port;
      this.query = query;
      this.entityName = entityName;
      this.keyspace = keyspace;
      this.table = table;
      this.rowKey = rowKey;
    }

    @Override
    public PCollection apply(PBegin input) {
      org.apache.beam.sdk.io.Read.Bounded bounded =
          org.apache.beam.sdk.io.Read.from(createSource());

      PTransform<PInput, PCollection> transform = bounded;

      return input.getPipeline().apply(transform);
    }

    @VisibleForTesting
    BoundedSource createSource() {
      return new BoundedCassandraSource(hosts, keyspace, port, table, query, rowKey,
          entityName);
    }

    @Override
    public void validate(PBegin input) {
      Preconditions.checkNotNull(hosts, "hosts");
      Preconditions.checkNotNull(port, "port");
      Preconditions.checkNotNull(query, "query");
      Preconditions.checkNotNull(entityName, "entityName");
      Preconditions.checkNotNull(keyspace, "keyspace");
      Preconditions.checkNotNull(table, "table");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.addIfNotNull(DisplayData.item("hosts", hosts.toString()));
      builder.addIfNotNull(DisplayData.item("port", port));
      builder.addIfNotNull(DisplayData.item("query", query));
      builder.addIfNotNull(DisplayData.item("entityName", entityName));
      builder.addIfNotNull(DisplayData.item("keyspace", keyspace));
      builder.addIfNotNull(DisplayData.item("table", table));

    }

  }

  private static class BoundedCassandraSource extends BoundedSource {

    private String[] hosts;
    private String keyspace;
    private int port;
    private String table;
    private String query;
    private String rowKey;
    private Class entityName;

    private String rowCountQuery = null;

    BoundedCassandraSource(String[] hosts, String keyspace, int port, String table,
                           String query,
                           String rowKey, Class entityName) {
      this.hosts = hosts;
      this.keyspace = keyspace;
      this.port = port;
      this.table = table;
      this.query = query;
      this.rowKey = rowKey;
      this.entityName = entityName;
    }

    @Override
    public Coder getDefaultOutputCoder() {
      return SerializableCoder.of(entityName);
    }

    @Override
    public void validate() {
      Preconditions.checkNotNull(hosts, "hosts");
      Preconditions.checkNotNull(port, "port");
      Preconditions.checkNotNull(keyspace, "keyspace");
      Preconditions.checkNotNull(table, "table");
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
      Cluster cluster = Cluster.builder().addContactPoints(hosts).withPort(port).build();
      Session session = cluster.newSession();

      ResultSet resultSet = session.execute("SELECT partitions_count, mean_partition_size FROM "
          + "system.size_estimates WHERE keyspace_name = ? AND table_name "
          + "= ?", keyspace, table);

      Row row = resultSet.one();

      long partitionsCount = row.getLong("partitions_count");
      long meanPartitionSize = row.getLong("mean_partition_size");

      return partitionsCount * meanPartitionSize;
    }

    @Override
    public List<BoundedSource> splitIntoBundles(long desiredBundleSizeBytes,
                                                PipelineOptions pipelineOptions) {
      int exponent = 63;
      long numSplits = 10;
      long startToken, endToken = 0L;
      List<BoundedSource> sourceList = new ArrayList<>();
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
        splitQuery = QueryBuilder.select().from(keyspace, table).where(QueryBuilder.gte("token("
        + rowKey + ")", startToken)).and(QueryBuilder.lt("token(" + rowKey + ")", endToken))
            .toString();
        query = splitQuery;
        sourceList.add(new BoundedCassandraSource(hosts, keyspace, port, table, query,
            rowKey, entityName));
      }
      return sourceList;
    }

  }

  private static class BoundedCassandraReader extends BoundedSource.BoundedReader {

    private final BoundedCassandraSource source;

    private Cluster cluster;
    private Session session;
    private ResultSet resultSet;
    private Iterator iterator;
    private Object current;

    public BoundedCassandraReader(BoundedCassandraSource source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      cluster = Cluster.builder().addContactPoints(source.hosts).withPort(source.port).build();
      session = cluster.connect();
      resultSet = session.execute(source.query);
      final MappingManager mappingManager = new MappingManager(session);
      Mapper mapper = (Mapper) mappingManager.mapper(source.entityName);
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
    public Object getCurrent() {
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
  public static class Write<T> extends PTransform<PCollection<T>, PDone> {

    private String[] hosts;
    private String keyspace;
    private int port;

    public Write withHosts(String[] hosts) {
      return new Write(hosts, keyspace, port);
    }

    public Write withKeyspace(String keyspace) {
      return new Write(hosts, keyspace, port);
    }

    public Write withPort(int port) {
      return new Write(hosts, keyspace, port);
    }

    private Write(String[] hosts, String keyspace, int port) {
      this.hosts = hosts;
      this.keyspace = keyspace;
      this.port = port;
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
          CassandraWriteOperation<T> operation = c.element();
          operation.finalize();
        }

      }).withSideInputs(voidView));

      return PDone.in(pipeline);
    }

  }

  private static class CassandraWriteOperation<T> implements Serializable {

    private final String[] hosts;
    private final String keyspace;
    private final int port;

    private transient Cluster cluster;
    private transient Session session;
    private transient MappingManager mappingManager;

    private synchronized Cluster getCluster() {
      if (cluster == null) {
        cluster = Cluster.builder().addContactPoints(hosts).withPort(port).withoutMetrics()
            .withoutJMXReporting().build();
      }
      return cluster;
    }

    private synchronized Session getSession() {
      if (session == null) {
        Cluster cluster = getCluster();
        session = cluster.connect(keyspace);
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

    public CassandraWriteOperation(Write<T> write) {
      hosts = write.hosts;
      port = write.port;
      keyspace = write.keyspace;
    }

    public CassandraWriter<T> createWriter() {
      return new CassandraWriter<T>(this, getMappingManager());
    }

    public void finalize() {
      getSession().close();
      getCluster().close();
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
