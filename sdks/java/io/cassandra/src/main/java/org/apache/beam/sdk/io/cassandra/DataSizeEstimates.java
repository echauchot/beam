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
import com.datastax.driver.core.exceptions.InvalidQueryException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This utility class estimates the total size of a table using Cassandra system table.
 * system.size_estimates from one node of the cluster. As for now, such a feature is not available
 * as a library in Cassandra. So it is coded manually, similarly to cassandra spark connector.
 */
public class DataSizeEstimates {

  private static final Logger logger = Logger.getLogger(CassandraIO.class.getName());

  /**
   * Estimates the size of a table.
   *
   * @param connectionConfiguration The configuration of the connection to the cluster
   * @param table The table to estimate
   * @return The size of the table in bytes
   */
  public static long getEstimatedSize(
      CassandraIO.ConnectionConfiguration connectionConfiguration, String table) {
    try (Cluster cluster = connectionConfiguration.getCluster()) {
      List<TokenRange> tokenRanges = getTokenRanges(cluster, connectionConfiguration, table);
      long size = 0L;
      for (TokenRange tokenRange : tokenRanges) {
        size += tokenRange.meanPartitionSize * tokenRange.partitionCount;
      }
      return Math.round(size / getRingFraction(cluster, tokenRanges));
    }
  }

  /**
   * values that we get from system.size_estimates are for one node. We need to extrapolate to the
   * whole cluster. This method estimates the percentage, the node represents in the cluster
   *
   * @param cluster The cassandra {@link Cluster} object
   * @param tokenRanges The list of {@link TokenRange}
   * @return The percentage the node represent in the whole cluster
   */
  //
  private static float getRingFraction(Cluster cluster, List<TokenRange> tokenRanges) {
    Partitioner partitioner = new Partitioner(cluster);
    BigInteger addressedTokens = new BigInteger("0");
    for (TokenRange tokenRange : tokenRanges) {
      addressedTokens =
          addressedTokens.add(distance(cluster, tokenRange.rangeStart, tokenRange.rangeEnd));
    }
    // it is < 1 because it is a percentage
    return Float.valueOf(addressedTokens.divide(partitioner.getTotalTokens()).toString());
  }

  /**
   * Gets the list of token ranges that a table occupies on a give Cassandra node.
   *
   * @param connectionConfiguration The {@link CassandraIO.ConnectionConfiguration} for cluster
   *     connection
   * @param table The table whose token ranges are fetched
   * @return The list of {@link TokenRange}
   */
  //compatible Cassandra > 2.1.5
  private static List<TokenRange> getTokenRanges(
      Cluster cluster, CassandraIO.ConnectionConfiguration connectionConfiguration, String table) {
    try (Session session = cluster.newSession()) {
      ResultSet resultSet =
          session.execute(
              "SELECT range_start, range_end, partitions_count, mean_partition_size FROM "
                  + "system.size_estimates WHERE keyspace_name = ? AND table_name = ?",
              connectionConfiguration.getKeyspace(),
              table);

      ArrayList<TokenRange> tokenRanges = new ArrayList<>();
      for (Row row : resultSet) {
        TokenRange tokenRange =
            new TokenRange(
                row.getLong("partitions_count"),
                row.getLong("mean_partition_size"),
                row.getString("range_start"),
                row.getString("range_end"));
        tokenRanges.add(tokenRange);
      }
      // The table may not contain the estimates yet
      // or have partitions_count and mean_partition_size fields = 0
      // if the data was just inserted and the amount of data in the table was small.
      // This is very common situation during tests,
      // when we insert a few rows and immediately query them.
      // However, for tiny data sets the lack of size estimates is not a problem at all,
      // because we don't want to split tiny data anyways.
      // Therefore, we're not issuing a warning if the result set was empty
      // or mean_partition_size and partitions_count = 0.
      return tokenRanges;
    } catch (InvalidQueryException e) {
      logger.log(
          Level.INFO,
          "Failed to fetch size estimates for $keyspaceName.$tableName "
              + "from system.size_estimates table. "
              + "The number of created bundles may be inaccurate. "
              + "Please make sure you use Cassandra 2.1.5 or newer.");
      ArrayList<TokenRange> tokenRanges = new ArrayList<>();
      tokenRanges.add(new TokenRange(0L, 0L, "0", "0"));
      return tokenRanges;
    }
  }

  /**
   * Measure distance between two tokens.
   *
   * @param cluster The cassandra {@link Cluster} object
   * @param token1 The measure is symmetrical so token1 and token2 can be exchanged
   * @param token2 The measure is symmetrical so token1 and token2 can be exchanged
   * @return Number of tokens that separate token1 and token2
   */
  private static BigInteger distance(Cluster cluster, BigInteger token1, BigInteger token2) {
    Partitioner partitioner = new Partitioner(cluster);
    //token2 > token1
    if (token2.compareTo(token1) == 1) {
      return token2.subtract(token1);
    } else {
      return token2.subtract(token1).add(partitioner.getTotalTokens());
    }
  }

  static class Partitioner {
    BigInteger minToken;
    BigInteger maxToken;

    Partitioner(Cluster cluster) {
      String partitionerName = cluster.getMetadata().getPartitioner();
      switch (partitionerName) {
        case "org.apache.cassandra.dht.Murmur3Partitioner":
          minToken = new BigInteger(String.valueOf(Long.MIN_VALUE));
          maxToken = new BigInteger(String.valueOf(Long.MAX_VALUE));
          break;
        case "org.apache.cassandra.dht.RandomPartitioner":
          minToken = new BigInteger("-1");
          maxToken = new BigInteger(String.valueOf((long) Math.pow(2, 127)));
          break;
        default:
          minToken = new BigInteger(String.valueOf(Long.MIN_VALUE));
          maxToken = new BigInteger(String.valueOf(Long.MAX_VALUE));
          break;
      }
    }

    BigInteger getTotalTokens() {
      return maxToken.subtract(minToken);
    }
  }

  private static class TokenRange {
    private long partitionCount;
    private long meanPartitionSize;
    private BigInteger rangeStart;
    private BigInteger rangeEnd;

    private TokenRange(
        long partitionCount, long meanPartitionSize, String rangeStart, String rangeEnd) {
      this.partitionCount = partitionCount;
      this.meanPartitionSize = meanPartitionSize;
      this.rangeStart = new BigInteger(rangeStart);
      this.rangeEnd = new BigInteger(rangeEnd);
    }
  }
}
