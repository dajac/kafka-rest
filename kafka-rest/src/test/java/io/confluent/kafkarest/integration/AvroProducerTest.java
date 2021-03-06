/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.integration;

import static io.confluent.kafkarest.TestUtils.assertOKResponse;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafkarest.TestUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v1.AvroPartitionProduceRequest;
import io.confluent.kafkarest.entities.v1.AvroPartitionProduceRequest.AvroPartitionProduceRecord;
import io.confluent.kafkarest.entities.v1.AvroTopicProduceRequest;
import io.confluent.kafkarest.entities.v1.AvroTopicProduceRequest.AvroTopicProduceRecord;
import io.confluent.kafkarest.entities.v1.PartitionOffset;
import io.confluent.kafkarest.entities.v1.ProduceResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;

// This test is much lighter than the Binary one which exercises all variants. Since binary
// covers most code paths well, this just tries to exercise Avro-specific parts.
public class AvroProducerTest extends ClusterTestHarness {

  private static final String topicName = "topic1";

  protected Properties deserializerProps;

  // This test assumes that AvroConverterTest is good enough and testing one primitive type for
  // keys and one complex type for records is sufficient.
  private static final String keySchemaStr = "{\"name\":\"int\",\"type\": \"int\"}";
  private static final String valueSchemaStr = "{\"type\": \"record\", "
                                               + "\"name\":\"test\","
                                               + "\"fields\":[{"
                                               + "  \"name\":\"field\", "
                                               + "  \"type\": \"int\""
                                               + "}]}";
  private static final Schema valueSchema = new Schema.Parser().parse(valueSchemaStr);

  private final static JsonNode[] testKeys = {
      TestUtils.jsonTree("1"),
      TestUtils.jsonTree("2"),
      TestUtils.jsonTree("3"),
      TestUtils.jsonTree("4")
  };

  private final static JsonNode[] testValues = {
      TestUtils.jsonTree("{\"field\": 1}"),
      TestUtils.jsonTree("{\"field\": 2}"),
      TestUtils.jsonTree("{\"field\": 3}"),
      TestUtils.jsonTree("{\"field\": 4}"),
  };

  // Produce to topic inputs & results

  protected final List<AvroTopicProduceRecord> topicRecordsWithPartitionsAndKeys = Arrays.asList(
      new AvroTopicProduceRecord(testKeys[0], testValues[0], 0),
      new AvroTopicProduceRecord(testKeys[1], testValues[1], 1),
      new AvroTopicProduceRecord(testKeys[2], testValues[2], 1),
      new AvroTopicProduceRecord(testKeys[3], testValues[3], 2)
  );
  protected final List<PartitionOffset> partitionOffsetsWithPartitionsAndKeys = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(1, 0L, null, null),
      new PartitionOffset(1, 1L, null, null)
  );

  // Produce to partition inputs & results
  protected final List<AvroPartitionProduceRecord> partitionRecordsOnlyValues = Arrays.asList(
      new AvroPartitionProduceRecord(null, testValues[0]),
      new AvroPartitionProduceRecord(null, testValues[1]),
      new AvroPartitionProduceRecord(null, testValues[2]),
      new AvroPartitionProduceRecord(null, testValues[3])
  );
  protected final List<PartitionOffset> producePartitionOffsetOnlyValues = Arrays.asList(
      new PartitionOffset(0, 0L, null, null),
      new PartitionOffset(0, 1L, null, null),
      new PartitionOffset(0, 2L, null, null),
      new PartitionOffset(0, 3L, null, null)
  );

  public AvroProducerTest() {
    super(1, true);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final int numPartitions = 3;
    final int replicationFactor = 1;
    kafka.utils.TestUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor,
        JavaConversions.asScalaBuffer(this.servers),
        new Properties());

    deserializerProps = new Properties();
    deserializerProps.setProperty("schema.registry.url", schemaRegConnect);
  }

  protected <K, V> void testProduceToTopic(
      List<AvroTopicProduceRecord> records, List<PartitionOffset> offsetResponses
  ) {
    testProduceToTopic(records, offsetResponses, Collections.emptyMap());
  }

  protected <K, V> void testProduceToTopic(
      List<AvroTopicProduceRecord> records,
      List<PartitionOffset> offsetResponses,
      Map<String, String> queryParams
  ) {
    AvroTopicProduceRequest payload =
        AvroTopicProduceRequest.create(
            records,
            keySchemaStr,
            /* keySchemaId= */ null,
            valueSchemaStr,
            /* valueSchemaId= */ null);
    Response response = request("/topics/" + topicName, queryParams)
        .post(Entity.entity(payload, Versions.KAFKA_V1_JSON_AVRO));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse produceResponse = TestUtils
        .tryReadEntityOrLog(response, ProduceResponse.class);
    TestUtils.assertPartitionOffsetsEqual(offsetResponses, produceResponse.getOffsets());
    TestUtils.assertTopicContains(
        plaintextBrokerList,
        topicName,
        payload.toProduceRequest().getRecords(),
        null,
        KafkaAvroDeserializer.class.getName(),
        KafkaAvroDeserializer.class.getName(),
        deserializerProps,
        false);
    assertEquals(produceResponse.getKeySchemaId(), (Integer) 1);
    assertEquals(produceResponse.getValueSchemaId(), (Integer) 2);
  }

  @Test
  public void testProduceToTopicWithPartitionsAndKeys() {
    testProduceToTopic(topicRecordsWithPartitionsAndKeys, partitionOffsetsWithPartitionsAndKeys);
  }

  protected <K, V> void testProduceToPartition(
      List<AvroPartitionProduceRecord> records, List<PartitionOffset> offsetResponse
  ) {
    testProduceToPartition(records, offsetResponse, Collections.emptyMap());
  }

  protected <K, V> void testProduceToPartition(List<AvroPartitionProduceRecord> records,
      List<PartitionOffset> offsetResponse,
      Map<String, String> queryParams) {
    AvroPartitionProduceRequest payload =
        AvroPartitionProduceRequest.create(
            records,
            /* keySchema= */ null,
            /* keySchemaId= */ null,
            /* valueSchema= */ valueSchemaStr,
            /* valueSchemaId= */ null);
    Response response = request("/topics/" + topicName + "/partitions/0", queryParams)
        .post(Entity.entity(payload, Versions.KAFKA_V1_JSON_AVRO));
    assertOKResponse(response, Versions.KAFKA_MOST_SPECIFIC_DEFAULT);
    final ProduceResponse poffsetResponse
        = TestUtils.tryReadEntityOrLog(response, ProduceResponse.class);
    assertEquals(offsetResponse, poffsetResponse.getOffsets());
    TestUtils.assertTopicContains(
        plaintextBrokerList,
        topicName,
        payload.toProduceRequest().getRecords(),
        0,
        KafkaAvroDeserializer.class.getName(),
        KafkaAvroDeserializer.class.getName(),
        deserializerProps,
        false);
    assertEquals((Integer) 1, poffsetResponse.getValueSchemaId());
  }

  @Test
  public void testProduceToPartitionOnlyValues() {
    testProduceToPartition(partitionRecordsOnlyValues, producePartitionOffsetOnlyValues);
  }
}
