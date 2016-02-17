/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.plumber;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Granularity;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.client.cache.MapCache;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Druids;
import io.druid.query.MetricValueExtractor;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.QueryableIndex;
import io.druid.segment.TestHelper;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireDepartmentTest;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 */
public class RealtimeExactCountTest
{
  private RealtimePlumber plumber;
  private RealtimePlumberSchool realtimePlumberSchool;
  private DataSegmentAnnouncer announcer;
  private SegmentPublisher segmentPublisher;
  private DataSegmentPusher dataSegmentPusher;
  private SegmentHandoffNotifier handoffNotifier;
  private SegmentHandoffNotifierFactory handoffNotifierFactory;
  private ServiceEmitter emitter;
  private RealtimeTuningConfig tuningConfig;
  private DataSchema schema;
  private FireDepartmentMetrics metrics;

  @Before
  public void setUp() throws Exception
  {
    final File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    ObjectMapper jsonMapper = new DefaultObjectMapper();

    schema = new DataSchema(
        "test",
        jsonMapper.convertValue(
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec("timestamp", "auto", null),
                    new DimensionsSpec(ImmutableList.of("foo"), null, null)
                )
            ),
            Map.class
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(Granularity.DAY, QueryGranularity.DAY, null),
        jsonMapper
    );

    announcer = EasyMock.createMock(DataSegmentAnnouncer.class);
    announcer.announceSegment(EasyMock.<DataSegment>anyObject());
    EasyMock.expectLastCall().anyTimes();

    segmentPublisher = EasyMock.createNiceMock(SegmentPublisher.class);
    dataSegmentPusher = EasyMock.createNiceMock(DataSegmentPusher.class);
    handoffNotifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    handoffNotifier = EasyMock.createNiceMock(SegmentHandoffNotifier.class);
    EasyMock.expect(handoffNotifierFactory.createSegmentHandoffNotifier(EasyMock.anyString()))
            .andReturn(handoffNotifier)
            .anyTimes();
    EasyMock.expect(
        handoffNotifier.registerSegmentHandoffCallback(
            EasyMock.<SegmentDescriptor>anyObject(),
            EasyMock.<Executor>anyObject(),
            EasyMock.<Runnable>anyObject()
        )
    ).andReturn(true).anyTimes();

    emitter = EasyMock.createMock(ServiceEmitter.class);
    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall().anyTimes();

    EasyMock.replay(announcer, segmentPublisher, dataSegmentPusher, handoffNotifierFactory, handoffNotifier, emitter);

    tuningConfig = new RealtimeTuningConfig(
        null,
        new Period("PT10M"),
        new Period("PT10M"),
        null,
        new IntervalStartVersioningPolicy(),
        new NoopRejectionPolicyFactory(),
        null,
        null,
        null,
        false,
        0, 0
    );

    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>of(
            TimeseriesQuery.class,
            factory
        )
    );

    realtimePlumberSchool = new RealtimePlumberSchool(
        emitter,
        conglomerate,
        dataSegmentPusher,
        announcer,
        segmentPublisher,
        handoffNotifierFactory,
        MoreExecutors.sameThreadExecutor(),
        TestHelper.getTestIndexMerger(),
        TestHelper.getTestIndexMergerV9(),
        TestHelper.getTestIndexIO(),
        MapCache.create(0),
        FireDepartmentTest.NO_CACHE_CONFIG,
        TestHelper.getObjectMapper()
    );

    metrics = new FireDepartmentMetrics();
    plumber = (RealtimePlumber) realtimePlumberSchool.findPlumber(schema, tuningConfig, metrics);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(announcer, segmentPublisher, dataSegmentPusher, handoffNotifierFactory, handoffNotifier, emitter);
    FileUtils.deleteDirectory(
        new File(
            tuningConfig.getBasePersistDirectory(),
            schema.getDataSource()
        )
    );
  }

  @Test(timeout = 60000)
  public void testExactCount() throws Exception
  {
    final Object commitMetadata = "dummyCommitMetadata";
    final CountDownLatch doneSignal = new CountDownLatch(1);
    final Committer committer = new Committer()
    {
      @Override
      public Object getMetadata()
      {
        return commitMetadata;
      }

      @Override
      public void run()
      {
        doneSignal.countDown();
      }
    };
    plumber.startJob();
    plumber.add(getTestInputRowFull("2016-01-01T00:01:00Z", ImmutableList.of("foo"), ImmutableList.of("bar")), Suppliers.ofInstance(committer));
    plumber.add(getTestInputRowFull("2016-01-01T00:06:00Z", ImmutableList.of("foo"), ImmutableList.of("bar")), Suppliers.ofInstance(committer));
    plumber.persist(committer);
    plumber.add(getTestInputRowFull("2016-01-01T01:12:00Z", ImmutableList.of("foo"), ImmutableList.of("bar")), Suppliers.ofInstance(committer));
    plumber.persist(committer);

    doneSignal.await();

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
        .dataSource("test")
        .intervals("2016-01-01/2016-01-02")
        .aggregators(
            Arrays.<AggregatorFactory>asList(
                new CountAggregatorFactory("count"),
                new LongSumAggregatorFactory("sum", "rows")
            )
        )
        .granularity(QueryGranularity.ALL)
        .build();

    QueryRunner runner = plumber.getQueryRunner(query);
    Sequence seq = runner.run(query, Collections.EMPTY_MAP);
    ArrayList list = Sequences.toList(seq, Lists.newArrayList());
    Assert.assertEquals(1, list.size());
    TimeseriesResultValue result = (TimeseriesResultValue) ((Result) list.get(0)).getValue();
    Assert.assertEquals(3L, result.getMetric("sum"));
    Assert.assertEquals(1L, result.getMetric("count"));
  }

  private InputRow getTestInputRowFull(final String timeStr, final List<String> dims, final List<String> dimVals)
  {
    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return dims;
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return new DateTime(timeStr).getMillis();
      }

      @Override
      public DateTime getTimestamp()
      {
        return new DateTime(timeStr);
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        return dimVals;
      }

      @Override
      public float getFloatMetric(String metric)
      {
        return 0;
      }

      @Override
      public long getLongMetric(String metric)
      {
        return 0L;
      }

      @Override
      public Object getRaw(String dimension)
      {
        return null;
      }

      @Override
      public int compareTo(Row o)
      {
        return 0;
      }
    };
  }
}
