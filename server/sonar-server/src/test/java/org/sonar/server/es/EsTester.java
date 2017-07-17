/*
 * SonarQube
 * Copyright (C) 2009-2017 SonarSource SA
 * mailto:info AT sonarsource DOT com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.sonar.server.es;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang.reflect.ConstructorUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.elasticsearch.transport.MockTcpTransportPlugin;
import org.junit.rules.ExternalResource;
import org.sonar.api.config.internal.MapSettings;
import org.sonar.core.config.ConfigurationProvider;
import org.sonar.core.platform.ComponentContainer;
import org.sonar.elasticsearch.test.EsTestCluster;
import org.sonar.server.es.metadata.MetadataIndex;
import org.sonar.server.es.metadata.MetadataIndexDefinition;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.sonar.server.es.DefaultIndexSettings.REFRESH_IMMEDIATE;

public class EsTester extends ExternalResource {

  private final List<IndexDefinition> indexDefinitions;
  private final EsTestCluster cluster;
  private final EsClient client;

  private ComponentContainer container;

  public EsTester(IndexDefinition... defs) {
    this.indexDefinitions = asList(defs);

    Path tempDirectory;
    try {
      tempDirectory = Files.createTempDirectory("es-unit-test");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try {
      cluster = RandomizedContext.current().runWithPrivateRandomness(new com.carrotsearch.randomizedtesting.Randomness(0L), new Callable<EsTestCluster>() {
        @Override
        public EsTestCluster call() throws Exception {
          return new EsTestCluster(0L, tempDirectory, false, 1, 1, "test cluster",
            getNodeConfigSource(), 0, false, "node-prefix", asList(            AssertingLocalTransport.TestPlugin.class
  //      ,
  //      MockTcpTransportPlugin.class
          ), i -> i);
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      cluster.beforeTest(new Random(), 1L);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    client = new EsClient(cluster.masterClient());
  }

  public void afterTest() throws IOException {
    cluster.afterTest();
  }

  protected NodeConfigurationSource getNodeConfigSource() {
    Settings.Builder networkSettings = Settings.builder();
    final boolean isNetwork = false;

      if (isNetwork) {//FIXME
        networkSettings.put(NetworkModule.TRANSPORT_TYPE_KEY, AssertingLocalTransport.ASSERTING_TRANSPORT_NAME);
      } else {
        networkSettings.put(NetworkModule.TRANSPORT_TYPE_KEY, "local");
      }

    NodeConfigurationSource nodeConfigurationSource = new NodeConfigurationSource() {
      @Override
      public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
          .put(NetworkModule.HTTP_ENABLED.getKey(), false)
          .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(),
            isNetwork ? DiscoveryModule.DISCOVERY_TYPE_SETTING.getDefault(Settings.EMPTY) : "local")
          .put(networkSettings.build())
          .build();
      }

      @Override
      public Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.emptyList();
      }

      @Override
      public Settings transportClientSettings() {
        return Settings.builder().put(networkSettings.build()).build();
      }

      @Override
      public Collection<Class<? extends Plugin>> transportClientPlugins() {
        Collection<Class<? extends Plugin>> plugins = Collections.emptyList();
        if (isNetwork && plugins.contains(MockTcpTransportPlugin.class) == false) {
          plugins = new ArrayList<>(plugins);
          plugins.add(MockTcpTransportPlugin.class);
        } else if (isNetwork == false && plugins.contains(AssertingLocalTransport.class) == false) {
          plugins = new ArrayList<>(plugins);
          plugins.add(AssertingLocalTransport.TestPlugin.class);
        }
        return Collections.unmodifiableCollection(plugins);
      }
    };
    return nodeConfigurationSource;
  }

  public void name() {
  }

  @Override
  public void before() throws Throwable {
    cluster.beforeTest(new Random(), 1L);

    if (!indexDefinitions.isEmpty()) {
      container = new ComponentContainer();
      container.addSingleton(new MapSettings());
      container.addSingleton(new ConfigurationProvider());
      container.addSingletons(indexDefinitions);
      container.addSingleton(client);
      container.addSingleton(IndexDefinitions.class);
      container.addSingleton(IndexCreator.class);
      container.addSingleton(MetadataIndex.class);
      container.addSingleton(MetadataIndexDefinition.class);
      container.startComponents();
    }
  }

  @Override
  public void after() {
    if (container != null) {
      container.stopComponents();
    }
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      try {
        afterInternal(true);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      cluster.close();
    }
  }

  protected final void afterInternal(boolean afterClass) throws Exception {
    boolean success = false;
    try {
      try {
        if (cluster != null) {
//          if (currentClusterScope != ESIntegTestCase.Scope.TEST) {
//            MetaData metaData = client().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
//            assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(), metaData
//              .persistentSettings().getAsMap().size(), equalTo(0));
//            assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(), metaData
//              .transientSettings().getAsMap().size(), equalTo(0));
//          }
          ensureClusterSizeConsistency();
          ensureClusterStateConsistency();
//          if (isInternalCluster()) {
//            // check no pending cluster states are leaked
//            for (Discovery discovery : internalCluster().getInstances(Discovery.class)) {
//              if (discovery instanceof ZenDiscovery) {
//                final ZenDiscovery zenDiscovery = (ZenDiscovery) discovery;
//                assertBusy(new Runnable() {
//                  @Override
//                  public void run() {
//                    assertThat("still having pending states: " + Strings.arrayToDelimitedString(zenDiscovery.pendingClusterStates(), "\n"),
//                      zenDiscovery.pendingClusterStates(), emptyArray());
//                  }
//                });
//              }
//            }
//          }
          cluster.beforeIndexDeletion();
          cluster.wipe(Collections.emptySet()); // wipe after to make sure we fail in the test that didn't ack the delete
          if (afterClass) {// || currentClusterScope == ESIntegTestCase.Scope.TEST) {
            cluster.close();
          }
          cluster.assertAfterTest();
        }
      } finally {
//        if (currentClusterScope == ESIntegTestCase.Scope.TEST) {
//          clearClusters(); // it is ok to leave persistent / transient cluster state behind if scope is TEST
//        }
      }
      success = true;
    } finally {
      if (!success) {
        // if we failed here that means that something broke horribly so we should clear all clusters
        // TODO: just let the exception happen, WTF is all this horseshit
        // afterTestRule.forceFailure();
      }
    }
  }

  protected void ensureClusterSizeConsistency() {
    if (cluster != null) { // if static init fails the cluster can be null
//      logger.trace("Check consistency for [{}] nodes", cluster().size());
      assertNoTimeout(cluster.client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(cluster.size())).get());
    }
  }

  /**
   * Verifies that all nodes that have the same version of the cluster state as master have same cluster state
   */
  protected void ensureClusterStateConsistency() throws IOException {
    if (cluster != null) {
      ClusterState masterClusterState = cluster.client().admin().cluster().prepareState().all().get().getState();
      byte[] masterClusterStateBytes = ClusterState.Builder.toBytes(masterClusterState);
      // remove local node reference
      masterClusterState = ClusterState.Builder.fromBytes(masterClusterStateBytes, null);
      Map<String, Object> masterStateMap = convertToMap(masterClusterState);
      int masterClusterStateSize = ClusterState.Builder.toBytes(masterClusterState).length;
      String masterId = masterClusterState.nodes().getMasterNodeId();
      for (Client client : cluster.getClients()) {
        ClusterState localClusterState = client.admin().cluster().prepareState().all().setLocal(true).get().getState();
        byte[] localClusterStateBytes = ClusterState.Builder.toBytes(localClusterState);
        // remove local node reference
        localClusterState = ClusterState.Builder.fromBytes(localClusterStateBytes, null);
        final Map<String, Object> localStateMap = convertToMap(localClusterState);
        final int localClusterStateSize = ClusterState.Builder.toBytes(localClusterState).length;
        // Check that the non-master node has the same version of the cluster state as the master and
        // that the master node matches the master (otherwise there is no requirement for the cluster state to match)
        if (masterClusterState.version() == localClusterState.version() && masterId.equals(localClusterState.nodes().getMasterNodeId())) {
//          try {
//            assertEquals("clusterstate UUID does not match", masterClusterState.stateUUID(), localClusterState.stateUUID());
//            // We cannot compare serialization bytes since serialization order of maps is not guaranteed
//            // but we can compare serialization sizes - they should be the same
//            assertEquals("clusterstate size does not match", masterClusterStateSize, localClusterStateSize);
//            // Compare JSON serialization
//            assertNull("clusterstate JSON serialization does not match", differenceBetweenMapsIgnoringArrayOrder(masterStateMap, localStateMap));
//          } catch (AssertionError error) {
//            logger.error("Cluster state from master:\n{}\nLocal cluster state:\n{}", masterClusterState.toString(), localClusterState.toString());
//            throw error;
//          }
        }
      }
    }

  }




  private void deleteIndices() {
    client.nativeClient().admin().indices().prepareDelete("_all").get();
  }

  public void deleteIndex(String indexName) {
    client.nativeClient().admin().indices().prepareDelete(indexName).get();
  }

  public void putDocuments(String index, String type, BaseDoc... docs) {
    putDocuments(new IndexType(index, type), docs);
  }

  public void putDocuments(IndexType indexType, BaseDoc... docs) {
    try {
      BulkRequestBuilder bulk = client.prepareBulk()
        .setRefreshPolicy(REFRESH_IMMEDIATE);
      for (BaseDoc doc : docs) {
        bulk.add(new IndexRequest(indexType.getIndex(), indexType.getType(), doc.getId())
          .parent(doc.getParent())
          .routing(doc.getRouting())
          .source(doc.getFields()));
      }
      EsUtils.executeBulkRequest(bulk, "");
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public long countDocuments(String index, String type) {
    return countDocuments(new IndexType(index, type));
  }

  public long countDocuments(IndexType indexType) {
    return client().prepareSearch(indexType).setSize(0).get().getHits().totalHits();
  }

  /**
   * Get all the indexed documents (no paginated results). Results are converted to BaseDoc objects.
   * Results are not sorted.
   */
  public <E extends BaseDoc> List<E> getDocuments(IndexType indexType, final Class<E> docClass) {
    List<SearchHit> hits = getDocuments(indexType);
    return newArrayList(Collections2.transform(hits, input -> {
      try {
        return (E) ConstructorUtils.invokeConstructor(docClass, input.getSource());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }));
  }

  /**
   * Get all the indexed documents (no paginated results). Results are not sorted.
   */
  public List<SearchHit> getDocuments(IndexType indexType) {
    SearchRequestBuilder req = client.nativeClient().prepareSearch(indexType.getIndex()).setTypes(indexType.getType()).setQuery(QueryBuilders.matchAllQuery());
    EsUtils.optimizeScrollRequest(req);
    req.setScroll(new TimeValue(60000))
      .setSize(100);

    SearchResponse response = req.get();
    List<SearchHit> result = newArrayList();
    while (true) {
      Iterables.addAll(result, response.getHits());
      response = client.nativeClient().prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      // Break condition: No hits are returned
      if (response.getHits().getHits().length == 0) {
        break;
      }
    }
    return result;
  }

  /**
   * Get a list of a specific field from all indexed documents.
   */
  public <T> List<T> getDocumentFieldValues(IndexType indexType, final String fieldNameToReturn) {
    return newArrayList(Iterables.transform(getDocuments(indexType), new Function<SearchHit, T>() {
      @Override
      public T apply(SearchHit input) {
        return (T) input.sourceAsMap().get(fieldNameToReturn);
      }
    }));
  }

  public List<String> getIds(IndexType indexType) {
    return FluentIterable.from(getDocuments(indexType)).transform(SearchHitToId.INSTANCE).toList();
  }

  public EsClient client() {
    return client;
  }

  private enum SearchHitToId implements Function<SearchHit, String> {
    INSTANCE;

    @Override
    public String apply(@Nonnull org.elasticsearch.search.SearchHit input) {
      return input.id();
    }
  }

  private static class NodeHolder {
    private static final NodeHolder INSTANCE = new NodeHolder();

    private final Node node;

    private NodeHolder() {
      String nodeName = "tmp-es-" + RandomUtils.nextInt();
      Path tmpDir;
      try {
        tmpDir = Files.createTempDirectory("tmp-es");
      } catch (IOException e) {
        throw new RuntimeException("Cannot create elasticsearch temporary directory", e);
      }

      tmpDir.toFile().deleteOnExit();

      Settings.Builder settings = Settings.builder()
        .put("transport.type", "local")
        .put("node.data", true)
        .put("cluster.name", nodeName)
        .put("node.name", nodeName)
        // the two following properties are probably not used because they are
        // declared on indices too
        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
        // limit the number of threads created (see org.elasticsearch.common.util.concurrent.EsExecutors)
        .put("processors", 1)
        .put("http.enabled", false)
        .put("config.ignore_system_properties", true)
        .put("path.home", tmpDir);
      node = new Node(settings.build());
      try {
        node.start();
      } catch (NodeValidationException e) {
        throw new RuntimeException("Cannot start Elasticsearch node", e);
      }
      checkState(!node.isClosed());

      // wait for node to be ready
      node.client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();

      // delete the indices (should not exist)
      DeleteIndexResponse response = node.client().admin().indices().prepareDelete("_all").get();
      checkState(response.isAcknowledged());
    }
  }
}
