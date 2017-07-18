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
package org.sonarqube.tests.projectAdministration;

import com.sonar.orchestrator.Orchestrator;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.sonarqube.tests.Category6Suite;
import org.sonarqube.tests.Tester;
import org.sonarqube.ws.Organizations;
import org.sonarqube.ws.WsComponents;
import org.sonarqube.ws.WsProjects;
import org.sonarqube.ws.client.GetRequest;
import org.sonarqube.ws.client.WsResponse;
import org.sonarqube.ws.client.component.SearchProjectsRequest;
import org.sonarqube.ws.client.project.CreateRequest;
import org.sonarqube.ws.client.project.SearchWsRequest;
import org.sonarqube.ws.client.project.UpdateKeyWsRequest;
import util.ItUtils;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class ProjectKeyUpdateTest {

  @ClassRule
  public static final Orchestrator orchestrator = Category6Suite.ORCHESTRATOR;

  @Rule
  public TestRule safeguard = new DisableOnDebug(Timeout.seconds(300));
  @Rule
  public Tester tester = new Tester(orchestrator)
    .setElasticsearchHttpPort(Category6Suite.SEARCH_HTTP_PORT);

  @After
  public void tearDown() throws Exception {
    unlockWritesOnProjectIndices();
  }

  @Test
  public void update_key_of_provisioned_project() {
    Organizations.Organization organization = tester.organizations().generate();
    WsProjects.CreateWsResponse.Project project = createProject(organization, "one", "Foo");

    updateKey(project, "two");

    assertThat(isInProjectsSearch(organization, "one")).isFalse();
    assertThat(isInProjectsSearch(organization, "two")).isTrue();
    assertThat(keyInComponentSearchProjects("Foo")).isEqualTo("two");
    assertThat(keysInComponentSuggestions("Foo")).containsExactly("two");
  }

  @Test
  public void recover_indexing_errors_when_updating_key_of_provisioned_project() throws Exception {
    Organizations.Organization organization = tester.organizations().generate();
    WsProjects.CreateWsResponse.Project project = createProject(organization, "one", "Foo");

    tester.elasticsearch().lockWrites("components");
    tester.elasticsearch().lockWrites("projectmeasures");

    updateKey(project, "two");

    assertThat(isInProjectsSearch(organization, "one")).isFalse();

    // WS gets the list of projects from ES then reloads projects from db.
    // That's why keys in WS responses are correct.
    assertThat(isInProjectsSearch(organization, "one")).isFalse();
    assertThat(isInProjectsSearch(organization, "two")).isTrue();
    assertThat(keyInComponentSearchProjects("Foo")).isEqualTo("two");
    assertThat(keysInComponentSuggestions("Foo")).containsExactly("two");

    // however searching by key is inconsistent
    assertThat(keyInComponentSearchProjects("one")).isEqualTo("two");
    assertThat(keysInComponentSuggestions("one")).containsExactly("two");
    assertThat(keyInComponentSearchProjects("two")).isNull();
    assertThat(keysInComponentSuggestions("two")).isEmpty();

    unlockWritesOnProjectIndices();

    boolean recovered = false;
    while (!recovered) {
      // recovery daemon runs every second, see Category6Suite
      Thread.sleep(1_000L);
      recovered = keyInComponentSearchProjects("one") == null &&
        keysInComponentSuggestions("one").isEmpty() &&
        "two".equals(keyInComponentSearchProjects("two")) &&
        keysInComponentSuggestions("two").contains("two");
    }
  }

  private void unlockWritesOnProjectIndices() throws Exception {
    tester.elasticsearch().unlockWrites("components");
    tester.elasticsearch().unlockWrites("projectmeasures");
  }

  private void updateKey(WsProjects.CreateWsResponse.Project project, String newKey) {
    tester.wsClient().projects().updateKey(UpdateKeyWsRequest.builder().setKey(project.getKey()).setNewKey(newKey).build());
  }

  private WsProjects.CreateWsResponse.Project createProject(Organizations.Organization organization, String key, String name) {
    CreateRequest createRequest = CreateRequest.builder().setKey(key).setName(name).setOrganization(organization.getKey()).build();
    return tester.wsClient().projects().create(createRequest).getProject();
  }

  /**
   * Projects administration page - uses database
   */
  private boolean isInProjectsSearch(Organizations.Organization organization, String key) {
    WsProjects.SearchWsResponse response = tester.wsClient().projects().search(
      SearchWsRequest.builder().setOrganization(organization.getKey()).setQuery(key).setQualifiers(singletonList("TRK")).build());
    return response.getComponentsCount() > 0;
  }

  /**
   * Projects page - api/components/search_projects - uses ES + DB
   */
  @CheckForNull
  private String keyInComponentSearchProjects(String name) {
    WsComponents.SearchProjectsWsResponse response = tester.wsClient().components().searchProjects(
      SearchProjectsRequest.builder().setFilter("query=\"" + name + "\"").build());
    if (response.getComponentsCount() > 0) {
      return response.getComponents(0).getKey();
    }
    return null;
  }

  /**
   * Top-right search engine - api/components/suggestions - uses ES + DB
   */
  private List<String> keysInComponentSuggestions(String name) {
    GetRequest request = new GetRequest("api/components/suggestions").setParam("s", name);
    WsResponse response = tester.wsClient().wsConnector().call(request);
    Map<String, Object> json = ItUtils.jsonToMap(response.content());
    Collection<Map<String, Object>> results = (Collection<Map<String, Object>>) json.get("results");
    return results.stream()
      .filter(map -> "TRK".equals(map.get("q")))
      .flatMap(map -> ((Collection<Map<String, Object>>) map.get("items")).stream())
      .map(map -> (String) map.get("key"))
      .collect(Collectors.toList());
  }
}
