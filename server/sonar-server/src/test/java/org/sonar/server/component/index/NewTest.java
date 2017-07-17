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
package org.sonar.server.component.index;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sonar.api.config.internal.MapSettings;
import org.sonar.server.es.EsTester;

@RunWith(com.carrotsearch.randomizedtesting.RandomizedRunner.class)
public class NewTest  {

  public static EsTester es;

  @BeforeClass
  public static void beforeClass() throws Exception {
    es = new EsTester(
      new ComponentIndexDefinition(new MapSettings().asConfig())
    );
  }

  @Before
  public void before() {
    try {
      es.before();
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    try {
    es.after();
  } catch (Exception e) {
    e.printStackTrace();
  }
}

  @After
  public void after() throws Exception {
    try {
      es.afterTest();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void name() throws Exception {
//es.createIndices();

    IndicesExistsResponse x = es.client().prepareIndicesExist("components").get();
    System.out.println(x.isExists());
//    IndicesExistsResponse x2 = es.client().prepareIndicesExist("components").get();
//    System.out.println(x2.isExists());
  }
}
