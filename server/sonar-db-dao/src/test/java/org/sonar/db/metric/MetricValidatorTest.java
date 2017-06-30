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
package org.sonar.db.metric;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.google.common.base.Strings.repeat;
import static org.sonar.db.metric.MetricDtoValidator.validateDescription;
import static org.sonar.db.metric.MetricDtoValidator.validateKey;
import static org.sonar.db.metric.MetricDtoValidator.validateShortName;
import static org.sonar.db.metric.MetricTesting.newMetricDto;

public class MetricValidatorTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private MetricDto metric = newMetricDto();

  @Test
  public void fail_if_key_longer_than_64_characters() {
    String a65 = repeat("a", 65);
    metric.setKey(a65);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Metric key length (65) is longer than the maximum authorized (64). '" + a65 + "' was provided.");

    validateKey(metric.getKey());
  }

  @Test
  public void fail_if_name_longer_than_64_characters() {
    String a65 = repeat("a", 65);
    metric.setShortName(a65);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Metric name length (65) is longer than the maximum authorized (64). '" + a65 + "' was provided.");

    validateShortName(metric.getShortName());
  }

  @Test
  public void fail_if_description_longer_than_255_characters() {
    String a256 = repeat("a", 256);
    metric.setDescription(a256);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Metric description length (256) is longer than the maximum authorized (255). '" + a256 + "' was provided.");

    validateDescription(metric.getDescription());
  }
}
