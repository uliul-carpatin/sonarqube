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
// @flow
import React from 'react';
import { formatMeasure } from '../../../helpers/measures';
import { translate } from '../../../helpers/l10n';
import type { MeasureHistory } from '../types';

type Props = {
  measuresHistory: Array<MeasureHistory>,
  tooltipIdx: number
};

export default function GraphsTooltipsContentDuplication({ measuresHistory, tooltipIdx }: Props) {
  const duplicationDensity = measuresHistory.find(
    measure => measure.metric === 'duplicated_lines_density'
  );
  if (!duplicationDensity || !duplicationDensity.history[tooltipIdx]) {
    return null;
  }
  const duplicationDensityValue = duplicationDensity.history[tooltipIdx].value;
  if (!duplicationDensityValue) {
    return null;
  }
  return (
    <tbody>
      <tr>
        <td className="project-activity-graph-tooltip-separator" colSpan="3">
          <hr />
        </td>
      </tr>
      <tr className="project-activity-graph-tooltip-line">
        <td
          colSpan="2"
          className="project-activity-graph-tooltip-value text-right spacer-right thin">
          {formatMeasure(duplicationDensityValue, 'PERCENT')}
        </td>
        <td>
          {translate('metric.duplicated_lines_density.name')}
        </td>
      </tr>
    </tbody>
  );
}
