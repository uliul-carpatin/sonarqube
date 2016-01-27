/*
 * SonarQube
 * Copyright (C) 2009-2016 SonarSource SA
 * mailto:contact AT sonarsource DOT com
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

import org.sonar.api.config.Settings;
import org.sonar.api.server.authentication.OAuth2IdentityProvider;
import org.sonar.api.server.authentication.UserIdentity;

public class FakeOAuth2IdProvider implements OAuth2IdentityProvider {

  private static final String ENABLED = "sonar.auth.fake-oauth2-id-provider.enabled";
  private static final String URL = "sonar.auth.fake-oauth2-id-provider.url";
  private static final String USER_INFO = "sonar.auth.fake-oauth2-id-provider.user";

  private final Settings settings;

  public FakeOAuth2IdProvider(Settings settings) {
    this.settings = settings;
  }


  @Override
  public void init(InitContext context) {
    String url = settings.getString(URL);
    if (url == null) {
      throw new IllegalStateException(String.format("The property %s is required", URL));
    }
    context.redirectTo(url);
  }

  @Override
  public void callback(CallbackContext context) {
    String userInfoProperty = settings.getString(USER_INFO);
    if (userInfoProperty == null) {
      throw new IllegalStateException(String.format("The property %s is required", USER_INFO));
    }
    String[] userInfos = userInfoProperty.split(",");
    context.authenticate(UserIdentity.builder()
      .setId(userInfos[0])
      .setName(userInfos[1])
      .setEmail(userInfos[2])
      .build());
    context.redirectToRequestedPage();
  }

  @Override
  public String getKey() {
    return "fake-oauth2-id-provider";
  }

  @Override
  public String getName() {
    return "Fake oauth2 identity provider";
  }

  @Override
  public String getIconPath() {
    return "/static/baseauthplugin/base.png";
  }

  @Override
  public boolean isEnabled() {
    return settings.getBoolean(ENABLED);
  }

  @Override
  public boolean allowsUsersToSignUp() {
    return true;
  }

}
