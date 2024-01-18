/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.*;
import com.dremio.exec.store.jdbc.AbstractDremioSqlDialect;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.PostgreSQLDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.legacy.LegacyDialect;
import com.dremio.exec.store.jdbc.legacy.PostgreSQLLegacyDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.dremio.security.PasswordCredentials;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.protostuff.Tag;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;


/**
 * Configuration for 瀚高 sources.
 */
@SourceType(value = "HIGHGO", label = "highgo", uiConfig = "highgo-layout.json", externalQuerySupported = true)
public class HighgoConf extends AbstractArpConf<HighgoConf> {


    private static final String ARP_FILENAME = "arp/implementation/highgo-arp.yaml";

    /*  49 */   private static final PostgreSQLDialect PG_ARP_DIALECT = AbstractArpConf.<PostgreSQLDialect>loadArpFile(ARP_FILENAME, PostgreSQLDialect::new);

    private static final String DRIVER = "com.highgo.jdbc.Driver";

    @NotBlank
    @Tag(2)
    @Min(1L)
    @Max(65535L)
    @DisplayMetadata(label = "Port")
    public String port = "5866";

    @NotBlank
    @Tag(1)
    @DisplayMetadata(label = "Host")
    public String hostname;

    @NotBlank
    @Tag(3)
    @DisplayMetadata(label = "Database Name")
    public String databaseName;

    @Tag(4)
    public String username;
    @Tag(5)
    @Secret
    public String password;
    @Tag(6)
    public AuthenticationType authenticationType;
    @Tag(7)
    @DisplayMetadata(label = "Record fetch size")
    @NotMetadataImpacting
    public int fetchSize = 200;


    @Tag(8)
    @DisplayMetadata(label = "Enable legacy dialect")
    public boolean useLegacyDialect = false;

    @Tag(9)
    @DisplayMetadata(label = "Encrypt connection")
    @NotMetadataImpacting
    public boolean useSsl = false;

    @Tag(10)
    @NotMetadataImpacting
    @DisplayMetadata(label = "Validation Mode")
    /*  94 */ public EncryptionValidationMode encryptionValidationMode = EncryptionValidationMode.CERTIFICATE_AND_HOSTNAME_VALIDATION;

    @Tag(11)
    @DisplayMetadata(label = "Secret resource url")
    public String secretResourceUrl;

    @Tag(12)
    @NotMetadataImpacting
    @JsonIgnore
    public boolean enableExternalQuery = false;

    @Tag(13)
    public List<Property> propertyList;

    @Tag(14)
    @DisplayMetadata(label = "Maximum idle connections")
    @NotMetadataImpacting
    public int maxIdleConns = 8;

    @Tag(15)
    @DisplayMetadata(label = "Connection idle time (s)")
    @NotMetadataImpacting
    public int idleTimeSec = 60;

    @Tag(16)
    @DisplayMetadata(label = "Query timeout (s)")
    @NotMetadataImpacting
    public int queryTimeoutSec = 0;


    @Override
    public JdbcPluginConfig buildPluginConfig(JdbcPluginConfig.Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
        return configBuilder.withDialect((AbstractDremioSqlDialect) getDialect())
                .withDatasourceFactory(() -> newDataSource(credentialsService))
                .withShowOnlyConnDatabase(false)
                .withFetchSize(this.fetchSize)
                .withQueryTimeout(this.queryTimeoutSec)
                .build();
    }

    private CloseableDataSource newDataSource(CredentialsService credentialsService) throws SQLException {
        Properties properties = new Properties();

        PasswordCredentials credsFromCredentialsService = null;

        if (!Strings.isNullOrEmpty(this.secretResourceUrl)) {
            try {
                URI secretURI = URI.create(this.secretResourceUrl);
                credsFromCredentialsService = (PasswordCredentials) credentialsService.getCredentials(secretURI);
            } catch (IOException e) {
                throw new SQLException(e.getMessage(), e);
            }
        }

        if (this.useSsl) {
            properties.setProperty("ssl", "true");
            properties.setProperty("sslmode", getSslMode());
        }

        properties.setProperty("OpenSourceSubProtocolOverride", "true");
        if (null != this.propertyList) {
            this.propertyList.forEach(p -> properties.put(p.name, p.value));
        }

        return DataSources.newGenericConnectionPoolDataSource(DRIVER,

                  toJdbcConnectionString(), this.username,

                 (credsFromCredentialsService != null) ? credsFromCredentialsService.getPassword() : this.password, properties, DataSources.CommitMode.FORCE_MANUAL_COMMIT_MODE, this.maxIdleConns, this.idleTimeSec);
    }


    private String toJdbcConnectionString() {
        String hostname = (String) Preconditions.checkNotNull(this.hostname, "missing hostname");
        String portAsString = (String) Preconditions.checkNotNull(this.port, "missing port");
        int port = Integer.parseInt(portAsString);
        String db = (String) Preconditions.checkNotNull(this.databaseName, "missing database");

        return String.format("jdbc:highgo://%s:%d/%s", new Object[]{hostname, Integer.valueOf(port), db});
    }

    private String getSslMode() {
        Preconditions.checkNotNull(this.encryptionValidationMode, "missing validation mode");

        switch (this.encryptionValidationMode) {
            case CERTIFICATE_AND_HOSTNAME_VALIDATION:
                return "verify-full";
            case CERTIFICATE_ONLY_VALIDATION:
                return "verify-ca";
            case NO_VALIDATION:
                return "require";
        }
        throw new IllegalStateException(this.encryptionValidationMode + " is unknown");
    }


    protected LegacyDialect getLegacyDialect() {
        return (LegacyDialect) PostgreSQLLegacyDialect.INSTANCE;
    }


    protected ArpDialect getArpDialect() {
        return (ArpDialect) PG_ARP_DIALECT;
    }

    protected boolean getLegacyFlag() {
        return this.useLegacyDialect;
    }


    public static HighgoConf newMessage() {
        HighgoConf result = new HighgoConf();
        result.useLegacyDialect = true;
        return result;
    }

    @VisibleForTesting
    public static PostgreSQLDialect getDialectSingleton() {
        return PG_ARP_DIALECT;
    }

    @Override
    public JdbcDremioSqlDialect getDialect() {
        return PG_ARP_DIALECT;
    }

}
