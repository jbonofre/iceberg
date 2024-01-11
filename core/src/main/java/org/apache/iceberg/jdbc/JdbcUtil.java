/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

final class JdbcUtil {
  // property to control strict-mode (aka check if namespace exists when creating a table)
  static final String STRICT_MODE_PROPERTY = JdbcCatalog.PROPERTY_PREFIX + "strict-mode";

  // Catalog Table & View
  static final String CATALOG_TABLE_NAME = "iceberg_tables";
  static final String CATALOG_VIEW_NAME = "iceberg_views";
  static final String CATALOG_NAME = "catalog_name";
  static final String TABLE_NAMESPACE = "table_namespace";
  static final String VIEW_NAMESPACE = "view_namespace";
  static final String TABLE_NAME = "table_name";
  static final String VIEW_NAME = "view_name";

  static final String DO_COMMIT_TABLE_OR_VIEW_SQL =
      "UPDATE "
          + "%s" // table or view SQL table
          + " SET "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " = ? , "
          + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
          + " = ? "
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + " %s = ? AND " // table or view namespace
          + " %s = ? AND " // table or view name
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " = ?";
  static final String CREATE_CATALOG_TABLE_OR_VIEW =
      "CREATE TABLE "
          + "%s" // table or view SQL table
          + "("
          + CATALOG_NAME
          + " VARCHAR(255) NOT NULL,"
          + "%s" // table or view namespace
          + " VARCHAR(255) NOT NULL,"
          + "%s" // table or view name
          + " VARCHAR(255) NOT NULL,"
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " VARCHAR(1000),"
          + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
          + " VARCHAR(1000),"
          + "PRIMARY KEY ("
          + CATALOG_NAME
          + ", "
          + "%s" // table or view namespace
          + ", "
          + "%s" // table or view name
          + ")"
          + ")";
  static final String GET_TABLE_OR_VIEW_SQL =
      "SELECT * FROM "
          + "%s" // table or view SQL table
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + " %s = ? AND " // table or view namespace
          + " %s = ? "; // table or view name
  static final String LIST_TABLES_OR_VIEWS_SQL =
      "SELECT * FROM "
          + "%s" // catalog table or view SQL table
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + "%s" // table or view namespace
          + " = ?";
  static final String RENAME_TABLE_OR_VIEW_SQL =
      "UPDATE "
          + "%s" // table or view SQL table
          + " SET "
          + "%s = ?, " // table or view namespace
          + "%s = ? " // table or view name
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + " %s = ? AND " // table or view namespace
          + "%s = ?"; // table or view name
  static final String DROP_TABLE_OR_VIEW_SQL =
      "DELETE FROM "
          + "%s" // table or view SQL table
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + "%s = ? AND " // table or view namespace
          + "%s = ?"; // table or view name
  static final String GET_TABLE_NAMESPACE_SQL =
      "SELECT "
          + TABLE_NAMESPACE
          + " FROM "
          + CATALOG_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + " ( "
          + TABLE_NAMESPACE
          + " = ? OR "
          + TABLE_NAMESPACE
          + " LIKE ? ESCAPE '\\' "
          + " ) "
          + " LIMIT 1";
  static final String GET_VIEW_NAMESPACE_SQL =
      "SELECT "
          + VIEW_NAMESPACE
          + " FROM "
          + CATALOG_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + " ( "
          + VIEW_NAMESPACE
          + " = ? OR "
          + VIEW_NAMESPACE
          + " LIKE ? ESCAPE '\\' "
          + " ) "
          + " LIMIT 1";
  static final String LIST_TABLE_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + TABLE_NAMESPACE
          + " FROM "
          + CATALOG_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " LIKE ?";
  static final String LIST_VIEW_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + VIEW_NAMESPACE
          + " FROM "
          + CATALOG_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + VIEW_NAMESPACE
          + " LIKE ?";
  static final String LIST_ALL_TABLE_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + TABLE_NAMESPACE
          + " FROM "
          + CATALOG_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ?";
  static final String LIST_ALL_VIEW_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + VIEW_NAMESPACE
          + " FROM "
          + CATALOG_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ?";
  static final String DO_COMMIT_CREATE_TABLE_OR_VIEW_SQL =
      "INSERT INTO "
          + "%s" // table or view SQL table
          + " ("
          + CATALOG_NAME
          + ", "
          + "%s" // table or view namespace
          + ", "
          + "%s" // table or view name
          + ", "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + ", "
          + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
          + ") "
          + " VALUES (?,?,?,?,null)";

  // Catalog Namespace Properties
  static final String NAMESPACE_PROPERTIES_TABLE_NAME = "iceberg_namespace_properties";
  static final String NAMESPACE_NAME = "namespace";
  static final String NAMESPACE_PROPERTY_KEY = "property_key";
  static final String NAMESPACE_PROPERTY_VALUE = "property_value";

  static final String CREATE_NAMESPACE_PROPERTIES_TABLE =
      "CREATE TABLE "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + "("
          + CATALOG_NAME
          + " VARCHAR(255) NOT NULL,"
          + NAMESPACE_NAME
          + " VARCHAR(255) NOT NULL,"
          + NAMESPACE_PROPERTY_KEY
          + " VARCHAR(255),"
          + NAMESPACE_PROPERTY_VALUE
          + " VARCHAR(1000),"
          + "PRIMARY KEY ("
          + CATALOG_NAME
          + ", "
          + NAMESPACE_NAME
          + ", "
          + NAMESPACE_PROPERTY_KEY
          + ")"
          + ")";
  static final String GET_NAMESPACE_PROPERTIES_SQL =
      "SELECT "
          + NAMESPACE_NAME
          + " FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + " ( "
          + NAMESPACE_NAME
          + " = ? OR "
          + NAMESPACE_NAME
          + " LIKE ? ESCAPE '\\' "
          + " ) ";
  static final String INSERT_NAMESPACE_PROPERTIES_SQL =
      "INSERT INTO "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " ("
          + CATALOG_NAME
          + ", "
          + NAMESPACE_NAME
          + ", "
          + NAMESPACE_PROPERTY_KEY
          + ", "
          + NAMESPACE_PROPERTY_VALUE
          + ") VALUES ";
  static final String INSERT_PROPERTIES_VALUES_BASE = "(?,?,?,?)";
  static final String GET_ALL_NAMESPACE_PROPERTIES_SQL =
      "SELECT * "
          + " FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + NAMESPACE_NAME
          + " = ? ";
  static final String DELETE_NAMESPACE_PROPERTIES_SQL =
      "DELETE FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + NAMESPACE_NAME
          + " = ? AND "
          + NAMESPACE_PROPERTY_KEY
          + " IN ";
  static final String DELETE_ALL_NAMESPACE_PROPERTIES_SQL =
      "DELETE FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + NAMESPACE_NAME
          + " = ?";
  static final String LIST_PROPERTY_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + NAMESPACE_NAME
          + " FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + NAMESPACE_NAME
          + " LIKE ?";
  static final String LIST_ALL_PROPERTY_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + NAMESPACE_NAME
          + " FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ?";

  // Utilities
  private static final Joiner JOINER_DOT = Joiner.on('.');
  private static final Splitter SPLITTER_DOT = Splitter.on('.');

  private JdbcUtil() {}

  public static Namespace stringToNamespace(String namespace) {
    Preconditions.checkArgument(namespace != null, "Invalid namespace %s", namespace);
    return Namespace.of(Iterables.toArray(SPLITTER_DOT.split(namespace), String.class));
  }

  public static String namespaceToString(Namespace namespace) {
    return JOINER_DOT.join(namespace.levels());
  }

  public static TableIdentifier stringToTableIdentifier(String tableNamespace, String tableName) {
    return TableIdentifier.of(JdbcUtil.stringToNamespace(tableNamespace), tableName);
  }

  public static Properties filterAndRemovePrefix(Map<String, String> properties, String prefix) {
    Properties result = new Properties();
    properties.forEach(
        (key, value) -> {
          if (key.startsWith(prefix)) {
            result.put(key.substring(prefix.length()), value);
          }
        });

    return result;
  }

  /**
   * Create SQL statement to get a table or view.
   *
   * @param table true to get the SQL statement for a table, false for a view.
   * @return the SQL statement.
   */
  public static String getTableOrViewSqlStatement(boolean table) {
    String tableOrViewTableName = table ? CATALOG_TABLE_NAME : CATALOG_VIEW_NAME;
    String tableOrViewNamespace = table ? TABLE_NAMESPACE : VIEW_NAMESPACE;
    String tableOrViewName = table ? TABLE_NAME : VIEW_NAME;
    return String.format(
        GET_TABLE_OR_VIEW_SQL, tableOrViewTableName, tableOrViewNamespace, tableOrViewName);
  }

  /**
   * Create SQL statement to update a table or view.
   *
   * @param table true to get the SQL statement for a table, false for a view.
   * @return the SQL statement.
   */
  public static String doCommitSqlStatement(boolean table) {
    String tableOrViewTableName = table ? CATALOG_TABLE_NAME : CATALOG_VIEW_NAME;
    String tableOrViewNamespace = table ? TABLE_NAMESPACE : VIEW_NAMESPACE;
    String tableOrViewName = table ? TABLE_NAME : VIEW_NAME;
    return String.format(
        DO_COMMIT_TABLE_OR_VIEW_SQL, tableOrViewTableName, tableOrViewNamespace, tableOrViewName);
  }

  /**
   * Create SQL statement to create a table or view.
   *
   * @param table true to get the SQL statement for a table, false for a view.
   * @return the SQL statement.
   */
  public static String createCatalogTableOrViewSqlStatement(boolean table) {
    String tableOrViewTableName = table ? CATALOG_TABLE_NAME : CATALOG_VIEW_NAME;
    String tableOrViewNamespace = table ? TABLE_NAMESPACE : VIEW_NAMESPACE;
    String tableOrViewName = table ? TABLE_NAME : VIEW_NAME;
    return String.format(
        CREATE_CATALOG_TABLE_OR_VIEW,
        tableOrViewTableName,
        tableOrViewNamespace,
        tableOrViewName,
        tableOrViewNamespace,
        tableOrViewName);
  }

  /**
   * Create SQL statement to list tables or views.
   *
   * @param table true to get the SQL statement for a table, false for a view.
   * @return the SQL statement.
   */
  public static String listTablesOrViewsSqlStatement(boolean table) {
    String tableOrViewTableName = table ? CATALOG_TABLE_NAME : CATALOG_VIEW_NAME;
    String tableOrViewNamespace = table ? TABLE_NAMESPACE : VIEW_NAMESPACE;
    return String.format(LIST_TABLES_OR_VIEWS_SQL, tableOrViewTableName, tableOrViewNamespace);
  }

  /**
   * Create SQL statement to rename a table or view.
   *
   * @param table true to get the SQL statement for a table, false for a view.
   * @return the SQL statement.
   */
  public static String renameTableOrViewSqlStatement(boolean table) {
    String tableOrViewTableName = table ? CATALOG_TABLE_NAME : CATALOG_VIEW_NAME;
    String tableOrViewNamespace = table ? TABLE_NAMESPACE : VIEW_NAMESPACE;
    String tableOrViewName = table ? TABLE_NAME : VIEW_NAME;
    return String.format(
        RENAME_TABLE_OR_VIEW_SQL,
        tableOrViewTableName,
        tableOrViewNamespace,
        tableOrViewName,
        tableOrViewNamespace,
        tableOrViewName);
  }

  /**
   * Create SQL statement to delete a table or view.
   *
   * @param table true to get the SQL statement for a table, false for a view.
   * @return the SQL statement.
   */
  public static String dropTableOrViewSqlStatement(boolean table) {
    String tableOrViewTableName = table ? CATALOG_TABLE_NAME : CATALOG_VIEW_NAME;
    String tableOrViewNamespace = table ? TABLE_NAMESPACE : VIEW_NAMESPACE;
    String tableOrViewName = table ? TABLE_NAME : VIEW_NAME;
    return String.format(
        DROP_TABLE_OR_VIEW_SQL, tableOrViewTableName, tableOrViewNamespace, tableOrViewName);
  }

  /**
   * Create SQL statement to create a table or view.
   *
   * @param table true to get the SQL statement for a table, false for a view.
   * @return the SQL statement.
   */
  public static String doCommitCreateTableOrViewSqlStatement(boolean table) {
    String tableOrViewTableName = table ? CATALOG_TABLE_NAME : CATALOG_VIEW_NAME;
    String tableOrViewNamespace = table ? TABLE_NAMESPACE : VIEW_NAMESPACE;
    String tableOrViewName = table ? TABLE_NAME : VIEW_NAME;
    return String.format(
        DO_COMMIT_CREATE_TABLE_OR_VIEW_SQL,
        tableOrViewTableName,
        tableOrViewNamespace,
        tableOrViewName);
  }

  /**
   * Update a table or view.
   *
   * @param table true to update a table, false to update a view.
   * @param connections the {@link JdbcClientPool} to connect to the database.
   * @param catalogName the catalog name.
   * @param tableOrViewIdentifier the table or view identifier.
   * @param newMetadataLocation the new metadata location.
   * @param oldMetadataLocation the old metadata location.
   * @return the number of updated record.
   * @throws SQLException if a SQL error happens on the database.
   * @throws InterruptedException interrupt the SQL query execution.
   */
  public static int updateTableOrView(
      boolean table,
      JdbcClientPool connections,
      String catalogName,
      TableIdentifier tableOrViewIdentifier,
      String newMetadataLocation,
      String oldMetadataLocation)
      throws SQLException, InterruptedException {
    return connections.run(
        conn -> {
          try (PreparedStatement sql = conn.prepareStatement(doCommitSqlStatement(table))) {
            // UPDATE
            sql.setString(1, newMetadataLocation);
            sql.setString(2, oldMetadataLocation);
            // WHERE
            sql.setString(3, catalogName);
            sql.setString(4, namespaceToString(tableOrViewIdentifier.namespace()));
            sql.setString(5, tableOrViewIdentifier.name());
            sql.setString(6, oldMetadataLocation);
            return sql.executeUpdate();
          }
        });
  }

  /**
   * Get a table or view.
   *
   * @param table true of get a table, false to get a view.
   * @param connections the {@link JdbcClientPool} to connect to the database.
   * @param catalogName the catalog name.
   * @param tableOrViewIdentifier the table or view identifier.
   * @return the table or view.
   * @throws SQLException if a SQL error happens on the database.
   * @throws InterruptedException interrupt the SQL query execution.
   */
  public static Map<String, String> getTableOrView(
      boolean table,
      JdbcClientPool connections,
      String catalogName,
      TableIdentifier tableOrViewIdentifier)
      throws SQLException, InterruptedException {
    return connections.run(
        conn -> {
          Map<String, String> tableOrView = Maps.newHashMap();

          String getTableOrViewSqlStatement = JdbcUtil.getTableOrViewSqlStatement(table);
          try (PreparedStatement sql = conn.prepareStatement(getTableOrViewSqlStatement)) {
            sql.setString(1, catalogName);
            sql.setString(2, namespaceToString(tableOrViewIdentifier.namespace()));
            sql.setString(3, tableOrViewIdentifier.name());
            ResultSet rs = sql.executeQuery();

            if (rs.next()) {
              tableOrView.put(JdbcUtil.CATALOG_NAME, rs.getString(JdbcUtil.CATALOG_NAME));
              tableOrView.put(
                  (table ? JdbcUtil.TABLE_NAMESPACE : JdbcUtil.VIEW_NAMESPACE),
                  rs.getString((table ? JdbcUtil.TABLE_NAMESPACE : JdbcUtil.VIEW_NAMESPACE)));
              tableOrView.put(
                  (table ? JdbcUtil.TABLE_NAME : JdbcUtil.VIEW_NAME),
                  rs.getString((table ? JdbcUtil.TABLE_NAME : JdbcUtil.VIEW_NAME)));
              tableOrView.put(
                  BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
                  rs.getString(BaseMetastoreTableOperations.METADATA_LOCATION_PROP));
              tableOrView.put(
                  BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP,
                  rs.getString(BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP));
            }

            rs.close();
          }

          return tableOrView;
        });
  }

  /**
   * Insert a table or view in the catalog.
   *
   * @param table true to insert a table, false to insert a view.
   * @param connections the {@link JdbcClientPool} to connect to the database.
   * @param catalogName the catalog name.
   * @param namespace the namespace.
   * @param tableOrViewIdentifier the table or view identifier.
   * @param newMetadataLocation the new metadata location.
   * @return the number of updated records.
   * @throws SQLException if a SQL error happens on the database.
   * @throws InterruptedException interrupt the SQL query execution.
   */
  public static int doCommitCreateTableOrView(
      boolean table,
      JdbcClientPool connections,
      String catalogName,
      Namespace namespace,
      TableIdentifier tableOrViewIdentifier,
      String newMetadataLocation)
      throws SQLException, InterruptedException {
    return connections.run(
        conn -> {
          try (PreparedStatement sql =
              conn.prepareStatement(doCommitCreateTableOrViewSqlStatement(table))) {
            sql.setString(1, catalogName);
            sql.setString(2, namespaceToString(namespace));
            sql.setString(3, tableOrViewIdentifier.name());
            sql.setString(4, newMetadataLocation);
            return sql.executeUpdate();
          }
        });
  }

  static boolean viewExists(
      String catalogName, JdbcClientPool connections, TableIdentifier viewIdentifier) {
    if (exists(
        connections,
        JdbcUtil.getTableOrViewSqlStatement(false),
        catalogName,
        namespaceToString(viewIdentifier.namespace()),
        viewIdentifier.name())) {
      return true;
    }
    return false;
  }

  static boolean tableExists(
      String catalogName, JdbcClientPool connections, TableIdentifier tableIdentifier) {
    if (exists(
        connections,
        JdbcUtil.getTableOrViewSqlStatement(true),
        catalogName,
        namespaceToString(tableIdentifier.namespace()),
        tableIdentifier.name())) {
      return true;
    }
    return false;
  }

  public static String updatePropertiesStatement(int size) {
    StringBuilder sqlStatement =
        new StringBuilder(
            "UPDATE "
                + NAMESPACE_PROPERTIES_TABLE_NAME
                + " SET "
                + NAMESPACE_PROPERTY_VALUE
                + " = CASE");
    for (int i = 0; i < size; i += 1) {
      sqlStatement.append(" WHEN " + NAMESPACE_PROPERTY_KEY + " = ? THEN ?");
    }
    sqlStatement.append(
        " END WHERE "
            + CATALOG_NAME
            + " = ? AND "
            + NAMESPACE_NAME
            + " = ? AND "
            + NAMESPACE_PROPERTY_KEY
            + " IN ");

    String values = String.join(",", Collections.nCopies(size, String.valueOf('?')));
    sqlStatement.append("(").append(values).append(")");

    return sqlStatement.toString();
  }

  public static String insertPropertiesStatement(int size) {
    StringBuilder sqlStatement = new StringBuilder(JdbcUtil.INSERT_NAMESPACE_PROPERTIES_SQL);

    for (int i = 0; i < size; i++) {
      if (i != 0) {
        sqlStatement.append(", ");
      }
      sqlStatement.append(JdbcUtil.INSERT_PROPERTIES_VALUES_BASE);
    }

    return sqlStatement.toString();
  }

  public static String deletePropertiesStatement(Set<String> properties) {
    StringBuilder sqlStatement = new StringBuilder(JdbcUtil.DELETE_NAMESPACE_PROPERTIES_SQL);
    String values = String.join(",", Collections.nCopies(properties.size(), String.valueOf('?')));
    sqlStatement.append("(").append(values).append(")");

    return sqlStatement.toString();
  }

  static boolean namespaceExists(
      String catalogName, JdbcClientPool connections, Namespace namespace) {

    String namespaceEquals = JdbcUtil.namespaceToString(namespace);
    // when namespace has sub-namespace then additionally checking it with LIKE statement.
    // catalog.db can exists as: catalog.db.ns1 or catalog.db.ns1.ns2
    String namespaceStartsWith =
        namespaceEquals.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%") + ".%";
    if (exists(
        connections,
        JdbcUtil.GET_TABLE_NAMESPACE_SQL,
        catalogName,
        namespaceEquals,
        namespaceStartsWith)) {
      return true;
    }

    if (exists(
        connections,
        JdbcUtil.GET_NAMESPACE_PROPERTIES_SQL,
        catalogName,
        namespaceEquals,
        namespaceStartsWith)) {
      return true;
    }

    if (exists(
        connections,
        JdbcUtil.GET_VIEW_NAMESPACE_SQL,
        catalogName,
        namespaceEquals,
        namespaceStartsWith)) {
      return true;
    }

    return false;
  }

  @SuppressWarnings("checkstyle:NestedTryDepth")
  private static boolean exists(JdbcClientPool connections, String sql, String... args) {
    try {
      return connections.run(
          conn -> {
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
              for (int pos = 0; pos < args.length; pos += 1) {
                preparedStatement.setString(pos + 1, args[pos]);
              }

              try (ResultSet rs = preparedStatement.executeQuery()) {
                if (rs.next()) {
                  return true;
                }
              }
            }

            return false;
          });
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to execute exists query: %s", sql);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in SQL query");
    }
  }
}
