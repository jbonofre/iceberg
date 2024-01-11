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

import java.util.Map;
import java.util.Properties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcUtil {

  @Test
  public void testGetTableOrViewSqlStatement() {
    String tableSql = JdbcUtil.getTableOrViewSqlStatement(true);
    Assertions.assertThat(tableSql)
        .isEqualTo(
            "SELECT * FROM iceberg_tables WHERE catalog_name = ? AND  table_namespace = ? AND  table_name = ? ");
    String viewSql = JdbcUtil.getTableOrViewSqlStatement(false);
    Assertions.assertThat(viewSql)
        .isEqualTo(
            "SELECT * FROM iceberg_views WHERE catalog_name = ? AND  view_namespace = ? AND  view_name = ? ");
  }

  @Test
  public void testDoCommitSqlStatement() {
    String tableSql = JdbcUtil.doCommitSqlStatement(true);
    Assertions.assertThat(tableSql)
        .isEqualTo(
            "UPDATE iceberg_tables SET metadata_location = ? , previous_metadata_location = ?  WHERE catalog_name = ? AND  table_namespace = ? AND  table_name = ? AND metadata_location = ?");
    String viewSql = JdbcUtil.doCommitSqlStatement(false);
    Assertions.assertThat(viewSql)
        .isEqualTo(
            "UPDATE iceberg_views SET metadata_location = ? , previous_metadata_location = ?  WHERE catalog_name = ? AND  view_namespace = ? AND  view_name = ? AND metadata_location = ?");
  }

  @Test
  public void testCreateTableOrViewSqlStatement() {
    String tableSql = JdbcUtil.createCatalogTableOrViewSqlStatement(true);
    Assertions.assertThat(tableSql)
        .isEqualTo(
            "CREATE TABLE iceberg_tables(catalog_name VARCHAR(255) NOT NULL,table_namespace VARCHAR(255) NOT NULL,table_name VARCHAR(255) NOT NULL,metadata_location VARCHAR(1000),previous_metadata_location VARCHAR(1000),PRIMARY KEY (catalog_name, table_namespace, table_name))");
    String viewSql = JdbcUtil.createCatalogTableOrViewSqlStatement(false);
    Assertions.assertThat(viewSql)
        .isEqualTo(
            "CREATE TABLE iceberg_views(catalog_name VARCHAR(255) NOT NULL,view_namespace VARCHAR(255) NOT NULL,view_name VARCHAR(255) NOT NULL,metadata_location VARCHAR(1000),previous_metadata_location VARCHAR(1000),PRIMARY KEY (catalog_name, view_namespace, view_name))");
  }

  @Test
  public void testListTablesOrViewsSqlStatement() {
    String tableSql = JdbcUtil.listTablesOrViewsSqlStatement(true);
    Assertions.assertThat(tableSql)
        .isEqualTo("SELECT * FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ?");
    String viewSql = JdbcUtil.listTablesOrViewsSqlStatement(false);
    Assertions.assertThat(viewSql)
        .isEqualTo("SELECT * FROM iceberg_views WHERE catalog_name = ? AND view_namespace = ?");
  }

  @Test
  public void testRenameTableOrViewSqlStatement() {
    String tableSql = JdbcUtil.renameTableOrViewSqlStatement(true);
    Assertions.assertThat(tableSql)
        .isEqualTo(
            "UPDATE iceberg_tables SET table_namespace = ?, table_name = ?  WHERE catalog_name = ? AND  table_namespace = ? AND table_name = ?");
    String viewSql = JdbcUtil.renameTableOrViewSqlStatement(false);
    Assertions.assertThat(viewSql)
        .isEqualTo(
            "UPDATE iceberg_views SET view_namespace = ?, view_name = ?  WHERE catalog_name = ? AND  view_namespace = ? AND view_name = ?");
  }

  @Test
  public void testDropTableOrViewSqlStatement() {
    String tableSql = JdbcUtil.dropTableOrViewSqlStatement(true);
    Assertions.assertThat(tableSql)
        .isEqualTo(
            "DELETE FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?");
    String viewSql = JdbcUtil.dropTableOrViewSqlStatement(false);
    Assertions.assertThat(viewSql)
        .isEqualTo(
            "DELETE FROM iceberg_views WHERE catalog_name = ? AND view_namespace = ? AND view_name = ?");
  }

  @Test
  public void testDoCommitCreateTableOrViewSqlStatement() {
    String tableSql = JdbcUtil.doCommitCreateTableOrViewSqlStatement(true);
    System.out.println(tableSql);
  }

  @Test
  public void testFilterAndRemovePrefix() {
    Map<String, String> input = Maps.newHashMap();
    input.put("warehouse", "/tmp/warehouse");
    input.put("user", "foo");
    input.put("jdbc.user", "bar");
    input.put("jdbc.pass", "secret");
    input.put("jdbc.jdbc.abcxyz", "abcxyz");

    Properties expected = new Properties();
    expected.put("user", "bar");
    expected.put("pass", "secret");
    expected.put("jdbc.abcxyz", "abcxyz");

    Properties actual = JdbcUtil.filterAndRemovePrefix(input, "jdbc.");

    Assertions.assertThat(expected).isEqualTo(actual);
  }
}
