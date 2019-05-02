/*-
 * This work is dual-licensed
 * - under the Apache Software License 2.0 (the "ASL")
 * - under the jOOQ License and Maintenance Agreement (the "jOOQ License")
 * =============================================================================
 * You may choose which license applies to you:
 *
 * - If you're using this work with Open Source databases, you may choose
 *   either ASL or jOOQ License.
 * - If you're using this work with at least one commercial database, you must
 *   choose jOOQ License
 *
 * For more information, please visit http://www.jooq.org/licenses
 *
 * Apache Software License 2.0:
 * -----------------------------------------------------------------------------
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * jOOQ License and Maintenance Agreement:
 * -----------------------------------------------------------------------------
 * Data Geekery grants the Customer the non-exclusive, timely limited and
 * non-transferable license to install and use the Software under the terms of
 * the jOOQ License and Maintenance Agreement.
 *
 * This library is distributed with a LIMITED WARRANTY. See the jOOQ License
 * and Maintenance Agreement for more details: http://www.jooq.org/licensing
 */
package org.jooq.mcve.test;

import static org.jooq.mcve.Tables.TEST;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.mcve.jsonbinding.PostgresJSONJacksonJsonNodeConverter;
import org.jooq.mcve.tables.records.TestRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MCVETest {

  public static final DataType<JsonNode> JSON_TYPE     = SQLDataType.VARCHAR.asConvertedDataType(new PostgresJSONJacksonJsonNodeConverter());
  private static final ObjectMapper      OBJECT_MAPPER = new ObjectMapper();

  private Connection connection;
  private DSLContext ctx;

  @Before
  public void setup() throws Exception {
    this.connection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/jooq-test", "postgres", "postgrespwd");
    this.ctx = DSL.using(this.connection);
    this.ctx.truncate(TEST).restartIdentity().execute();
  }

  @After
  public void after() throws Exception {
    this.ctx = null;
    this.connection.close();
    this.connection = null;
  }

  @Test
  public void mcveTest() throws IOException {
    this.ctx.insertInto(TEST).columns(TEST.ID, TEST.VALUE).values(Integer.valueOf(1), OBJECT_MAPPER.readTree("\n" +
        "{\n" +
        "  \"age\": 35,\n" +
        "  \"eyeColor\": \"brown\",\n" +
        "  \"name\": {\n" +
        "    \"first\": \"Alice\",\n" +
        "    \"last\": \"Smith\"\n" +
        "  }\n" +
        "}\n")).execute();

    // assertions OK
    final TestRecord record = this.ctx.selectFrom(TEST).where(TEST.ID.eq(Integer.valueOf(1))).fetchOne();
    Assert.assertEquals(35, record.get(TEST.VALUE).get("age").asInt());
    Assert.assertEquals("Alice", record.get(TEST.VALUE).get("name").get("first").asText());

    this.ctx.update(TEST).set(TEST.VALUE, OBJECT_MAPPER.readTree("\n" +
        "{\n" +
        "  \"age\": 35,\n" +
        "  \"name\": {\n" +
        "    \"first\": \"Bob\",\n" +
        "    \"last\": \"Smith\"\n" +
        "  }\n" +
        "}\n")).where(TEST.ID.eq(Integer.valueOf(1))).execute();

    // assertion OK
    record.refresh();
    Assert.assertEquals("Bob", record.get(TEST.VALUE).get("name").get("first").asText());

    // FAILS TO EXECUTE

    /*-
     * UPDATE test
     *     SET test.value = jsonb_set(test.value, '{name}', '{"first": "Christian", "last": "Smith"}')
     *     WHERE test.id = 1
     */
    this.ctx.update(TEST).set(TEST.VALUE, MCVETest.setByKey(TEST.VALUE, "name", OBJECT_MAPPER.readTree("\n" +
        "{\n" +
        "    \"first\": \"Christian\",\n" +
        "    \"last\": \"Smith\"\n" +
        "}\n"))).where(TEST.ID.eq(Integer.valueOf(1))).execute();

    record.refresh();
    Assert.assertEquals("Christian", record.get(TEST.VALUE).get("name").get("first").asText());
  }

  /**
   * Set JSON object value at key (function: {@code jsonb_set}).
   *
   * @param field
   *          The JSON field to be updated
   * @param key
   *          The key to be assigned the specified {@code value}
   * @param value
   *          The value to set for the given {@code key}
   * @return A new {@code Field<JsonNode>} with the updated key-value pair
   */
  public static Field<JsonNode> setByKey(final Field<JsonNode> field, final String key, final JsonNode value) {
    return DSL.field("jsonb_set({0}, {1}, {2})", JsonNode.class, field, DSL.array(key), value);
  }
}
