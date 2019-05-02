package org.jooq.mcve.jsonbinding;

import java.io.IOException;

import org.jooq.Converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

@SuppressWarnings("serial")
public class PostgresJSONJacksonJsonNodeConverter implements Converter<Object, JsonNode> {

  private final ObjectMapper mapper;

  public PostgresJSONJacksonJsonNodeConverter() {
    this.mapper = new ObjectMapper();
  }

  @Override
  public JsonNode from(final Object t) {
    try {
      return t == null ? NullNode.instance : this.mapper.readTree(t.toString());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object to(final JsonNode u) {
    try {
      return (u == null) || u.equals(NullNode.instance) ? null : this.mapper.writeValueAsString(u);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<Object> fromType() {
    return Object.class;
  }

  @Override
  public Class<JsonNode> toType() {
    return JsonNode.class;
  }
}
