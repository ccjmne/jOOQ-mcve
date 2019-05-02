package org.jooq.mcve.jsonbinding;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Objects;

import org.jooq.Binding;
import org.jooq.BindingGetResultSetContext;
import org.jooq.BindingGetSQLInputContext;
import org.jooq.BindingGetStatementContext;
import org.jooq.BindingRegisterContext;
import org.jooq.BindingSQLContext;
import org.jooq.BindingSetSQLOutputContext;
import org.jooq.BindingSetStatementContext;
import org.jooq.Converter;
import org.jooq.impl.DSL;

import com.fasterxml.jackson.databind.JsonNode;

@SuppressWarnings("serial")
public class JsonBinder implements Binding<Object, JsonNode> {

  @Override
  public Converter<Object, JsonNode> converter() {
    return new PostgresJSONJacksonJsonNodeConverter();
  }

  @Override
  public void sql(final BindingSQLContext<JsonNode> ctx) throws SQLException {
    ctx.render().visit(DSL.val(ctx.convert(this.converter()).value())).sql("::jsonb");
  }

  @Override
  public void register(final BindingRegisterContext<JsonNode> ctx) throws SQLException {
    ctx.statement().registerOutParameter(ctx.index(), Types.VARCHAR);
  }

  @Override
  public void set(final BindingSetStatementContext<JsonNode> ctx) throws SQLException {
    ctx.statement()
        .setString(ctx.index(), Objects.toString(ctx.convert(this.converter()).value(), null));
  }

  @Override
  public void set(final BindingSetSQLOutputContext<JsonNode> ctx) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void get(final BindingGetResultSetContext<JsonNode> ctx) throws SQLException {
    ctx.convert(this.converter()).value(ctx.resultSet().getString(ctx.index()));
  }

  @Override
  public void get(final BindingGetStatementContext<JsonNode> ctx) throws SQLException {
    ctx.convert(this.converter()).value(ctx.statement().getString(ctx.index()));
  }

  @Override
  public void get(final BindingGetSQLInputContext<JsonNode> ctx) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
