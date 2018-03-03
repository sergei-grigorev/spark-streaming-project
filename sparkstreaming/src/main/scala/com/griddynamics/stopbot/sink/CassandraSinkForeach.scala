package com.griddynamics.stopbot.sink

import java.net.InetAddress

import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import org.apache.spark.sql.{ ForeachWriter, Row }

class CassandraSinkForeach(servers: Set[String], keyspace: String, table: String, columns: Array[String], ttl: Int) extends ForeachWriter[Row] {

  /* Cassandra connector */
  val pool =
    CassandraConnector(
      hosts = servers.map(r => InetAddress.getByName(r.trim)))

  /* we support duplicates and don't have to check is a partition was saved */
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(row: Row): Unit = {
    pool.withSessionDo { session: Session =>
      val objects = row.toSeq.asInstanceOf[Seq[AnyRef]]

      session.execute(
        QueryBuilder.insertInto(keyspace, table)
          .values(columns, objects.toArray)
          .using(QueryBuilder.ttl(ttl)))
    }
  }

  /* how we can close it manually ? */
  override def close(errorOrNull: Throwable): Unit = {}
}
