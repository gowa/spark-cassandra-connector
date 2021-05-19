package com.datastax.spark.connector

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

import com.datastax.driver.core.{UDTValue => DriverUDTValue}
import com.datastax.spark.connector.types.NullableTypeConverter

final case class UDTValue(metaData: CassandraRowMetadata, columnValues: IndexedSeq[AnyRef])
  extends ScalaGettableData {

  def this(columnNames: IndexedSeq[String], columnValues: IndexedSeq[AnyRef]) =
    this(CassandraRowMetadata.fromColumnNames(columnNames), columnValues)

  def columnNames: IndexedSeq[String] = metaData.columnNames

  override def productArity: Int = columnValues.size
  override def productElement(i: Int) = columnValues(i)

  def unapply(t: UDTValue): Some[(IndexedSeq[String],IndexedSeq[AnyRef])] =
    Some((t.metaData.columnNames,t.columnValues))
}

object UDTValue {

  def fromJavaDriverUDTValue(value: DriverUDTValue, metadata: Option[CassandraRowMetadata] = None): UDTValue = {
    val fields = metadata match {
      case Some(m) => m.columnNames
      case None => value.getType.getFieldNames.toIndexedSeq
    }

    val values = fields.map{ field => GettableData.get(value, field, GettableData.getColumnMetadata(field, metadata)) }

    metadata match {
      case Some(m) => UDTValue(m, values)
      case None => UDTValue(fields, values)
    }
  }

  def fromMap(map: Map[String, Any]): UDTValue =
    new UDTValue(map.keys.toIndexedSeq, map.values.map(_.asInstanceOf[AnyRef]).toIndexedSeq)

  val TypeTag = implicitly[TypeTag[UDTValue]]
  val Symbol = typeOf[UDTValue].asInstanceOf[TypeRef].sym

  implicit object UDTValueConverter extends NullableTypeConverter[UDTValue] {
    def targetTypeTag = TypeTag
    def convertPF = {
      case x: UDTValue => x
    }
  }

  def apply(columnNames: IndexedSeq[String], columnValues: IndexedSeq[AnyRef]): UDTValue =
    new UDTValue(columnNames, columnValues)
}
