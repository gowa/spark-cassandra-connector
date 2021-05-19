package com.datastax.spark.connector.rdd.reader

import com.datastax.driver.core.Row
import com.datastax.spark.connector.{CassandraRowMetadata, GettableData}
import com.datastax.spark.connector.types.TypeConverter

import scala.reflect.ClassTag

// The below fragment may look very repetitive and copy-pasted,
// however there is no other way to code this for functions of different arity
// while preserving good type-safety.

trait FunctionBasedRowReader[R] extends RowReader[R] with ThisRowReaderAsFactory[R] {
  def ct: ClassTag[R]
  override val targetClass: Class[R] = ct.runtimeClass.asInstanceOf[Class[R]]
  override def neededColumns = None
}

class FunctionBasedRowReader1[R, A0](f: A0 => R)(
  implicit a0c: TypeConverter[A0], @transient override val ct: ClassTag[R]) extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) =
    f(a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, Some(rowMetaData)))))

}

class FunctionBasedRowReader2[R, A0, A1](f: (A0, A1) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata)))
    )
  }
}

class FunctionBasedRowReader3[R, A0, A1, A2](f: (A0, A1, A2) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2), GettableData.getColumnMetadata(2, metadata))))
  }

}

class FunctionBasedRowReader4[R, A0, A1, A2, A3](f: (A0, A1, A2, A3) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2), GettableData.getColumnMetadata(2, metadata))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3), GettableData.getColumnMetadata(3, metadata)))
    )
  }
}

class FunctionBasedRowReader5[R, A0, A1, A2, A3, A4](f: (A0, A1, A2, A3, A4) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2), GettableData.getColumnMetadata(2, metadata))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3), GettableData.getColumnMetadata(3, metadata))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4), GettableData.getColumnMetadata(4, metadata)))
    )
  }
}

class FunctionBasedRowReader6[R, A0, A1, A2, A3, A4, A5](f: (A0, A1, A2, A3, A4, A5) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2), GettableData.getColumnMetadata(2, metadata))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3), GettableData.getColumnMetadata(3, metadata))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4), GettableData.getColumnMetadata(4, metadata))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5), GettableData.getColumnMetadata(5, metadata)))
    )
  }
}

class FunctionBasedRowReader7[R, A0, A1, A2, A3, A4, A5, A6](f: (A0, A1, A2, A3, A4, A5, A6) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2), GettableData.getColumnMetadata(2, metadata))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3), GettableData.getColumnMetadata(3, metadata))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4), GettableData.getColumnMetadata(4, metadata))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5), GettableData.getColumnMetadata(5, metadata))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6), GettableData.getColumnMetadata(6, metadata)))
    )
  }
}

class FunctionBasedRowReader8[R, A0, A1, A2, A3, A4, A5, A6, A7]
(f: (A0, A1, A2, A3, A4, A5, A6, A7) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2), GettableData.getColumnMetadata(2, metadata))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3), GettableData.getColumnMetadata(3, metadata))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4), GettableData.getColumnMetadata(4, metadata))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5), GettableData.getColumnMetadata(5, metadata))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6), GettableData.getColumnMetadata(6, metadata))),
      a7c.convert(GettableData.get(row, 7, rowMetaData.codecs(7), GettableData.getColumnMetadata(7, metadata)))
    )
  }
}

class FunctionBasedRowReader9[R, A0, A1, A2, A3, A4, A5, A6, A7, A8]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2), GettableData.getColumnMetadata(2, metadata))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3), GettableData.getColumnMetadata(3, metadata))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4), GettableData.getColumnMetadata(4, metadata))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5), GettableData.getColumnMetadata(5, metadata))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6), GettableData.getColumnMetadata(6, metadata))),
      a7c.convert(GettableData.get(row, 7, rowMetaData.codecs(7), GettableData.getColumnMetadata(7, metadata))),
      a8c.convert(GettableData.get(row, 8, rowMetaData.codecs(8), GettableData.getColumnMetadata(8, metadata)))
    )
  }
}

class FunctionBasedRowReader10[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  a9c: TypeConverter[A9],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2), GettableData.getColumnMetadata(2, metadata))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3), GettableData.getColumnMetadata(3, metadata))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4), GettableData.getColumnMetadata(4, metadata))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5), GettableData.getColumnMetadata(5, metadata))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6), GettableData.getColumnMetadata(6, metadata))),
      a7c.convert(GettableData.get(row, 7, rowMetaData.codecs(7), GettableData.getColumnMetadata(7, metadata))),
      a8c.convert(GettableData.get(row, 8, rowMetaData.codecs(8), GettableData.getColumnMetadata(8, metadata))),
      a9c.convert(GettableData.get(row, 9, rowMetaData.codecs(9), GettableData.getColumnMetadata(9, metadata)))
    )
  }
}

class FunctionBasedRowReader11[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  a9c: TypeConverter[A9],
  a10c: TypeConverter[A10],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2), GettableData.getColumnMetadata(2, metadata))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3), GettableData.getColumnMetadata(3, metadata))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4), GettableData.getColumnMetadata(4, metadata))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5), GettableData.getColumnMetadata(5, metadata))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6), GettableData.getColumnMetadata(6, metadata))),
      a7c.convert(GettableData.get(row, 7, rowMetaData.codecs(7), GettableData.getColumnMetadata(7, metadata))),
      a8c.convert(GettableData.get(row, 8, rowMetaData.codecs(8), GettableData.getColumnMetadata(8, metadata))),
      a9c.convert(GettableData.get(row, 9, rowMetaData.codecs(9), GettableData.getColumnMetadata(9, metadata))),
      a10c.convert(GettableData.get(row, 10, rowMetaData.codecs(10), GettableData.getColumnMetadata(10, metadata)))
    )
  }
}

class FunctionBasedRowReader12[R, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11]
(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R)(
  implicit
  a0c: TypeConverter[A0],
  a1c: TypeConverter[A1],
  a2c: TypeConverter[A2],
  a3c: TypeConverter[A3],
  a4c: TypeConverter[A4],
  a5c: TypeConverter[A5],
  a6c: TypeConverter[A6],
  a7c: TypeConverter[A7],
  a8c: TypeConverter[A8],
  a9c: TypeConverter[A9],
  a10c: TypeConverter[A10],
  a11c: TypeConverter[A11],
  @transient override val ct: ClassTag[R])
  extends FunctionBasedRowReader[R] {

  override def read(row: Row, rowMetaData: CassandraRowMetadata) = {
    val metadata = Some(rowMetaData)
    f(
      a0c.convert(GettableData.get(row, 0, rowMetaData.codecs(0), GettableData.getColumnMetadata(0, metadata))),
      a1c.convert(GettableData.get(row, 1, rowMetaData.codecs(1), GettableData.getColumnMetadata(1, metadata))),
      a2c.convert(GettableData.get(row, 2, rowMetaData.codecs(2), GettableData.getColumnMetadata(2, metadata))),
      a3c.convert(GettableData.get(row, 3, rowMetaData.codecs(3), GettableData.getColumnMetadata(3, metadata))),
      a4c.convert(GettableData.get(row, 4, rowMetaData.codecs(4), GettableData.getColumnMetadata(4, metadata))),
      a5c.convert(GettableData.get(row, 5, rowMetaData.codecs(5), GettableData.getColumnMetadata(5, metadata))),
      a6c.convert(GettableData.get(row, 6, rowMetaData.codecs(6), GettableData.getColumnMetadata(6, metadata))),
      a7c.convert(GettableData.get(row, 7, rowMetaData.codecs(7), GettableData.getColumnMetadata(7, metadata))),
      a8c.convert(GettableData.get(row, 8, rowMetaData.codecs(8), GettableData.getColumnMetadata(8, metadata))),
      a9c.convert(GettableData.get(row, 9, rowMetaData.codecs(9), GettableData.getColumnMetadata(9, metadata))),
      a10c.convert(GettableData.get(row, 10, rowMetaData.codecs(10), GettableData.getColumnMetadata(10, metadata))),
      a11c.convert(GettableData.get(row, 11, rowMetaData.codecs(11), GettableData.getColumnMetadata(11, metadata)))
    )
  }
}

