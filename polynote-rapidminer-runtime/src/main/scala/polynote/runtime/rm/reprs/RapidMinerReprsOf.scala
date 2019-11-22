package polynote.runtime.rm.reprs

import java.io.{ByteArrayOutputStream, DataOutput, DataOutputStream}
import java.nio.ByteBuffer

import com.rapidminer.example._
import com.rapidminer.tools.Ontology
import polynote.runtime.rm.reprs.RMOntology.{IAttributeBlock, IAttributeValue}
import polynote.runtime._

import scala.collection.JavaConverters._

trait RapidMinerReprsOf[A] extends ReprsOf[A] {

}

private[reprs] sealed trait LowPriorityRapidMinerReprsOf { self: RapidMinerReprsOf.type =>

  implicit def dataset: RapidMinerReprsOf[ExampleSet]= instance {
    ds => exampleSet(ds)
  }

}

object RMOntology {
  /** [[Ontology.ATTRIBUTE_VALUE_TYPE]] */
  sealed abstract class IAttributeValue extends Product with Serializable

  object IAttributeValue {
    final case object AttributeValue extends IAttributeValue
    sealed trait INominal extends IAttributeValue
    final case object Nominal extends INominal
    sealed trait INumerical extends IAttributeValue
    final case object Numerical extends INumerical
    sealed trait IDateTime extends IAttributeValue
    final case object DateTime extends IDateTime
    final case object Integer extends INumerical
    final case object Real extends INumerical
    sealed trait IString extends INominal
    final case object String extends IString
    final case object Binominal extends INominal
    final case object Polynominal extends INominal
    final case object FilePath extends IString
    final case object Date extends IDateTime
    final case object Time extends IDateTime
    def apply(value: Int): IAttributeValue = value match {
      case Ontology.ATTRIBUTE_VALUE => AttributeValue
      case Ontology.NOMINAL => Nominal
      case Ontology.NUMERICAL => Numerical
      case Ontology.DATE_TIME => DateTime
      case Ontology.INTEGER => Integer
      case Ontology.REAL => Real
      case Ontology.STRING => String
      case Ontology.BINOMINAL => Binominal
      case Ontology.POLYNOMINAL => Polynominal
      case Ontology.FILE_PATH => FilePath
      case Ontology.DATE => Date
      case Ontology.TIME => Time
    }
  }

  /** [[Ontology.ATTRIBUTE_BLOCK_TYPE]]  */
  sealed abstract class IAttributeBlock extends Product with Serializable

  object IAttributeBlock {
    final case object AttributeBlock extends IAttributeBlock
    final case object SingleValue extends IAttributeBlock
    sealed trait IValueSeries extends IAttributeBlock
    final case object ValueSeries extends IAttributeBlock
    final case object ValueSeriesStart extends IValueSeries
    final case object ValueSeriesEnd extends IValueSeries
    sealed trait IValueMatrix extends IAttributeBlock
    final case object ValueMatrix extends IAttributeBlock
    final case object ValueMatrixStart extends IValueMatrix
    final case object ValueMatrixEnd extends IValueMatrix
    final case object ValueMatrixRowStart extends IValueMatrix

    def apply(value: Int): IAttributeBlock = value match {
      case Ontology.ATTRIBUTE_BLOCK => AttributeBlock
      case Ontology.SINGLE_VALUE => SingleValue
      case Ontology.VALUE_SERIES => ValueSeries
      case Ontology.VALUE_SERIES_START => ValueSeriesStart
      case Ontology.VALUE_SERIES_END => ValueSeriesEnd
      case Ontology.VALUE_MATRIX => ValueMatrix
      case Ontology.VALUE_MATRIX_START => ValueMatrixStart
      case Ontology.VALUE_MATRIX_END => ValueMatrixEnd
      case Ontology.VALUE_MATRIX_ROW_START => ValueMatrixRowStart
    }
  }

}

object RapidMinerReprsOf extends LowPriorityRapidMinerReprsOf {

  private def dataTypeAndEncoder(dataType: Attribute, nullable: Boolean): Option[(DataType, DataOutput => Example => Int => Unit)] =
    if (nullable) {
      dataTypeAndEncoder(dataType, nullable = false).map {
        case (dt, encode) => OptionalType(dt) -> {
          out => {
            val encodeUnderlying = encode(out)
            row => index => if (row.getValue(dataType).isNaN) out.writeBoolean(false) else {
              out.writeBoolean(true)
              encodeUnderlying(row)(index)
            }
          }
        }
      }

    } else
    Option((IAttributeValue(dataType.getValueType), IAttributeBlock(dataType.getBlockType))).collect {
      case (IAttributeValue.AttributeValue, IAttributeBlock.SingleValue)  => StringType -> (out => row => index => DataEncoder.string.encode(out, row.getValueAsString(dataType)))
      case (IAttributeValue.Binominal, IAttributeBlock.SingleValue) => BoolType -> (out => row => index => DataEncoder.boolean.encode(out, dataType.getValue(row.getDataRow) == 1d))
      case (IAttributeValue.Integer, IAttributeBlock.SingleValue)   => IntType -> (out => row => index => DataEncoder.int.encode(out, row.getNumericalValue(dataType).toInt))
      case (_:IAttributeValue.INumerical, IAttributeBlock.SingleValue)   => DoubleType -> (out => row => index => DataEncoder.double.encode(out, row.getNumericalValue(dataType)))
//      case (IAttributeValue.Binominal, IAttributeBlock.ValueSeries)  => BinaryType -> (out => row => index => DataEncoder.byteArray.encode(out, dataType.getValue(row.getDataRow).))
      case (_:IAttributeValue.INominal, IAttributeBlock.SingleValue)  => StringType -> (out => row => index => DataEncoder.string.encode(out, row.getNominalValue(dataType)))

    }

  private def structDataTypeAndEncoder(schema: Attributes): (StructType, (DataOutput, Example) => Unit) = {
    val (fieldTypes, fieldEncoders) = schema.allAttributes.asScala.toList.zipWithIndex.flatMap {
      case (attr: Attribute, index) =>
        dataTypeAndEncoder(attr, attr.getAllStatistics.asScala.collectFirst { case us: UnknownStatistics => us }.forall(us => us.getStatistics(attr, Statistics.UNKNOWN, "") == 0d)).map {
          case (dt, enc) => StructField(attr.getName, dt) -> ((out: DataOutput, row: Example) => enc(out)(row)(index))
        }
    }.unzip

    StructType(fieldTypes) -> {
      (out, row) =>
        fieldEncoders.foreach(_.apply(out, row))
    }
  }

  private[polynote] class FixedSizeDataFrameDecoder(structType: StructType, encode: (DataOutput, Example) => Unit) extends (Example => Array[Byte]) with Serializable {
    assert(structType.size >= 0)
    override def apply(row: Example): Array[Byte] = {
      // TODO: share/reuse this buffer? Any reason to be threadsafe?
      val arr = new Array[Byte](structType.size)
      val buf = ByteBuffer.wrap(arr)
      encode(new DataEncoder.BufferOutput(buf), row)
      arr
    }
  }

  private[polynote] class VariableSizeDataFrameDecoder(structType: StructType, encode: (DataOutput, Example) => Unit) extends (Example => Array[Byte]) with Serializable {
    override def apply(row: Example): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      try {
        encode(new DataOutputStream(out), row)
        out.toByteArray
      } finally {
        out.close()
      }
    }
  }


  private[polynote] class ExampleSetHandle(
                                            val handle: Int,
                                            exampleSet: ExampleSet
                                         ) extends StreamingDataRepr.Handle with Serializable {

    private val (structType, encode) = structDataTypeAndEncoder(exampleSet.getAttributes)


    val dataType: DataType = structType
    val knownSize: Option[Int] = None

    // TODO: It might be nice to iterate instead of collect, but maybe in a better way than toLocalIterator...
    private lazy val collectedData = {
      val rowToBytes: Example => Array[Byte] = if (structType.size >= 0) {
        new FixedSizeDataFrameDecoder(structType, encode)
      } else {
        new VariableSizeDataFrameDecoder(structType, encode)
      }

      exampleSet.asScala.take(1000000).map(rowToBytes)
    }

    def iterator: Iterator[ByteBuffer] = collectedData.iterator.map(ByteBuffer.wrap)


    def modify(ops: List[TableOp]): Either[Throwable, Int => StreamingDataRepr.Handle] = {


      @inline def tryEither[T](thunk: => T): Either[Throwable, T] = try Right(thunk) catch {
        case err: Throwable => Left(err)
      }

      ops.foldLeft(tryEither(exampleSet)) {
        (dfOrErr, op) => dfOrErr.right.flatMap {
          df => op match {
            case GroupAgg(cols, aggs) => ???
            case QuantileBin(column, binCount, err) => ???

            case Select(columns) => tryEither{
              val table = df.getExampleTable
              table.getAttributes.filterNot(attr => columns.contains(attr.getName)).foreach(table.removeAttribute)
              table.createExampleSet()
            }
          }
        }
      }.right.map {
        modifiedDataFrame => new ExampleSetHandle(_, modifiedDataFrame)
      }
    }

    override def release(): Unit = {
      exampleSet.cleanup()
      super.release()
    }
  }

  def instance[T](reprs: T => Array[ValueRepr]): RapidMinerReprsOf[T] = new RapidMinerReprsOf[T] {
    def apply(value: T): Array[ValueRepr] = reprs(value)
  }

  implicit val exampleSet: RapidMinerReprsOf[ExampleSet] = {
    instance {
      df => Array(StreamingDataRepr.fromHandle(new ExampleSetHandle(_, df)))
    }
  }


}


