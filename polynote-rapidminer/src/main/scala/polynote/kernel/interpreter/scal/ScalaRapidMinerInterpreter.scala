package polynote.kernel.interpreter.scal

import com.rapidminer.{RapidMiner, Process => RMProcess}
import com.rapidminer.RapidMiner.ExecutionMode
import com.rapidminer.example.ExampleSet
import polynote.kernel.environment.{CurrentNotebook, CurrentTask}
import polynote.kernel.interpreter.{Interpreter, State}
import polynote.kernel.{BaseEnv, Completion, GlobalEnv, InterpreterEnv, ResultValue, ScalaCompiler, Signatures, TaskManager, ValueReprCodec}
import polynote.messages.CellID
import polynote.runtime.{BoolType, DataRepr, StreamingDataRepr, StringRepr}
import polynote.runtime.rm.reprs.{RMOntology, RapidMinerReprsOf}

import scala.collection.JavaConverters._
import zio.{RIO, Task, ZIO}

import scala.reflect.runtime.universe._

class ScalaRapidMinerInterpreter private[scal]() extends Interpreter {
  import ScalaRapidMinerInterpreter._
  /**
    * Run the given code in the given [[State]], returning an updated [[State]].
    *
    * @param code  The code string to be executed
    * @param state The given [[State]] will have the Cell ID of the cell containing the given code, and it will
    *              initially have empty values. Its `prev` will point to the [[State]] returned by the closes prior
    *              executed cell, or to [[State.Root]] if there is no such cell.
    */
  override def run(code: String, state: State): RIO[InterpreterEnv, State] = Task {
    val process = new RMProcess(code)
    val exampleSets = process.run().asList
    RapidMinerCellState(state.id, state.prev,
      exampleSets.asScala.toList.collect{case exampleSet: ExampleSet => ResultValue(s"${exampleSet.getName}ExampleSet", "ExampleSet", RapidMinerReprsOf.exampleSet(exampleSet).toList, state.id, exampleSet, typeOf[ExampleSet], None)}
//      exampleSet.getAttributes.asScala.toList.map{
//      case attr => ResultValue(attr.getName, "String", List(StringRepr(s"${attr.getName} repr")), state.id, s"${attr.getName} attr", scala.reflect.runtime.universe.NoType, None)
//    }
      /*List(ResultValue(exampleSet.getName, "ExampleSet", exampleSet.getAttributes.asScala.toList.map(a => {
      println(s"attribute: $a")
      StringRepr(a.getName)
    }/*RMOntology.IAttributeValue(a.getValueType) match {
      case RMOntology.IAttributeValue.String => StringRepr(a.getName)
    }*/), state.id, (), scala.reflect.runtime.universe.NoType, None))*/)
  }

  /**
    * Ask for completions (if applicable) at the given position in the given code string.
    *
    * @param code  The code string in which completions are requested
    * @param pos   The position within the code string at which completions are requested
    * @param state The given [[State]] will have the Cell ID of the cell containing the given code, and it will
    *              initially have empty values. Its `prev` will point to the [[State]] returned by the closes prior
    *              executed cell, or to [[State.Root]] if there is no such cell.
    */
  override def completionsAt(code: String, pos: Int, state: State): Task[List[Completion]] = Task {
    List.empty
  }

  /**
    * Ask for parameter hints (if applicable) at the given position in the given code string.
    *
    * @param code  The code string in which parameter hints are requested
    * @param pos   The position within the code string at which parameter hints are requested
    * @param state The given [[State]] will have the Cell ID of the cell containing the given code, and it will
    *              initially have empty values. Its `prev` will point to the [[State]] returned by the closes prior
    *              executed cell, or to [[State.Root]] if there is no such cell.
    */
  override def parametersAt(code: String, pos: Int, state: State): Task[Option[Signatures]] = Task {
    None
  }

  /**
    * Initialize the interpreter, running any predef code and setting up an initial state.
    *
    * @param state A [[State]] which is the current state of the notebook execution.
    * @return An initial state for this interpreter
    */
  override def init(state: State): RIO[InterpreterEnv, State] = ZIO.effectTotal{
    RapidMiner.setExecutionMode(ExecutionMode.EMBEDDED_WITHOUT_UI)
    RapidMiner.init()
    state
  }

  /**
    * Shut down this interpreter, releasing its resources and ending any internally managed tasks or processes
    */
  override def shutdown(): Task[Unit] = Task {
    RapidMiner.quit(RapidMiner.ExitMode.NORMAL)
  }
}

object ScalaRapidMinerInterpreter {
  case class RapidMinerCellState(id: CellID, prev: State, values: List[ResultValue]) extends State {
    override def withPrev(prev: State): RapidMinerCellState = copy(prev = prev)

    override def updateValues(fn: ResultValue => ResultValue): State = copy(values = values.map(fn))

    override def updateValuesM[R](fn: ResultValue => RIO[R, ResultValue]): RIO[R, State] =
      ZIO.sequence(values.map(fn)).map(values => copy(values = values))
  }

  object Factory extends Interpreter.Factory {
    override def languageName: String = "RapidMiner"

    override def apply(): RIO[BaseEnv with GlobalEnv with ScalaCompiler.Provider with CurrentNotebook with CurrentTask with TaskManager, Interpreter] =
      ZIO.succeed{new ScalaRapidMinerInterpreter()}
  }
}


