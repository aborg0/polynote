package polynote.kernel.interpreter.scal

import com.rapidminer.RapidMiner
import polynote.kernel.environment.{CurrentNotebook, CurrentTask}
import polynote.kernel.interpreter.{Interpreter, State}
import polynote.kernel.{BaseEnv, Completion, GlobalEnv, InterpreterEnv, ScalaCompiler, Signatures, TaskManager}
import zio.{RIO, Task, ZIO}

class ScalaRapidMinerInterpreter private[scal]() extends Interpreter {
  /**
    * Run the given code in the given [[State]], returning an updated [[State]].
    *
    * @param code  The code string to be executed
    * @param state The given [[State]] will have the Cell ID of the cell containing the given code, and it will
    *              initially have empty values. Its `prev` will point to the [[State]] returned by the closes prior
    *              executed cell, or to [[State.Root]] if there is no such cell.
    */
  override def run(code: String, state: State): RIO[InterpreterEnv, State] = ???

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
  override def init(state: State): RIO[InterpreterEnv, State] = ZIO.succeed{
    RapidMiner.init()
//    RapidMiner.hideSplash()
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
  object Factory extends Interpreter.Factory {
    override def languageName: String = "RapidMiner"

    override def apply(): RIO[BaseEnv with GlobalEnv with ScalaCompiler.Provider with CurrentNotebook with CurrentTask with TaskManager, Interpreter] =
      ZIO.succeed{new ScalaRapidMinerInterpreter()}
  }
}


