package polynote.kernel.interpreter

import polynote.kernel.interpreter.scal.ScalaRapidMinerInterpreter

class RapidMinerInterpreters extends Loader {
  override def factories: Map[String, Interpreter.Factory] = Map(
    "rapidminer" -> ScalaRapidMinerInterpreter.Factory
  )
}
