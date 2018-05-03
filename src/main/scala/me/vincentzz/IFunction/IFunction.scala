package me.vincentzz.IFunction

case class IFunction (func: AnyRef){
  def apply(params: List[Any]):Any = params match {
    case Nil    => func.asInstanceOf[Any]
    case p::Nil => func.asInstanceOf[Any=>Any](p)
    case p::ps  => IFunction(func.asInstanceOf[Any=>Any](p).asInstanceOf[AnyRef])(ps)
  }
}
