package me.vincentzz.relational.function

case class IFunction (func: AnyRef){
  def apply(params: List[_]):Any = params match {
    case p::ps  => IFunction(func.asInstanceOf[Any=>Any](p).asInstanceOf[AnyRef])(ps)
    case Nil    => func.asInstanceOf[Any]
  }
}
