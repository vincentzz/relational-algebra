package me.vincentzz.util

object TimeTracker {

  def track[T] (action : => T): (T, Long) = {
    val startAt = System.currentTimeMillis
    val result  = action
    val endAt   = System.currentTimeMillis
    (result, endAt - startAt)
  }
}
