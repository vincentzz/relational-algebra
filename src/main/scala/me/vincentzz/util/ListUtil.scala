package me.vincentzz.util

import scala.annotation.tailrec

object ListUtil {

  def containsDups[A](list: List[A]): Boolean = containsDups_(list, Set.empty)

  @tailrec
  def containsDups_[A](list: List[A], seen: Set[A]): Boolean = list match {
    case x :: xs => if (seen.contains(x)) true else containsDups_(xs, seen + x)
    case _       => false
  }

  def allElemEqs[A](list: List[A]): Boolean = allElemEqs_(list, list.head)

  @tailrec
  def allElemEqs_[A](list: List[A], v: A): Boolean = list match {
    case x :: xs => x.equals(v) && allElemEqs_(xs, v)
    case _       => true
  }
}