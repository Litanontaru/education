package com.epam.scala

class MyRun {
  def t(): Int = {
    println("t")
    0
  }

  def d(): Int = {
    println("d")
    0
  }

  def r(g: Int): Int = {
    println(g)
    0
  }

  def s(f: () => Int, v: () => Int, g: (() => Int, () => Int) => Int): Int = {
    var o = 0

    def oo() : Int = {
      o += 1
      o
    }

    println("s" + oo)
    g(f, v)
  }

  val r: Int = {
    println("r")
    5
  }

  val f: MG = G

  def run(): Int = s(g, d, _() + _())

  val g: () => Int = G.apply

  class MG {
    var i: Int = 0

    println("M")

    def +(a: Int) = {
      i += a
      this
    }
  }

  object G extends MG {
    def apply(): Int = {
      println("G" + i)
      val tuple: (Int, String) = i.->("5")
      i
    }
  }
}
