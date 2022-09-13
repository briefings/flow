package com.grey.functions

import org.apache.spark.sql.types.StructType

object ScalaCaseClass {

  private val scalaDataType = new ScalaDataType()

  def scalaCaseClass(schema: StructType): String = {

    val definitions: Seq[String] = schema.map{ variable =>
      variable.name + ": " + scalaDataType.scalaDataType(dataTypeOfVariable = variable.dataType)
    }

    s"""
       |case class CaseClass (${definitions.mkString(", ")})
     """.stripMargin

  }

}
