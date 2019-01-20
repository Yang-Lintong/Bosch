package com.bosch.test

import org.apache.spark.sql.DataFrame

import scala.util.control.Breaks


/**
  * Created by saschavetter on 06/07/16.
  */

object IsbnEncoder {
  implicit def dmcEncoder(df: DataFrame) = new IsbnEncoderImplicit(df)
}

class IsbnEncoderImplicit(df: DataFrame) extends Serializable {

  /**
    * Creates a new row for each element of the ISBN code
    *
    * @return a data frame with new rows for each element of the ISBN code
    */
  def explodeIsbn(): DataFrame = {
    // new column with a Array and explode it
    val handleIsbnFrame = df.explode("isbn", "new_isbn") { (line: String) => handleIsbn(line) }
    // change col name
    val dataFrame = handleIsbnFrame.select("name", "year", "new_isbn").toDF("name", "year", "isbn")
    //dataFrame.show()
    dataFrame
  }

  def handleIsbn(line: String): Array[String] = {
    // isbn code rule
    val ruleItem1 = RuleItem("1", 2, "4", 4, "8", 3)
    val ruleArray = Array(ruleItem1)

    val code = line.replaceAll("-", "").replaceAll(" ", "").replaceAll("ISBN:", "")
    if (line.startsWith("ISBN:") && code.startsWith("978") && checkoutCode(code)) {
      val eanString = "ISBN-EAN: 978"
      val groupString: StringBuffer = new StringBuffer("ISBN-GROUP: ")
      val publisherString: StringBuffer = new StringBuffer("ISBN-PUBLISHER: ")
      val titleString: StringBuffer = new StringBuffer("ISBN-TITLE: ")
      val loop = new Breaks
      loop.breakable {
        for (ruleItem <- ruleArray) {
          var needContinue = true

          val groupHeadCode = code.substring(3)
          var groupCode: String = ""
          if (groupHeadCode.startsWith(ruleItem.groupStr)) {
            groupCode = groupHeadCode.substring(0, ruleItem.groupLen)
          } else {
            needContinue = false
          }

          val publisherHeadCode = groupHeadCode.substring(ruleItem.groupLen)
          var publisherCode = ""
          if (publisherHeadCode.startsWith(ruleItem.publisherStr) && needContinue) {
            publisherCode = publisherHeadCode.substring(0, ruleItem.publisherLen)
          } else {
            needContinue = false
          }

          val titleHeadCode = publisherHeadCode.substring(ruleItem.publisherLen)
          var titleCode = ""
          if (titleHeadCode.startsWith(ruleItem.titleStr) && needContinue) {
            titleCode = titleHeadCode.substring(0, ruleItem.titleLen)
          } else {
            needContinue = false
          }

          if (needContinue) {
            groupString.append(groupCode)
            publisherString.append(publisherCode)
            titleString.append(titleCode)
            loop.break()
          }
        }
      }
      Array(line, eanString, groupString.toString, publisherString.toString, titleString.toString)
    } else {
      Array(line)
    }
  }

  def checkoutCode(str: String): Boolean = {
    var sum = 0
    var index = 0
    for (number <- str) {
      val currentNumber = number - '0'
      if (currentNumber > 9) {
        return false
      }
      if (index % 2 == 0) {
        sum += currentNumber
      } else {
        sum += currentNumber * 3
      }
      index += 1
    }
    if (sum % 10 == 0) {
      true
    } else {
      false
    }
  }

  case class RuleItem(groupStr: String, groupLen: Int, publisherStr: String, publisherLen: Int, titleStr: String, titleLen: Int)

}
