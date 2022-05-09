
class ParserHelper {

  /*
* Parse String to double. If invalid parsing, than return 0
 */
  def parseDouble(s: String): Double = try { s.toDouble } catch { case _ => 0 }

  /*
* Parse String to Long. If invalid parsing, than return 0
*/
  def parseLong(s: String): Long = try { s.toLong } catch { case _ => 0 }

  /*
 * Parse String to String. If invalid parsing, than return 0
  */
  def parseString(s: Any): String = try { s.toString } catch { case _ => "" }

  /*
  * Calculate euro price given a string in dolars
   */
  def getEuroPrice(s: String): Double =  try {
    val price = s.replace("$","").toDouble
    //return only two decimals
    ((price*0.9) * 100).round / 100.toDouble
  } catch { case _ => 0 }

  /*
  * Parse String to Date format
   */
  def StringToDate(s:String): String = try {
    val splitResult = s.split(" ")
    val month = splitResult(0).toString().toLowerCase() match {
      case "january" => "01"
      case "february" => "02"
      case "march" => "03"
      case "april" => "04"
      case "may" => "05"
      case "june" => "06"
      case "july" => "07"
      case "august" => "08"
      case "september" => "09"
      case "october" => "10"
      case "november" => "11"
      case "december" => "12"
    }
    var day = splitResult(1).dropRight(1)
    if (day.length == 1) {
      day = "0".concat(day)
    }
    val year =  splitResult(2).takeRight(4)
    year.concat("-").concat(month).concat("-").concat(day).concat(" 00:00:00")
  }  catch { case _ => "" }

  //calculates Size in megaBytes
  def calculateMB(s:String): Double  = try {
    val value = s.dropRight(1).toDouble
    val symbol = s.takeRight(1).toUpperCase()
    symbol match {
      case "M" => value
      case "K" => value/1024
      case _ => 0
    }
  }catch { case _ => 0 }

}
