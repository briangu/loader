package tweeder


import com.ning.http.client.AsyncHttpClient
import org.json.JSONObject


object Tweeder
{
  var host = "http://localhost:8080"
  val asyncHttpClient = new AsyncHttpClient()

  def main(args: Array[String]) {
    if (args.length > 2) host = args(2)
    if (args(0).equals("glu")) {
      val obj = loadFile(args(1))
      val entries = obj.getJSONArray("entries")
      (0 until entries.length()).foreach(i => insert(entries.getJSONObject(i).toString, "glu"))
    } else {
      scala.io.Source.fromFile(args(1)).getLines.foreach { rawRecord =>
        val record = new JSONObject(rawRecord)
        if (!record.has("delete")) insert(rawRecord, "tweets")
      }
    }
  }

  def loadFile(path: String) : JSONObject = {
    val file = scala.io.Source.fromFile(path)
    val rawData = file.getLines.mkString
    file.close
    new JSONObject(rawData)
  }

  def insert(record: String, projection: String) : String = {
    try {
      print(".")
      val response = asyncHttpClient
        .preparePost("%s/records".format(host))
        .addParameter("projection", projection)
        .addParameter("record", record)
        .execute
        .get
      val responseBody = response.getResponseBody
      if (responseBody.startsWith("{")) {
        val jsonResponse = new JSONObject()
        if (jsonResponse.has("id")) {
          jsonResponse.getString("id")
        } else {
          null
        }
      } else {
//        println(responseBody)
        null
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
  }
}
