package sf.ingest.config

object config {

  val partitionJson =
    """
    {
      "partitionStrategy": "variable",
      "partitionPivot": "_documentType",
      "defaultPartitions": ["_documentType", "_plugin", "year", "month", "day"],
      "partitionKeys": [{
        "pivotValue": "syslog",
        "keys": ["_documentType", "_plugin"]
        },
        {
  	"pivotValue": "ESLogs",
        "keys": ["_documentType", "_plugin", "year", "month"]
        }
      ]
    }
    """.stripMargin
  
}
