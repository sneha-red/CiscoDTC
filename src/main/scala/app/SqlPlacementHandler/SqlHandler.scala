package app.SqlPlacementHandler

class SqlHandler {

  var extractUniqueExtensions =
    s"""
       |SELECT file_ext,count(*) as unique_file_count
       |FROM (select file_ext,nm from tmp_enriched_json where bg is NOT NULL and dp in (1,2,3) group by file_ext,nm)
       |GROUP BY file_ext
       |""".stripMargin

}
