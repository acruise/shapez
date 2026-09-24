package org.shapez

import java.nio.charset.StandardCharsets.UTF_8

class AnalyzerIntegrationTest extends munit.FunSuite {

  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)

  test("one-shot analyzeJsonl returns non-empty text") {
    val jsonl =
      """{"a": 1}
        |{"a": 2, "b": "x"}
        |{"a": 3}
        |""".stripMargin
    val report = Analyzer.analyzeJsonl(bytes(jsonl))
    assert(report.nonEmpty, "report should be non-empty")
    assert(report.contains("3 documents"), s"expected report to mention 3 documents:\n$report")
  }

  test("streaming Analyzer: feedJsonl + docCount + report") {
    val a = new Analyzer
    try {
      val accepted = a.feedJsonl(bytes("{\"x\": 1}\n{\"x\": 2}\n{\"x\": 3}\n"))
      assertEquals(accepted, 3L)
      assertEquals(a.docCount(), 3L)

      val more = a.feedJsonl(bytes("{\"y\": \"hello\"}\n"))
      assertEquals(more, 1L)
      assertEquals(a.docCount(), 4L)

      val report = a.report()
      assert(report.contains("4 documents"), s"expected report to mention 4 documents:\n$report")
    } finally a.close()
  }

  test("feedJsonl skips blank and malformed lines") {
    val a = new Analyzer
    try {
      val jsonl =
        """{"ok": 1}
          |
          |{not valid json}
          |{"ok": 2}
          |""".stripMargin
      val accepted = a.feedJsonl(bytes(jsonl))
      assertEquals(accepted, 2L)
      assertEquals(a.docCount(), 2L)
    } finally a.close()
  }

  test("feed(string) returns 1 on success, 0 on parse error") {
    val a = new Analyzer
    try {
      assertEquals(a.feed("""{"k": 1}"""), 1L)
      assertEquals(a.feed("""{"k": 2}"""), 1L)
      assertEquals(a.feed("""not valid"""), 0L)
      assertEquals(a.docCount(), 2L)
    } finally a.close()
  }

  test("close is idempotent") {
    val a = new Analyzer
    a.close()
    a.close() // must not throw
  }

  test("use-after-close raises IllegalStateException") {
    val a = new Analyzer
    a.close()
    intercept[IllegalStateException] {
      a.feed("""{"k": 1}""")
    }
  }

  test("reportProto returns parseable AnalysisReport") {
    val a = new Analyzer
    try {
      a.feedJsonl(bytes("""{"a": 1}""" + "\n" + """{"a": 2, "b": "x"}""" + "\n"))
      val report = a.reportProto()
      assertEquals(report.docCount, 2L)
      assert(report.shape.isDefined, "shape field set")
      val kind = report.shape.get.kind.get.variant
      assert(kind.isDefined, s"shape.kind.variant should be set; got $kind")
    } finally a.close()
  }

  test("analyzeJsonlProto one-shot returns AnalysisReport with expected doc count") {
    val report = Analyzer.analyzeJsonlProto(
      bytes("""{"k": 1}""" + "\n" + """{"k": 2}""" + "\n" + """{"k": 3}""" + "\n")
    )
    assertEquals(report.docCount, 3L)
    assert(report.shape.isDefined)
  }

  test("reportProtoBytes returns identical content to reportProto on roundtrip") {
    val a = new Analyzer
    try {
      a.feedJsonl(bytes("""{"v": "hello"}""" + "\n"))
      val raw = a.reportProtoBytes()
      val parsed = shapez.report.AnalysisReport.parseFrom(raw)
      assertEquals(parsed.docCount, 1L)
      assertEquals(parsed.toByteArray.toSeq, raw.toSeq)
    } finally a.close()
  }

  test("import path: shapez.report.AnalysisReport is the generated case class") {
    // Smoke check the import path lives where we expect after ScalaPB
    // generation. Catches refactors of the proto package or filename.
    val _: shapez.report.AnalysisReport = shapez.report.AnalysisReport.defaultInstance
  }

  test("stateBytes round-trips via fromStateBytes") {
    val a = new Analyzer
    try {
      a.feedJsonl(bytes("""{"a": 1}""" + "\n" + """{"a": 2}""" + "\n" + """{"a": 3}""" + "\n"))
      assertEquals(a.docCount(), 3L)
      val state = a.stateBytes()

      val b = Analyzer.fromStateBytes(state)
      try {
        assertEquals(b.docCount(), 3L)
        // Reports should match byte-for-byte after a round trip.
        assertEquals(b.report(), a.report())
        // And the typed AnalyzerState should parse.
        val parsed = b.state()
        assertEquals(parsed.docCount, 3L)
      } finally b.close()
    } finally a.close()
  }

  test("split feed via state round-trip matches continuous feed") {
    val continuous = new Analyzer
    val split = new Analyzer
    try {
      (0 until 20).foreach(i => continuous.feed(s"""{"x": ${i % 5}}"""))

      (0 until 10).foreach(i => split.feed(s"""{"x": ${i % 5}}"""))
      val midState = split.stateBytes()
      split.close()

      val resumed = Analyzer.fromStateBytes(midState)
      try {
        (10 until 20).foreach(i => resumed.feed(s"""{"x": ${i % 5}}"""))
        assertEquals(resumed.docCount(), continuous.docCount())
        assertEquals(resumed.report(), continuous.report())
      } finally resumed.close()
    } finally continuous.close()
  }

  test("fromStateBytes on garbage raises") {
    intercept[RuntimeException] {
      Analyzer.fromStateBytes("not a proto".getBytes)
    }
  }

  test("many small feeds work the same as one big chunk") {
    val streamed = {
      val a = new Analyzer
      try {
        (1 to 100).foreach(i => a.feed(s"""{"i": $i}"""))
        a.report()
      } finally a.close()
    }
    val batched = {
      val jsonl = (1 to 100).map(i => s"""{"i": $i}""").mkString("\n") + "\n"
      Analyzer.analyzeJsonl(bytes(jsonl))
    }
    // Reports include doc counts; both should reach 100.
    assert(streamed.contains("100 documents"), s"streamed missing 100 docs:\n$streamed")
    assert(batched.contains("100 documents"), s"batched missing 100 docs:\n$batched")
  }
}
