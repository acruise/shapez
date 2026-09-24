package org.shapez

import com.sun.jna.Pointer
import com.sun.jna.ptr.LongByReference

import shapez.report.AnalysisReport
import shapez.state.AnalyzerState

/** Handle-based streaming analyzer. Construct one per partition (or
  * per task), feed it documents, call [[report]] or [[reportProto]]
  * to materialize the current analysis, and `close()` when done.
  *
  * Not thread-safe: serialize feeds and finalization on a single
  * thread.
  */
final class Analyzer extends AutoCloseable {
  private val native = ShapezNative.INSTANCE

  private var handle: Pointer = {
    val p = native.shapez_create()
    if (p == null) throw new RuntimeException("shapez: shapez_create returned null")
    p
  }

  /** Feed a chunk of JSONL bytes. */
  def feedJsonl(bytes: Array[Byte]): Long = {
    requireOpen()
    native.shapez_feed_jsonl(handle, bytes, bytes.length.toLong)
  }

  /** Feed a single JSON document. */
  def feed(json: String): Long = {
    requireOpen()
    native.shapez_feed_json_string(handle, json)
  }

  /** Documents accepted so far. */
  def docCount(): Long = {
    requireOpen()
    native.shapez_doc_count(handle)
  }

  /** Render the current text report. Does not consume or close the
    * analyzer. */
  def report(): String = {
    requireOpen()
    val ptr = native.shapez_finalize(handle)
    if (ptr == null) throw new RuntimeException("shapez: shapez_finalize returned null")
    try ptr.getString(0L, "UTF-8")
    finally native.shapez_free_string(ptr)
  }

  /** Encode the current state as a `shapez.report.AnalysisReport`
    * protobuf and return the raw bytes. Wire format defined by
    * `shapez/proto/report.proto`. */
  def reportProtoBytes(): Array[Byte] = {
    requireOpen()
    val outLen = new LongByReference()
    val ptr = native.shapez_finalize_proto(handle, outLen)
    if (ptr == null) throw new RuntimeException("shapez: shapez_finalize_proto returned null")
    val len = outLen.getValue
    try ptr.getByteArray(0L, len.toInt)
    finally native.shapez_free_bytes(ptr, len)
  }

  /** Decode the current state to a `shapez.report.AnalysisReport`. */
  def reportProto(): AnalysisReport =
    AnalysisReport.parseFrom(reportProtoBytes())

  /** Serialize the analyzer's in-progress state to bytes. Use for
    * Spark UDAGG-style aggregation; round-trips faithfully through
    * [[Analyzer.fromStateBytes]]. The analyzer must be at quiescence
    * (between documents); serializing mid-traversal raises. */
  def stateBytes(): Array[Byte] = {
    requireOpen()
    val outLen = new LongByReference()
    val ptr = native.shapez_state_to_proto(handle, outLen)
    if (ptr == null) throw new RuntimeException("shapez: shapez_state_to_proto returned null")
    val len = outLen.getValue
    try ptr.getByteArray(0L, len.toInt)
    finally native.shapez_free_bytes(ptr, len)
  }

  /** Decode the analyzer's in-progress state to a `shapez.state.AnalyzerState`. */
  def state(): AnalyzerState =
    AnalyzerState.parseFrom(stateBytes())

  override def close(): Unit = {
    if (handle != null) {
      native.shapez_destroy(handle)
      handle = null
    }
  }

  private def requireOpen(): Unit =
    if (handle == null) throw new IllegalStateException("shapez analyzer is closed")

  // Used by Analyzer.fromStateBytes to swap in a handle produced by
  // a different native call. Frees the auto-created handle first.
  private[shapez] def replaceHandle(newHandle: Pointer): Unit = {
    if (handle != null) native.shapez_destroy(handle)
    handle = newHandle
  }
}

object Analyzer {

  /** One-shot: analyze a chunk of JSONL bytes and return the text
    * report. Allocates and frees its own analyzer. */
  def analyzeJsonl(bytes: Array[Byte]): String = {
    val native = ShapezNative.INSTANCE
    val ptr = native.shapez_analyze_jsonl(bytes, bytes.length.toLong)
    if (ptr == null) throw new RuntimeException("shapez: shapez_analyze_jsonl returned null")
    try ptr.getString(0L, "UTF-8")
    finally native.shapez_free_string(ptr)
  }

  /** One-shot: analyze a JSONL chunk and return the encoded proto bytes. */
  def analyzeJsonlProtoBytes(bytes: Array[Byte]): Array[Byte] = {
    val native = ShapezNative.INSTANCE
    val outLen = new LongByReference()
    val ptr = native.shapez_analyze_jsonl_proto(bytes, bytes.length.toLong, outLen)
    if (ptr == null) throw new RuntimeException("shapez: shapez_analyze_jsonl_proto returned null")
    val len = outLen.getValue
    try ptr.getByteArray(0L, len.toInt)
    finally native.shapez_free_bytes(ptr, len)
  }

  /** One-shot: analyze a JSONL chunk and return the parsed proto. */
  def analyzeJsonlProto(bytes: Array[Byte]): AnalysisReport =
    AnalysisReport.parseFrom(analyzeJsonlProtoBytes(bytes))

  /** Build a new `Analyzer` from a previously-serialized state buffer
    * (see [[Analyzer.stateBytes]]). Useful as the deserialize half of
    * a Spark UDAGG `reduce`/`merge` flow. Raises on decode failure. */
  def fromStateBytes(bytes: Array[Byte]): Analyzer = {
    val ptr = ShapezNative.INSTANCE.shapez_create_from_state_proto(bytes, bytes.length.toLong)
    if (ptr == null)
      throw new RuntimeException("shapez: shapez_create_from_state_proto returned null (decode failure)")
    val a = new Analyzer
    a.replaceHandle(ptr)
    a
  }
}
