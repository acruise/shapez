package org.shapez;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.LongByReference;

/**
 * JNA binding for the shapez C ABI exposed by {@code libshapez_jvm}.
 * Callers normally use {@link Analyzer} rather than this interface
 * directly.
 *
 * <p>Returned {@code Pointer} values from {@code shapez_finalize} and
 * {@code shapez_analyze_jsonl} are heap-owned UTF-8 C strings that
 * must be released via {@link #shapez_free_string(Pointer)}. The
 * proto-returning entries write the buffer length into a {@link
 * LongByReference} out-param and the caller releases the buffer via
 * {@link #shapez_free_bytes(Pointer, long)}.
 */
public interface ShapezNative extends Library {

    ShapezNative INSTANCE = Native.load("shapez_jvm", ShapezNative.class);

    /** Allocate a streaming analyzer. Returns null on failure. */
    Pointer shapez_create();

    /** Free a handle previously returned by {@link #shapez_create()}.
     *  Safe to call with null. */
    void shapez_destroy(Pointer handle);

    /** Feed a JSONL chunk. Returns the number of documents accepted,
     *  or -1 on a null/invalid argument. Blank and malformed lines are
     *  silently skipped. */
    long shapez_feed_jsonl(Pointer handle, byte[] bytes, long len);

    /** Feed a single JSON document (UTF-8, null-terminated). Returns 1
     *  on success, 0 on parse error, -1 on null/invalid argument. */
    long shapez_feed_json_string(Pointer handle, String json);

    /** Render the analyzer's current text report. Caller must free the
     *  returned pointer with {@link #shapez_free_string(Pointer)}.
     *  Returns null on failure. */
    Pointer shapez_finalize(Pointer handle);

    /** Encode the analyzer's current state as a {@code
     *  shapez.report.AnalysisReport} protobuf. The serialized length
     *  is written into {@code outLen}. Caller must free with
     *  {@link #shapez_free_bytes(Pointer, long)}. */
    Pointer shapez_finalize_proto(Pointer handle, LongByReference outLen);

    /** Documents accepted so far, or -1 if the handle is null/invalid. */
    long shapez_doc_count(Pointer handle);

    /** One-shot: analyze a JSONL chunk and return the text report.
     *  Caller must free the returned pointer with
     *  {@link #shapez_free_string(Pointer)}. */
    Pointer shapez_analyze_jsonl(byte[] bytes, long len);

    /** One-shot: analyze a JSONL chunk and return the protobuf
     *  {@code AnalysisReport}. Length goes to {@code outLen}; caller
     *  frees via {@link #shapez_free_bytes(Pointer, long)}. */
    Pointer shapez_analyze_jsonl_proto(byte[] bytes, long len, LongByReference outLen);

    /** Release a heap-owned string returned by {@code shapez_finalize}
     *  or {@code shapez_analyze_jsonl}. Safe to call with null. */
    void shapez_free_string(Pointer s);

    /** Release a heap-owned byte buffer returned by
     *  {@code shapez_finalize_proto} or
     *  {@code shapez_analyze_jsonl_proto}. The {@code len} must match
     *  the length the producer wrote into the out-param; calling with
     *  a mismatched length leaks or corrupts memory. */
    void shapez_free_bytes(Pointer ptr, long len);

    /** Serialize the analyzer's current in-progress state as a {@code
     *  shapez.state.AnalyzerState} protobuf. Length goes to {@code
     *  outLen}; caller frees via {@link #shapez_free_bytes(Pointer,
     *  long)}. The analyzer must be quiescent (between documents);
     *  serializing mid-traversal panics in Rust and returns null
     *  here. */
    Pointer shapez_state_to_proto(Pointer handle, LongByReference outLen);

    /** Build a fresh handle by decoding a previously-serialized state
     *  buffer. Returns null on decode failure. The returned handle
     *  owns the analyzer and must be freed via
     *  {@link #shapez_destroy(Pointer)}. */
    Pointer shapez_create_from_state_proto(byte[] bytes, long len);
}
