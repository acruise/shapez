//! C-ABI bindings for shapez, designed to be called via JNA from the
//! JVM (or any other FFI consumer comfortable with C-style symbols).
//!
//! Symbol convention: every public entry point is `shapez_*` with no
//! mangling. Buffers cross the boundary as `(ptr, len)` pairs; strings
//! we return are heap-owned `CString`s and the caller must release
//! them with [`shapez_free_string`].
//!
//! Threading: each handle owns a `StreamingAnalyzer` and must not be
//! shared across threads without external synchronization. The handle
//! pointer itself is fine to move between threads as long as feeding
//! and finalization are serialized on a single thread at a time.
//!
//! Panic safety: every entry point wraps its work in `catch_unwind`.
//! Panics are converted to sentinel returns (`null` / `-1`) rather
//! than unwound across the FFI boundary, which is undefined behavior.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;

use shapez::StreamingAnalyzer;
use shapez_json::drive_document;

/// Owned by the JVM as an opaque `Pointer`; freed by [`shapez_destroy`].
struct Handle {
    analyzer: StreamingAnalyzer,
    next_ordinal: u64,
}

impl Handle {
    fn new() -> Self {
        Self {
            analyzer: StreamingAnalyzer::new(),
            next_ordinal: 0,
        }
    }

    fn feed_one(&mut self, value: &serde_json::Value) {
        drive_document(&mut self.analyzer, self.next_ordinal, value);
        self.next_ordinal = self.next_ordinal.wrapping_add(1);
    }

    fn feed_jsonl(&mut self, buf: &[u8]) -> u64 {
        let mut accepted = 0u64;
        for raw in buf.split(|&b| b == b'\n') {
            let line = raw.strip_suffix(b"\r").unwrap_or(raw);
            if line.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(line) {
                self.feed_one(&v);
                accepted += 1;
            }
        }
        accepted
    }
}

fn report_to_cstr(report: String) -> *mut c_char {
    CString::new(report)
        .map(CString::into_raw)
        .unwrap_or(std::ptr::null_mut())
}

/// Leak a `Vec<u8>` as a `(ptr, len)` pair the caller will reclaim via
/// [`shapez_free_bytes`]. Capacity is forced equal to length first so
/// the corresponding `from_raw_parts` reclaims the right allocation.
fn bytes_to_raw(mut bytes: Vec<u8>, out_len: *mut i64) -> *mut u8 {
    bytes.shrink_to_fit();
    debug_assert_eq!(bytes.len(), bytes.capacity());
    let len = bytes.len() as i64;
    let ptr = bytes.as_mut_ptr();
    std::mem::forget(bytes);
    if !out_len.is_null() {
        unsafe { *out_len = len };
    }
    ptr
}

#[no_mangle]
pub extern "C" fn shapez_create() -> *mut c_void {
    catch_unwind(|| Box::into_raw(Box::new(Handle::new())) as *mut c_void)
        .unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn shapez_destroy(handle: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() {
            return;
        }
        unsafe { drop(Box::from_raw(handle as *mut Handle)) };
    }));
}

#[no_mangle]
pub extern "C" fn shapez_feed_jsonl(handle: *mut c_void, bytes: *const u8, len: i64) -> i64 {
    if handle.is_null() || bytes.is_null() || len < 0 {
        return -1;
    }
    let buf = unsafe { slice::from_raw_parts(bytes, len as usize) };
    catch_unwind(AssertUnwindSafe(|| {
        let h = unsafe { &mut *(handle as *mut Handle) };
        h.feed_jsonl(buf) as i64
    }))
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn shapez_feed_json_string(handle: *mut c_void, json: *const c_char) -> i64 {
    if handle.is_null() || json.is_null() {
        return -1;
    }
    catch_unwind(AssertUnwindSafe(|| {
        let s = match unsafe { CStr::from_ptr(json) }.to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        };
        let h = unsafe { &mut *(handle as *mut Handle) };
        match serde_json::from_str::<serde_json::Value>(s) {
            Ok(v) => {
                h.feed_one(&v);
                1
            }
            Err(_) => 0,
        }
    }))
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn shapez_finalize(handle: *const c_void) -> *mut c_char {
    if handle.is_null() {
        return std::ptr::null_mut();
    }
    catch_unwind(AssertUnwindSafe(|| {
        let h = unsafe { &*(handle as *const Handle) };
        report_to_cstr(h.analyzer.report())
    }))
    .unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn shapez_finalize_proto(
    handle: *const c_void,
    out_len: *mut i64,
) -> *mut u8 {
    if !out_len.is_null() {
        unsafe { *out_len = 0 };
    }
    if handle.is_null() {
        return std::ptr::null_mut();
    }
    catch_unwind(AssertUnwindSafe(|| {
        let h = unsafe { &*(handle as *const Handle) };
        bytes_to_raw(h.analyzer.report_proto(), out_len)
    }))
    .unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn shapez_doc_count(handle: *const c_void) -> i64 {
    if handle.is_null() {
        return -1;
    }
    catch_unwind(AssertUnwindSafe(|| {
        let h = unsafe { &*(handle as *const Handle) };
        h.analyzer.doc_count() as i64
    }))
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn shapez_analyze_jsonl(bytes: *const u8, len: i64) -> *mut c_char {
    if bytes.is_null() || len < 0 {
        return std::ptr::null_mut();
    }
    let buf = unsafe { slice::from_raw_parts(bytes, len as usize) };
    catch_unwind(AssertUnwindSafe(|| {
        let mut h = Handle::new();
        h.feed_jsonl(buf);
        report_to_cstr(h.analyzer.report())
    }))
    .unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn shapez_analyze_jsonl_proto(
    bytes: *const u8,
    len: i64,
    out_len: *mut i64,
) -> *mut u8 {
    if !out_len.is_null() {
        unsafe { *out_len = 0 };
    }
    if bytes.is_null() || len < 0 {
        return std::ptr::null_mut();
    }
    let buf = unsafe { slice::from_raw_parts(bytes, len as usize) };
    catch_unwind(AssertUnwindSafe(|| {
        let mut h = Handle::new();
        h.feed_jsonl(buf);
        bytes_to_raw(h.analyzer.report_proto(), out_len)
    }))
    .unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn shapez_state_to_proto(
    handle: *const c_void,
    out_len: *mut i64,
) -> *mut u8 {
    if !out_len.is_null() {
        unsafe { *out_len = 0 };
    }
    if handle.is_null() {
        return std::ptr::null_mut();
    }
    catch_unwind(AssertUnwindSafe(|| {
        let h = unsafe { &*(handle as *const Handle) };
        bytes_to_raw(shapez::analyzer_state_bytes(&h.analyzer), out_len)
    }))
    .unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn shapez_create_from_state_proto(
    bytes: *const u8,
    len: i64,
) -> *mut c_void {
    if bytes.is_null() || len < 0 {
        return std::ptr::null_mut();
    }
    let buf = unsafe { slice::from_raw_parts(bytes, len as usize) };
    catch_unwind(AssertUnwindSafe(|| {
        let analyzer = match shapez::analyzer_from_state_bytes(buf) {
            Ok(a) => a,
            Err(_) => return std::ptr::null_mut(),
        };
        let next_ordinal = analyzer.doc_count();
        let h = Box::new(Handle { analyzer, next_ordinal });
        Box::into_raw(h) as *mut c_void
    }))
    .unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn shapez_free_bytes(ptr: *mut u8, len: i64) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if ptr.is_null() || len <= 0 {
            return;
        }
        unsafe {
            let _ = Vec::from_raw_parts(ptr, len as usize, len as usize);
        }
    }));
}

#[no_mangle]
pub extern "C" fn shapez_free_string(s: *mut c_char) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if s.is_null() {
            return;
        }
        unsafe { drop(CString::from_raw(s)) };
    }));
}

#[cfg(test)]
mod tests {
    //! Pure-Rust smoke tests for the handle plumbing and the C-ABI
    //! entry points. We exercise the public functions directly,
    //! treating the opaque `*mut c_void` as just another pointer.

    use super::*;

    #[test]
    fn handle_create_and_destroy() {
        let h = shapez_create();
        assert!(!h.is_null());
        shapez_destroy(h);
    }

    #[test]
    fn destroy_null_is_safe() {
        shapez_destroy(std::ptr::null_mut());
    }

    #[test]
    fn feed_jsonl_round_trip() {
        let h = shapez_create();
        let input = b"{\"a\":1}\n{\"a\":2}\n\n{not json}\n{\"a\":3}\n";
        let accepted =
            shapez_feed_jsonl(h, input.as_ptr(), input.len() as i64);
        assert_eq!(accepted, 3);
        assert_eq!(shapez_doc_count(h), 3);

        let cstr = shapez_finalize(h);
        assert!(!cstr.is_null());
        let s = unsafe { CStr::from_ptr(cstr) }.to_str().unwrap().to_owned();
        assert!(s.contains("3 documents"));
        shapez_free_string(cstr);

        shapez_destroy(h);
    }

    #[test]
    fn feed_json_string_one_each() {
        let h = shapez_create();
        let ok = CString::new(r#"{"x":1}"#).unwrap();
        let bad = CString::new("not valid").unwrap();
        assert_eq!(shapez_feed_json_string(h, ok.as_ptr()), 1);
        assert_eq!(shapez_feed_json_string(h, bad.as_ptr()), 0);
        assert_eq!(shapez_doc_count(h), 1);
        shapez_destroy(h);
    }

    #[test]
    fn analyze_jsonl_one_shot() {
        let input = b"{\"a\":1}\n{\"a\":2}\n";
        let cstr = shapez_analyze_jsonl(input.as_ptr(), input.len() as i64);
        assert!(!cstr.is_null());
        let s = unsafe { CStr::from_ptr(cstr) }.to_str().unwrap().to_owned();
        assert!(s.contains("2 documents"));
        shapez_free_string(cstr);
    }

    #[test]
    fn finalize_proto_round_trips() {
        use prost::Message;
        use shapez::report::proto;

        let h = shapez_create();
        let input = b"{\"a\":1}\n{\"a\":2}\n";
        let _ = shapez_feed_jsonl(h, input.as_ptr(), input.len() as i64);
        let mut len: i64 = 0;
        let ptr = shapez_finalize_proto(h, &mut len as *mut i64);
        assert!(!ptr.is_null());
        assert!(len > 0);
        let bytes = unsafe { slice::from_raw_parts(ptr, len as usize) }.to_vec();
        shapez_free_bytes(ptr, len);
        let decoded = proto::AnalysisReport::decode(&*bytes).expect("decode");
        assert_eq!(decoded.doc_count, 2);
        assert!(decoded.shape.is_some());
        shapez_destroy(h);
    }

    #[test]
    fn analyze_jsonl_proto_one_shot() {
        use prost::Message;
        use shapez::report::proto;

        let input = b"{\"x\":\"a\"}\n{\"x\":\"b\"}\n{\"x\":\"c\"}\n";
        let mut len: i64 = 0;
        let ptr = shapez_analyze_jsonl_proto(input.as_ptr(), input.len() as i64, &mut len as *mut i64);
        assert!(!ptr.is_null());
        let bytes = unsafe { slice::from_raw_parts(ptr, len as usize) }.to_vec();
        shapez_free_bytes(ptr, len);
        let decoded = proto::AnalysisReport::decode(&*bytes).expect("decode");
        assert_eq!(decoded.doc_count, 3);
    }

    #[test]
    fn free_bytes_null_is_safe() {
        shapez_free_bytes(std::ptr::null_mut(), 0);
        shapez_free_bytes(std::ptr::null_mut(), 100);
    }

    #[test]
    fn state_round_trip_via_jna_layer() {
        let h = shapez_create();
        let input = b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n";
        let _ = shapez_feed_jsonl(h, input.as_ptr(), input.len() as i64);
        assert_eq!(shapez_doc_count(h), 3);

        let mut len: i64 = 0;
        let state_ptr = shapez_state_to_proto(h, &mut len as *mut i64);
        assert!(!state_ptr.is_null());
        assert!(len > 0);
        let state_bytes = unsafe { slice::from_raw_parts(state_ptr, len as usize) }.to_vec();
        shapez_free_bytes(state_ptr, len);

        // Rebuild from bytes; new handle should agree on doc count + report.
        let h2 = shapez_create_from_state_proto(state_bytes.as_ptr(), state_bytes.len() as i64);
        assert!(!h2.is_null());
        assert_eq!(shapez_doc_count(h2), 3);

        // Continue feeding on the rebuilt handle.
        let more = b"{\"a\":4}\n{\"a\":5}\n";
        let _ = shapez_feed_jsonl(h2, more.as_ptr(), more.len() as i64);
        assert_eq!(shapez_doc_count(h2), 5);

        shapez_destroy(h);
        shapez_destroy(h2);
    }

    #[test]
    fn create_from_invalid_state_bytes_returns_null() {
        let garbage = b"not a valid proto blob";
        let h = shapez_create_from_state_proto(garbage.as_ptr(), garbage.len() as i64);
        assert!(h.is_null());
    }
}
