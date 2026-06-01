use std::fmt::{self, Write as _};
use std::str::FromStr;

/// One step in a path through a nested value.
///
/// Paths are pure syntactic location. Whether an object step represents a
/// record field or a high-cardinality map key is a property of the shape
/// tree at that position, not of the path itself.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PathStep {
    Field(String),
    Index(u32),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Path(pub Vec<PathStep>);

impl Path {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn push(&mut self, step: PathStep) {
        self.0.push(step);
    }

    pub fn pop(&mut self) -> Option<PathStep> {
        self.0.pop()
    }
}

/// Pattern over paths, for matching assertions to ingest sites and for
/// canonical representation of paths with map-decided steps collapsed to
/// `AnyField`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PathPatternStep {
    Field(String),
    AnyField,
    Index(u32),
    AnyIndex,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct PathPattern(pub Vec<PathPatternStep>);

// --- Display ---

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(".")?;
        let mut first = true;
        for step in &self.0 {
            match step {
                PathStep::Field(name) => {
                    write_field_sep(f, first)?;
                    write_ident_or_quoted(f, name)?;
                }
                PathStep::Index(i) => write!(f, "[{}]", i)?,
            }
            first = false;
        }
        Ok(())
    }
}

impl fmt::Display for PathPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(".")?;
        let mut first = true;
        for step in &self.0 {
            match step {
                PathPatternStep::Field(name) => {
                    write_field_sep(f, first)?;
                    write_ident_or_quoted(f, name)?;
                }
                PathPatternStep::AnyField => {
                    write_field_sep(f, first)?;
                    f.write_str("*")?;
                }
                PathPatternStep::Index(i) => write!(f, "[{}]", i)?,
                PathPatternStep::AnyIndex => f.write_str("[*]")?,
            }
            first = false;
        }
        Ok(())
    }
}

/// First Field after root absorbs the root '.'; subsequent Fields get their own.
fn write_field_sep(f: &mut fmt::Formatter<'_>, first: bool) -> fmt::Result {
    if first { Ok(()) } else { f.write_str(".") }
}

fn write_ident_or_quoted(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
    if is_bare_ident(s) {
        f.write_str(s)
    } else {
        write_quoted(f, s)
    }
}

fn is_bare_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn write_quoted(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
    f.write_str("\"")?;
    for c in s.chars() {
        match c {
            '"' => f.write_str("\\\"")?,
            '\\' => f.write_str("\\\\")?,
            '\n' => f.write_str("\\n")?,
            '\t' => f.write_str("\\t")?,
            '\r' => f.write_str("\\r")?,
            '\0' => f.write_str("\\0")?,
            c if (c as u32) < 0x20 => write!(f, "\\u{{{:x}}}", c as u32)?,
            c => f.write_char(c)?,
        }
    }
    f.write_str("\"")
}

// --- Parser ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub pos: usize,
    pub kind: ParseErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseErrorKind {
    Empty,
    ExpectedRoot,
    UnexpectedChar(char),
    UnexpectedEnd,
    UnterminatedString,
    InvalidEscape(char),
    InvalidUnicodeEscape,
    InvalidNumber,
    WildcardInPath,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "at byte {}: {:?}", self.pos, self.kind)
    }
}

impl std::error::Error for ParseError {}

struct Parser<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self {
        Self { src: s.as_bytes(), pos: 0 }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let c = self.peek()?;
        self.pos += 1;
        Some(c)
    }

    fn err(&self, kind: ParseErrorKind) -> ParseError {
        ParseError { pos: self.pos, kind }
    }

    fn parse_pattern(&mut self) -> Result<PathPattern, ParseError> {
        if self.src.is_empty() {
            return Err(self.err(ParseErrorKind::Empty));
        }
        if self.bump() != Some(b'.') {
            return Err(ParseError { pos: 0, kind: ParseErrorKind::ExpectedRoot });
        }
        let mut steps = Vec::new();
        let mut first = true;
        loop {
            match self.peek() {
                None => return Ok(PathPattern(steps)),
                Some(b'.') => {
                    if first {
                        return Err(self.err(ParseErrorKind::UnexpectedChar('.')));
                    }
                    self.bump();
                    steps.push(self.parse_field_body()?);
                }
                Some(b'[') => {
                    self.bump();
                    steps.push(self.parse_index_body()?);
                }
                Some(c) if first && (c.is_ascii_alphabetic() || c == b'_' || c == b'"' || c == b'*') => {
                    steps.push(self.parse_field_body()?);
                }
                Some(c) => return Err(self.err(ParseErrorKind::UnexpectedChar(c as char))),
            }
            first = false;
        }
    }

    fn parse_field_body(&mut self) -> Result<PathPatternStep, ParseError> {
        match self.peek() {
            Some(b'*') => {
                self.bump();
                Ok(PathPatternStep::AnyField)
            }
            Some(b'"') => {
                let s = self.parse_quoted()?;
                Ok(PathPatternStep::Field(s))
            }
            Some(c) if c.is_ascii_alphabetic() || c == b'_' => {
                let s = self.parse_ident();
                Ok(PathPatternStep::Field(s))
            }
            Some(c) => Err(self.err(ParseErrorKind::UnexpectedChar(c as char))),
            None => Err(self.err(ParseErrorKind::UnexpectedEnd)),
        }
    }

    fn parse_index_body(&mut self) -> Result<PathPatternStep, ParseError> {
        let step = match self.peek() {
            Some(b'*') => {
                self.bump();
                PathPatternStep::AnyIndex
            }
            Some(c) if c.is_ascii_digit() => {
                let n = self.parse_u32()?;
                PathPatternStep::Index(n)
            }
            Some(c) => return Err(self.err(ParseErrorKind::UnexpectedChar(c as char))),
            None => return Err(self.err(ParseErrorKind::UnexpectedEnd)),
        };
        match self.bump() {
            Some(b']') => Ok(step),
            Some(c) => Err(ParseError { pos: self.pos - 1, kind: ParseErrorKind::UnexpectedChar(c as char) }),
            None => Err(self.err(ParseErrorKind::UnexpectedEnd)),
        }
    }

    fn parse_ident(&mut self) -> String {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.bump();
            } else {
                break;
            }
        }
        std::str::from_utf8(&self.src[start..self.pos]).unwrap().to_string()
    }

    fn parse_u32(&mut self) -> Result<u32, ParseError> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.bump();
            } else {
                break;
            }
        }
        let s = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
        s.parse::<u32>().map_err(|_| ParseError { pos: start, kind: ParseErrorKind::InvalidNumber })
    }

    fn parse_quoted(&mut self) -> Result<String, ParseError> {
        debug_assert_eq!(self.peek(), Some(b'"'));
        self.bump();
        let mut out = String::new();
        loop {
            match self.bump() {
                None => return Err(self.err(ParseErrorKind::UnterminatedString)),
                Some(b'"') => return Ok(out),
                Some(b'\\') => match self.bump() {
                    Some(b'"') => out.push('"'),
                    Some(b'\\') => out.push('\\'),
                    Some(b'n') => out.push('\n'),
                    Some(b't') => out.push('\t'),
                    Some(b'r') => out.push('\r'),
                    Some(b'0') => out.push('\0'),
                    Some(b'u') => out.push(self.parse_unicode_escape()?),
                    Some(c) => return Err(ParseError { pos: self.pos - 1, kind: ParseErrorKind::InvalidEscape(c as char) }),
                    None => return Err(self.err(ParseErrorKind::UnterminatedString)),
                },
                Some(_) => {
                    let start = self.pos - 1;
                    let rest = &self.src[start..];
                    let s = std::str::from_utf8(rest).unwrap();
                    let ch = s.chars().next().unwrap();
                    let len = ch.len_utf8();
                    self.pos = start + len;
                    out.push(ch);
                }
            }
        }
    }

    fn parse_unicode_escape(&mut self) -> Result<char, ParseError> {
        if self.bump() != Some(b'{') {
            return Err(self.err(ParseErrorKind::InvalidUnicodeEscape));
        }
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_hexdigit() {
                self.bump();
            } else {
                break;
            }
        }
        let hex = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
        if hex.is_empty() {
            return Err(self.err(ParseErrorKind::InvalidUnicodeEscape));
        }
        let cp = u32::from_str_radix(hex, 16).map_err(|_| self.err(ParseErrorKind::InvalidUnicodeEscape))?;
        if self.bump() != Some(b'}') {
            return Err(self.err(ParseErrorKind::InvalidUnicodeEscape));
        }
        char::from_u32(cp).ok_or_else(|| self.err(ParseErrorKind::InvalidUnicodeEscape))
    }
}

impl FromStr for PathPattern {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut p = Parser::new(s);
        p.parse_pattern()
    }
}

impl FromStr for Path {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let pat: PathPattern = s.parse()?;
        Path::try_from(pat)
    }
}

impl TryFrom<PathPattern> for Path {
    type Error = ParseError;
    fn try_from(pat: PathPattern) -> Result<Self, Self::Error> {
        let mut steps = Vec::with_capacity(pat.0.len());
        for s in pat.0 {
            match s {
                PathPatternStep::Field(n) => steps.push(PathStep::Field(n)),
                PathPatternStep::Index(i) => steps.push(PathStep::Index(i)),
                PathPatternStep::AnyField | PathPatternStep::AnyIndex => {
                    return Err(ParseError { pos: 0, kind: ParseErrorKind::WildcardInPath });
                }
            }
        }
        Ok(Path(steps))
    }
}

impl From<Path> for PathPattern {
    fn from(p: Path) -> Self {
        let steps = p
            .0
            .into_iter()
            .map(|s| match s {
                PathStep::Field(n) => PathPatternStep::Field(n),
                PathStep::Index(i) => PathPatternStep::Index(i),
            })
            .collect();
        PathPattern(steps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt_path(s: &str, expected: Path) {
        let got: Path = s.parse().unwrap();
        assert_eq!(got, expected);
        assert_eq!(got.to_string(), s);
    }

    fn rt_pat(s: &str, expected: PathPattern) {
        let got: PathPattern = s.parse().unwrap();
        assert_eq!(got, expected);
        assert_eq!(got.to_string(), s);
    }

    #[test]
    fn root_only() {
        rt_path(".", Path(vec![]));
    }

    #[test]
    fn single_field() {
        rt_path(".foo", Path(vec![PathStep::Field("foo".into())]));
    }

    #[test]
    fn nested_fields() {
        rt_path(
            ".user.profile.name",
            Path(vec![
                PathStep::Field("user".into()),
                PathStep::Field("profile".into()),
                PathStep::Field("name".into()),
            ]),
        );
    }

    #[test]
    fn index_at_root() {
        rt_path(".[0]", Path(vec![PathStep::Index(0)]));
    }

    #[test]
    fn field_index_field() {
        rt_path(
            ".users[3].name",
            Path(vec![
                PathStep::Field("users".into()),
                PathStep::Index(3),
                PathStep::Field("name".into()),
            ]),
        );
    }

    #[test]
    fn quoted_uuid_field() {
        rt_path(
            ".cache.\"6f9619ff-8b86-d011-b42d-00c04fc964ff\"",
            Path(vec![
                PathStep::Field("cache".into()),
                PathStep::Field("6f9619ff-8b86-d011-b42d-00c04fc964ff".into()),
            ]),
        );
    }

    #[test]
    fn quoted_field_with_space() {
        rt_path(
            ".user.\"display name\"",
            Path(vec![
                PathStep::Field("user".into()),
                PathStep::Field("display name".into()),
            ]),
        );
    }

    #[test]
    fn wildcards_in_pattern() {
        rt_pat(
            ".users[*].name",
            PathPattern(vec![
                PathPatternStep::Field("users".into()),
                PathPatternStep::AnyIndex,
                PathPatternStep::Field("name".into()),
            ]),
        );
        rt_pat(
            ".events[*].properties.*",
            PathPattern(vec![
                PathPatternStep::Field("events".into()),
                PathPatternStep::AnyIndex,
                PathPatternStep::Field("properties".into()),
                PathPatternStep::AnyField,
            ]),
        );
        rt_pat(
            ".*.name",
            PathPattern(vec![PathPatternStep::AnyField, PathPatternStep::Field("name".into())]),
        );
    }

    #[test]
    fn wildcard_rejected_in_path() {
        let err = ".users[*].name".parse::<Path>().unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::WildcardInPath));
    }

    #[test]
    fn empty_is_error() {
        assert!(matches!("".parse::<Path>(), Err(ParseError { kind: ParseErrorKind::Empty, .. })));
    }

    #[test]
    fn double_dot_is_error() {
        let err = "..foo".parse::<Path>().unwrap_err();
        assert!(matches!(err.kind, ParseErrorKind::UnexpectedChar('.')));
    }

    #[test]
    fn missing_root_is_error() {
        assert!(matches!(
            "foo".parse::<Path>(),
            Err(ParseError { kind: ParseErrorKind::ExpectedRoot, .. })
        ));
    }

    #[test]
    fn escape_sequences() {
        let p: Path = ".\"a\\nb\\tc\\\"d\"".parse().unwrap();
        assert_eq!(p.0[0], PathStep::Field("a\nb\tc\"d".into()));
    }

    #[test]
    fn unicode_escape() {
        let p: Path = ".\"\\u{1f600}\"".parse().unwrap();
        assert_eq!(p.0[0], PathStep::Field("\u{1f600}".into()));
    }
}
