use std::ops::RangeInclusive;

use rand::Rng;
use rand::rngs::StdRng;
use serde_json::{Map, Value};

use crate::shape::*;

pub fn instantiate(shape: &Shape, rng: &mut StdRng) -> Value {
    match shape {
        Shape::Null => Value::Null,
        Shape::Bool => Value::Bool(rng.gen()),
        Shape::Int => {
            let n: i64 = rng.gen_range(-10_000..=10_000);
            Value::Number(n.into())
        }
        Shape::Float => {
            let f: f64 = rng.gen_range(-1000.0..1000.0);
            serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null)
        }
        Shape::String(format) => Value::String(gen_string(*format, rng)),
        Shape::Enum(values) => values[rng.gen_range(0..values.len())].clone(),
        Shape::Const(value) => value.clone(),
        Shape::Variant(arms) => instantiate(&arms[rng.gen_range(0..arms.len())], rng),
        Shape::Array(spec) => match &spec.kind {
            ArrayKind::Uniform(element) => {
                let n = rng.gen_range(spec.min_len..=spec.max_len);
                let items: Vec<Value> = (0..n).map(|_| instantiate(element, rng)).collect();
                Value::Array(items)
            }
            ArrayKind::Tuple(positions) => {
                let max_n = (spec.max_len as usize).min(positions.len());
                let min_n = (spec.min_len as usize).min(positions.len());
                let n = if min_n == max_n { min_n } else { rng.gen_range(min_n..=max_n) };
                let items: Vec<Value> =
                    positions.iter().take(n).map(|s| instantiate(s, rng)).collect();
                Value::Array(items)
            }
        },
        Shape::Object(spec) => match spec {
            ObjectSpec::Record { fields } => {
                let mut obj = Map::new();
                for f in fields {
                    let present = f.required || rng.gen::<f64>() < 0.7;
                    if present {
                        obj.insert(f.name.clone(), instantiate(&f.shape, rng));
                    }
                }
                Value::Object(obj)
            }
            ObjectSpec::Map { key_format, value, min_size, max_size } => {
                let n = rng.gen_range(*min_size..=*max_size);
                let mut obj = Map::new();
                for _ in 0..n {
                    let key = gen_string(*key_format, rng);
                    obj.insert(key, instantiate(value, rng));
                }
                Value::Object(obj)
            }
        },
    }
}

fn gen_string(format: Option<StringFormat>, rng: &mut StdRng) -> String {
    match format {
        Some(StringFormat::Uuid) => gen_uuid(rng),
        Some(StringFormat::DateTime) => gen_datetime(rng),
        Some(StringFormat::Date) => gen_date(rng),
        Some(StringFormat::Email) => gen_email(rng),
        Some(StringFormat::Ipv4) => gen_ipv4(rng),
        Some(StringFormat::Ipv6) => gen_ipv6(rng),
        Some(StringFormat::Url) => gen_url(rng),
        None => gen_lorem(rng, 3..=12),
    }
}

fn gen_uuid(rng: &mut StdRng) -> String {
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes[..]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15],
    )
}

fn gen_datetime(rng: &mut StdRng) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        rng.gen_range(2020..=2026),
        rng.gen_range(1..=12),
        rng.gen_range(1..=28),
        rng.gen_range(0..24),
        rng.gen_range(0..60),
        rng.gen_range(0..60),
    )
}

fn gen_date(rng: &mut StdRng) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        rng.gen_range(2020..=2026),
        rng.gen_range(1..=12),
        rng.gen_range(1..=28),
    )
}

fn gen_email(rng: &mut StdRng) -> String {
    format!("{}@{}.com", gen_lorem(rng, 4..=8), gen_lorem(rng, 4..=8))
}

fn gen_ipv4(rng: &mut StdRng) -> String {
    format!(
        "{}.{}.{}.{}",
        rng.gen_range(0..=255),
        rng.gen_range(0..=255),
        rng.gen_range(0..=255),
        rng.gen_range(0..=255),
    )
}

fn gen_ipv6(rng: &mut StdRng) -> String {
    let parts: Vec<String> = (0..8).map(|_| format!("{:x}", rng.gen_range(0u16..=0xffff))).collect();
    parts.join(":")
}

fn gen_url(rng: &mut StdRng) -> String {
    format!("https://{}.com/{}", gen_lorem(rng, 4..=8), gen_lorem(rng, 4..=8))
}

fn gen_lorem(rng: &mut StdRng, len_range: RangeInclusive<usize>) -> String {
    let len = rng.gen_range(*len_range.start()..=*len_range.end());
    (0..len).map(|_| rng.gen_range(b'a'..=b'z') as char).collect()
}
