//! The few fields of an X.509 certificate the messages use, read from its DER without a parser
//! dependency: the validity and the subject alternative names (else the common name).

use super::*;

/// One element: its tag, its contents and what follows it.
pub(super) fn read(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let n = usize::from(first & 0x7f);
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |acc, b| (acc << 8) | usize::from(*b));
        (len, &rest[n..])
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

/// Every element of a constructed value's contents.
fn elements(mut input: &[u8]) -> Vec<(u8, &[u8])> {
    let mut out = Vec::new();
    while let Some((tag, value, rest)) = read(input) {
        out.push((tag, value));
        input = rest;
    }
    out
}

/// `UTCTime` (`YYMMDDHHMM[SS]Z`) or `GeneralizedTime` (`YYYYMMDDHHMM[SS][.f]Z`).
pub(super) fn time(tag: u8, value: &[u8]) -> Option<DateTime<Utc>> {
    let text = std::str::from_utf8(value).ok()?.strip_suffix('Z')?;
    let (year, rest) = match tag {
        0x17 => {
            let yy: i32 = text.get(0..2)?.parse().ok()?;
            (if yy >= 50 { 1900 + yy } else { 2000 + yy }, text.get(2..)?)
        }
        0x18 => (text.get(0..4)?.parse().ok()?, text.get(4..)?),
        _ => return None,
    };
    let rest = rest.split('.').next()?;
    if !(rest.len() == 8 || rest.len() == 10) || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let num = |at: usize| rest.get(at..at + 2)?.parse::<u32>().ok();
    let seconds = if rest.len() == 10 { num(8)? } else { 0 };
    let date = NaiveDate::from_ymd_opt(year, num(0)?, num(2)?)?;
    let moment = date.and_hms_opt(num(4)?, num(6)?, seconds)?;
    Some(Utc.from_utc_datetime(&moment))
}

/// The `dNSName` and `iPAddress` entries of a `subjectAltName` extension's value.
fn alt_names(value: &[u8]) -> Vec<String> {
    let Some((0x30, names, _)) = read(value) else {
        return Vec::new();
    };
    elements(names)
        .into_iter()
        .filter_map(|(tag, name)| match tag {
            0x82 => std::str::from_utf8(name).ok().map(str::to_string),
            0x87 => match name.len() {
                4 => Some(IpAddr::from(<[u8; 4]>::try_from(name).ok()?).to_string()),
                16 => Some(IpAddr::from(<[u8; 16]>::try_from(name).ok()?).to_string()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// The subject's common name (`2.5.4.3`).
fn common_name(subject: &[u8]) -> Option<String> {
    for (tag, rdn) in elements(subject) {
        if tag != 0x31 {
            continue;
        }
        for (tag, attribute) in elements(rdn) {
            if tag != 0x30 {
                continue;
            }
            let parts = elements(attribute);
            if let [(0x06, oid), (_, value), ..] = parts.as_slice()
                && *oid == [0x55, 0x04, 0x03]
            {
                return Some(String::from_utf8_lossy(value).into_owned());
            }
        }
    }
    None
}

/// The names and validity of a certificate.
pub(super) fn summarize(cert: &[u8]) -> Option<CertificateSummary> {
    let (0x30, certificate, _) = read(cert)? else {
        return None;
    };
    let (0x30, tbs, _) = read(certificate)? else {
        return None;
    };
    let mut fields = elements(tbs).into_iter().peekable();
    // `version` is an explicit `[0]`, absent for v1 certificates.
    if fields.peek().is_some_and(|(tag, _)| *tag == 0xa0) {
        fields.next();
    }
    let _serial = fields.next()?;
    let _signature = fields.next()?;
    let _issuer = fields.next()?;
    let (0x30, validity) = fields.next()? else {
        return None;
    };
    let (0x30, subject) = fields.next()? else {
        return None;
    };
    let _public_key = fields.next()?;
    let mut names = Vec::new();
    for (tag, value) in fields {
        // `extensions` is an explicit `[3]`.
        if tag != 0xa3 {
            continue;
        }
        let Some((0x30, extensions, _)) = read(value) else {
            continue;
        };
        for (tag, extension) in elements(extensions) {
            if tag != 0x30 {
                continue;
            }
            let parts = elements(extension);
            let Some((0x06, oid)) = parts.first() else {
                continue;
            };
            if *oid == [0x55, 0x1d, 0x11]
                && let Some((_, value)) = parts.iter().find(|(tag, _)| *tag == 0x04)
            {
                names = alt_names(value);
            }
        }
    }
    if names.is_empty() {
        names.extend(common_name(subject));
    }
    let times = elements(validity);
    Some(CertificateSummary {
        names,
        not_before: times.first().and_then(|(tag, v)| time(*tag, v)),
        not_after: times.get(1).and_then(|(tag, v)| time(*tag, v)),
        chain: 1,
    })
}
