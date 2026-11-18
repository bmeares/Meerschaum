//! One-sided characteristic-polynomial reconciliation for sparse immutable corrections.

use meerschaum_connector::{CPI_FIRST_POINT, CPI_PRIME, CpiPolynomials, CpiSeries};

fn add(a: i64, b: i64) -> i64 {
    (a + b) % CPI_PRIME
}

fn sub(a: i64, b: i64) -> i64 {
    (a - b).rem_euclid(CPI_PRIME)
}

fn mul(a: i64, b: i64) -> i64 {
    ((i128::from(a) * i128::from(b)) % i128::from(CPI_PRIME)) as i64
}

fn pow(mut base: i64, mut exp: i64) -> i64 {
    let mut result = 1;
    while exp > 0 {
        if exp % 2 == 1 {
            result = mul(result, base);
        }
        base = mul(base, base);
        exp /= 2;
    }
    result
}

fn value(coefficients: &[i64], x: i64) -> i64 {
    coefficients.iter().rev().fold(0, |y, &a| add(mul(y, x), a))
}

/// Recover a monic polynomial of known degree from that many points plus a check point.
/// Use Newton's incremental interpolation to add one constraint at a time.
fn interpolate(ratios: &[i64], degree: usize) -> Option<Vec<i64>> {
    if ratios.len() <= degree {
        return None;
    }
    let mut coefficients = vec![0; degree + 1];
    coefficients[degree] = 1;
    let mut basis = vec![1];
    for (i, &ratio) in ratios.iter().take(degree).enumerate() {
        let x = CPI_FIRST_POINT + i as i64;
        let delta = sub(ratio, value(&coefficients, x));
        let scale = mul(delta, pow(value(&basis, x), CPI_PRIME - 2));
        for (a, b) in coefficients.iter_mut().zip(&basis) {
            *a = add(*a, mul(scale, *b));
        }
        basis.push(0);
        for j in (1..basis.len()).rev() {
            basis[j] = sub(basis[j - 1], mul(basis[j], x));
        }
        basis[0] = sub(0, mul(basis[0], x));
    }
    ratios
        .iter()
        .enumerate()
        .skip(degree)
        .all(|(i, &ratio)| value(&coefficients, CPI_FIRST_POINT + i as i64) == ratio)
        .then_some(coefficients)
}

/// The target is a subset of the source, so dividing their samples cancels every shared root.
/// A failed consistency check causes a whole-day fetch.
pub(super) fn recover(
    source: &CpiSeries,
    target: &CpiSeries,
    source_rows: i64,
    target_rows: i64,
    points: usize,
) -> Option<CpiPolynomials> {
    if source.values().map(|s| s.rows).sum::<i64>() != source_rows
        || target.values().map(|s| s.rows).sum::<i64>() != target_rows
        || target.keys().any(|id| !source.contains_key(id))
    {
        return None;
    }
    let mut polynomials = Vec::new();
    for (id, source) in source {
        let target = target.get(id);
        let degree = usize::try_from(source.rows - target.map_or(0, |s| s.rows)).ok()?;
        if source.distinct_timestamps != source.rows
            || source.values.len() != points
            || target.is_some_and(|s| {
                s.distinct_timestamps != s.rows || s.values.len() != points || s.values.contains(&0)
            })
        {
            return None;
        }
        let ratios = source
            .values
            .iter()
            .enumerate()
            .map(|(i, &numerator)| {
                target.map_or(numerator, |samples| {
                    mul(numerator, pow(samples.values[i], CPI_PRIME - 2))
                })
            })
            .collect::<Vec<_>>();
        let coefficients = interpolate(&ratios, degree)?;
        if degree > 0 {
            polynomials.push((id.clone(), coefficients));
        }
    }
    Some(polynomials)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovers_sparse_roots_at_microsecond_precision() {
        let roots = [17, 17_000_001, 86_399_999_999];
        let samples = (0..4)
            .map(|i| {
                roots
                    .iter()
                    .fold(1, |a, &root| mul(a, sub(CPI_FIRST_POINT + i, root)))
            })
            .collect::<Vec<_>>();
        let coefficients = interpolate(&samples, roots.len()).unwrap();
        for root in roots {
            assert_eq!(value(&coefficients, root), 0);
        }
        let mut corrupted = samples;
        corrupted[3] += 1;
        assert!(interpolate(&corrupted, roots.len()).is_none());
    }
}
