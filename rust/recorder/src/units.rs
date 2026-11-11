//! Exact decimal string -> integer ticks / lots (no floats anywhere).
//!
//! Same contract as `python/mm/units.py`: a value that is not an exact multiple of the
//! step is an error, never rounded.

use anyhow::{bail, Context, Result};

/// A step such as "0.10" or "0.001", kept as an integer number of `10^-decimals` units.
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    pub text: String,
    decimals: u32,
    units: i64,
}

impl Step {
    pub fn parse(text: &str) -> Result<Step> {
        let decimals = text.split_once('.').map(|(_, f)| f.len() as u32).unwrap_or(0);
        let units = parse_fixed(text, decimals)?;
        if units <= 0 {
            bail!("step must be positive: {text:?}");
        }
        Ok(Step { text: text.to_string(), decimals, units })
    }

    /// `value / step` as an integer, or an error if it is not exact.
    pub fn to_units(&self, value: &str) -> Result<i64> {
        let v = parse_fixed(value, self.decimals)?;
        if v % self.units != 0 {
            bail!("{value:?} is not a multiple of step {:?}", self.text);
        }
        Ok(v / self.units)
    }
}

/// Parse a non-negative decimal string into an integer of `10^-decimals` units.
/// Extra fractional digits are allowed only if they are zeros ("0.100" with 2 decimals).
fn parse_fixed(s: &str, decimals: u32) -> Result<i64> {
    let (int_part, frac_part) = s.split_once('.').unwrap_or((s, ""));
    let all_digits = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
    if int_part.is_empty() || !all_digits(int_part) || !all_digits(frac_part) {
        bail!("not a non-negative decimal: {s:?}");
    }
    let d = decimals as usize;
    let (kept, extra) = if frac_part.len() > d { frac_part.split_at(d) } else { (frac_part, "") };
    if extra.bytes().any(|b| b != b'0') {
        bail!("{s:?} has more than {decimals} significant decimals");
    }
    let mut digits = String::with_capacity(int_part.len() + d);
    digits.push_str(int_part);
    digits.push_str(kept);
    for _ in kept.len()..d {
        digits.push('0');
    }
    digits.parse::<i64>().with_context(|| format!("overflow parsing {s:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_conversion() {
        let tick = Step::parse("0.10").unwrap();
        assert_eq!(tick.to_units("74810.40").unwrap(), 748104);
        assert_eq!(tick.to_units("74810.4").unwrap(), 748104);
        assert_eq!(tick.to_units("74810").unwrap(), 748100);
        assert!(tick.to_units("74810.45").is_err());
        let lot = Step::parse("0.001").unwrap();
        assert_eq!(lot.to_units("0.480").unwrap(), 480);
        assert_eq!(lot.to_units("0").unwrap(), 0);
        assert!(lot.to_units("-1").is_err());
        assert!(lot.to_units("1e3").is_err());
    }
}
