//! Typed filter selection kernels.

/// Converts a boolean slice and optional validity slice into a selection vector (`Vec<u32>`).
///
/// A row `i` is included in the selection vector if and only if:
/// - `data[i] == true`
/// - and `validity.map_or(true, |v| v[i])` is true.
///
/// This implementation chunks by 64 bits (8 bools at a time) and 512 bits (64 bools at a time)
/// to skip all-false regions and fast-path all-true regions without per-element branches.
pub fn bool_to_selection(data: &[bool], validity: Option<&[bool]>) -> Vec<u32> {
    let len = data.len();
    if len == 0 {
        return Vec::new();
    }
    match validity {
        Some(val) => bool_to_selection_with_validity(data, val),
        None => bool_to_selection_no_validity(data),
    }
}

const ALL_TRUE_U64: u64 = 0x0101010101010101;

#[inline(always)]
fn load_8_bools(slice: &[bool]) -> u64 {
    let mut bytes = [0u8; 8];
    for (i, &b) in slice[0..8].iter().enumerate() {
        bytes[i] = b as u8;
    }
    u64::from_ne_bytes(bytes)
}

fn bool_to_selection_no_validity(data: &[bool]) -> Vec<u32> {
    let len = data.len();
    let mut selected = Vec::new();

    let chunks64 = len / 64;
    let mut idx = 0;

    for _ in 0..chunks64 {
        let chunk = &data[idx..idx + 64];
        let base = idx as u32;

        let w0 = load_8_bools(&chunk[0..8]);
        let w1 = load_8_bools(&chunk[8..16]);
        let w2 = load_8_bools(&chunk[16..24]);
        let w3 = load_8_bools(&chunk[24..32]);
        let w4 = load_8_bools(&chunk[32..40]);
        let w5 = load_8_bools(&chunk[40..48]);
        let w6 = load_8_bools(&chunk[48..56]);
        let w7 = load_8_bools(&chunk[56..64]);

        let any_true = w0 | w1 | w2 | w3 | w4 | w5 | w6 | w7;
        if any_true == 0 {
            idx += 64;
            continue;
        }

        let all_true = w0 & w1 & w2 & w3 & w4 & w5 & w6 & w7;
        if all_true == ALL_TRUE_U64
            && w0 == ALL_TRUE_U64
            && w1 == ALL_TRUE_U64
            && w2 == ALL_TRUE_U64
            && w3 == ALL_TRUE_U64
            && w4 == ALL_TRUE_U64
            && w5 == ALL_TRUE_U64
            && w6 == ALL_TRUE_U64
            && w7 == ALL_TRUE_U64
        {
            selected.reserve(64);
            for i in 0..64 {
                selected.push(base + i);
            }
            idx += 64;
            continue;
        }

        // Mixed: process each 8-byte word
        let words = [w0, w1, w2, w3, w4, w5, w6, w7];
        for (w_i, &w) in words.iter().enumerate() {
            if w == 0 {
                continue;
            }
            let word_base = base + (w_i as u32) * 8;
            if w == ALL_TRUE_U64 {
                for i in 0..8 {
                    selected.push(word_base + i);
                }
                continue;
            }
            let bytes = w.to_ne_bytes();
            for (b_i, &b) in bytes.iter().enumerate() {
                if b != 0 {
                    selected.push(word_base + b_i as u32);
                }
            }
        }

        idx += 64;
    }

    // Remainder
    for (i, &b) in data[idx..].iter().enumerate() {
        if b {
            selected.push((idx + i) as u32);
        }
    }

    selected
}

fn bool_to_selection_with_validity(data: &[bool], val: &[bool]) -> Vec<u32> {
    let len = data.len().min(val.len());
    let mut selected = Vec::new();

    let chunks64 = len / 64;
    let mut idx = 0;

    for _ in 0..chunks64 {
        let d_chunk = &data[idx..idx + 64];
        let v_chunk = &val[idx..idx + 64];
        let base = idx as u32;

        let w0 = load_8_bools(&d_chunk[0..8]) & load_8_bools(&v_chunk[0..8]);
        let w1 = load_8_bools(&d_chunk[8..16]) & load_8_bools(&v_chunk[8..16]);
        let w2 = load_8_bools(&d_chunk[16..24]) & load_8_bools(&v_chunk[16..24]);
        let w3 = load_8_bools(&d_chunk[24..32]) & load_8_bools(&v_chunk[24..32]);
        let w4 = load_8_bools(&d_chunk[32..40]) & load_8_bools(&v_chunk[32..40]);
        let w5 = load_8_bools(&d_chunk[40..48]) & load_8_bools(&v_chunk[40..48]);
        let w6 = load_8_bools(&d_chunk[48..56]) & load_8_bools(&v_chunk[48..56]);
        let w7 = load_8_bools(&d_chunk[56..64]) & load_8_bools(&v_chunk[56..64]);

        let any_true = w0 | w1 | w2 | w3 | w4 | w5 | w6 | w7;
        if any_true == 0 {
            idx += 64;
            continue;
        }

        let all_true = w0 & w1 & w2 & w3 & w4 & w5 & w6 & w7;
        if all_true == ALL_TRUE_U64
            && w0 == ALL_TRUE_U64
            && w1 == ALL_TRUE_U64
            && w2 == ALL_TRUE_U64
            && w3 == ALL_TRUE_U64
            && w4 == ALL_TRUE_U64
            && w5 == ALL_TRUE_U64
            && w6 == ALL_TRUE_U64
            && w7 == ALL_TRUE_U64
        {
            selected.reserve(64);
            for i in 0..64 {
                selected.push(base + i);
            }
            idx += 64;
            continue;
        }

        // Mixed: process each 8-byte word
        let words = [w0, w1, w2, w3, w4, w5, w6, w7];
        for (w_i, &w) in words.iter().enumerate() {
            if w == 0 {
                continue;
            }
            let word_base = base + (w_i as u32) * 8;
            if w == ALL_TRUE_U64 {
                for i in 0..8 {
                    selected.push(word_base + i);
                }
                continue;
            }
            let bytes = w.to_ne_bytes();
            for (b_i, &b) in bytes.iter().enumerate() {
                if b != 0 {
                    selected.push(word_base + b_i as u32);
                }
            }
        }

        idx += 64;
    }

    // Remainder
    for (i, (&d, &v)) in data[idx..len].iter().zip(&val[idx..len]).enumerate() {
        if d && v {
            selected.push((idx + i) as u32);
        }
    }

    selected
}
