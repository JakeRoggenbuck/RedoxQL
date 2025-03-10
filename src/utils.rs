pub fn encode_bytes_to_ints(bytes: Vec<u8>) -> Vec<i64> {
    let mut i = 0u64;
    let mut value = 0u64;
    let mut string = vec![];

    for b in bytes {
        // Add the byte to the u64 number
        value |= (b as u64) << i;
        i += 8;

        if i == 64 {
            string.push(value as i64);
            value = 0;
            i = 0;
        }
    }

    if value != 0 {
        string.push(value as i64);
    }

    string
}

pub fn encode_str_to_ints(text: &str) -> Vec<i64> {
    let bytes = text.as_bytes();
    encode_bytes_to_ints(bytes.to_vec())
}

pub fn decode_bytes_from_ints(ints: Vec<i64>) -> Vec<u8> {
    let mut out_ints = vec![];

    for v in ints.into_iter() {
        for i in (0..64).step_by(8) {
            let c = ((v as u64) >> i) & 0xFF;
            if c == 0 {
                break;
            }

            out_ints.push(c as u8);
        }
    }

    return out_ints;
}

pub fn decode_string_from_ints(ints: Vec<i64>) -> String {
    let out_ints = decode_bytes_from_ints(ints);

    let s = String::from_utf8(out_ints);
    match s {
        Ok(a) => a,
        Err(_) => String::new(),
    }
}
