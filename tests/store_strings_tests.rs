use log::debug;
use redoxql::database::RDatabase;
use redoxql::query::RQuery;

fn encode_to_ints(text: &str) -> Vec<i64> {
    let bytes = text.as_bytes();

    let mut i = 0u64;
    let mut value = 0u64;
    let mut string = vec![];

    for b in bytes {
        // Add the byte to the u64 number
        value |= (*b as u64) << i;
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

fn decode_from_ints(ints: Vec<i64>) -> String {
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

    let s = String::from_utf8(out_ints);
    match s {
        Ok(a) => a,
        Err(_) => String::new(),
    }
}

#[test]
fn store_strings() {
    let mut db = RDatabase::new();
    let table_ref = db.create_table("Books".to_string(), 64, 0);
    let mut q = RQuery::new(table_ref);

    let name = "Cracking the Coding Interview";

    let a = encode_to_ints(name);
    debug!("{:?}", a);

    let b = decode_from_ints(a.clone());
    debug!("{:?}", b);

    // Insert string into db
    let mut str = vec![0; 64 - a.len()];
    str.extend(a);
    q.insert(str);

    let v = q.select(0, 0, vec![1; 64]);
    let mut vals = vec![];
    for x in &v.unwrap()[0].as_mut().unwrap().columns {
        if let Some(d) = x {
            vals.push(*d);
        }
    }

    assert_eq!(decode_from_ints(vals), "Cracking the Coding Interview");
}
