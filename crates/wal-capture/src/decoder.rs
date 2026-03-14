use pgiceberg_common::models::*;
use std::collections::HashMap;
use tracing::trace;

/// Decodes raw pgoutput v2 binary messages into typed events.
///
/// pgwire-replication gives us raw XLogData bytes.  This module parses the
/// pgoutput protocol framing:
///   - 'B' = Begin
///   - 'C' = Commit
///   - 'R' = Relation
///   - 'I' = Insert
///   - 'U' = Update
///   - 'D' = Delete
///   - 'T' = Truncate
///   - 'Y' = Type
///   - 'O' = Origin

#[derive(Debug)]
pub enum PgOutputMessage {
    Begin {
        final_lsn: Lsn,
        commit_ts: i64,
        xid: u32,
    },
    Commit {
        flags: u8,
        commit_lsn: Lsn,
        end_lsn: Lsn,
        commit_ts: i64,
    },
    Relation {
        oid: u32,
        schema: String,
        name: String,
        replica_identity: u8,
        columns: Vec<RelationColumn>,
    },
    Insert {
        relation_oid: u32,
        tuple: TupleData,
    },
    Update {
        relation_oid: u32,
        old_tuple: Option<TupleData>,
        new_tuple: TupleData,
    },
    Delete {
        relation_oid: u32,
        old_tuple: TupleData,
    },
    Truncate {
        relation_oids: Vec<u32>,
    },
    Type {
        oid: u32,
        schema: String,
        name: String,
    },
    Origin {
        lsn: Lsn,
        name: String,
    },
    /// Message types we don't handle — skip silently.
    Unknown(u8),
}

#[derive(Debug, Clone)]
pub struct RelationColumn {
    pub flags: u8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug, Clone)]
pub struct TupleData {
    pub columns: Vec<TupleColumn>,
}

#[derive(Debug, Clone)]
pub enum TupleColumn {
    Null,
    UnchangedToast,
    Text(Vec<u8>),
    Binary(Vec<u8>),
}

/// Decode a single pgoutput message from raw bytes.
pub fn decode_pgoutput(data: &[u8]) -> anyhow::Result<PgOutputMessage> {
    if data.is_empty() {
        anyhow::bail!("Empty pgoutput message");
    }

    let msg_type = data[0];
    let body = &data[1..];

    match msg_type {
        b'B' => decode_begin(body),
        b'C' => decode_commit(body),
        b'R' => decode_relation(body),
        b'I' => decode_insert(body),
        b'U' => decode_update(body),
        b'D' => decode_delete(body),
        b'T' => decode_truncate(body),
        b'Y' => decode_type(body),
        b'O' => decode_origin(body),
        _ => {
            trace!(msg_type = msg_type, "Unknown pgoutput message type");
            Ok(PgOutputMessage::Unknown(msg_type))
        }
    }
}

fn read_u8(data: &[u8], pos: &mut usize) -> u8 {
    let v = data[*pos];
    *pos += 1;
    v
}

fn read_u16(data: &[u8], pos: &mut usize) -> u16 {
    let v = u16::from_be_bytes([data[*pos], data[*pos + 1]]);
    *pos += 2;
    v
}

fn read_u32(data: &[u8], pos: &mut usize) -> u32 {
    let v = u32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos += 4;
    v
}

fn read_i32(data: &[u8], pos: &mut usize) -> i32 {
    let v = i32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos += 4;
    v
}

fn read_u64(data: &[u8], pos: &mut usize) -> u64 {
    let v = u64::from_be_bytes([
        data[*pos],
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
        data[*pos + 4],
        data[*pos + 5],
        data[*pos + 6],
        data[*pos + 7],
    ]);
    *pos += 8;
    v
}

fn read_i64(data: &[u8], pos: &mut usize) -> i64 {
    read_u64(data, pos) as i64
}

fn read_string(data: &[u8], pos: &mut usize) -> String {
    let start = *pos;
    while *pos < data.len() && data[*pos] != 0 {
        *pos += 1;
    }
    let s = String::from_utf8_lossy(&data[start..*pos]).to_string();
    *pos += 1; // skip null terminator
    s
}

fn decode_begin(data: &[u8]) -> anyhow::Result<PgOutputMessage> {
    let mut pos = 0;
    let final_lsn = Lsn(read_u64(data, &mut pos));
    let commit_ts = read_i64(data, &mut pos);
    let xid = read_u32(data, &mut pos);
    Ok(PgOutputMessage::Begin {
        final_lsn,
        commit_ts,
        xid,
    })
}

fn decode_commit(data: &[u8]) -> anyhow::Result<PgOutputMessage> {
    let mut pos = 0;
    let flags = read_u8(data, &mut pos);
    let commit_lsn = Lsn(read_u64(data, &mut pos));
    let end_lsn = Lsn(read_u64(data, &mut pos));
    let commit_ts = read_i64(data, &mut pos);
    Ok(PgOutputMessage::Commit {
        flags,
        commit_lsn,
        end_lsn,
        commit_ts,
    })
}

fn decode_relation(data: &[u8]) -> anyhow::Result<PgOutputMessage> {
    let mut pos = 0;
    let oid = read_u32(data, &mut pos);
    let schema = read_string(data, &mut pos);
    let name = read_string(data, &mut pos);
    let replica_identity = read_u8(data, &mut pos);
    let ncols = read_u16(data, &mut pos) as usize;

    let mut columns = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let flags = read_u8(data, &mut pos);
        let col_name = read_string(data, &mut pos);
        let type_oid = read_u32(data, &mut pos);
        let type_modifier = read_i32(data, &mut pos);
        columns.push(RelationColumn {
            flags,
            name: col_name,
            type_oid,
            type_modifier,
        });
    }

    Ok(PgOutputMessage::Relation {
        oid,
        schema,
        name,
        replica_identity,
        columns,
    })
}

fn decode_tuple_data(data: &[u8], pos: &mut usize) -> anyhow::Result<TupleData> {
    let ncols = read_u16(data, pos) as usize;
    let mut columns = Vec::with_capacity(ncols);

    for _ in 0..ncols {
        let col_type = read_u8(data, pos);
        match col_type {
            b'n' => columns.push(TupleColumn::Null),
            b'u' => columns.push(TupleColumn::UnchangedToast),
            b't' => {
                let len = read_u32(data, pos) as usize;
                let value = data[*pos..*pos + len].to_vec();
                *pos += len;
                columns.push(TupleColumn::Text(value));
            }
            b'b' => {
                let len = read_u32(data, pos) as usize;
                let value = data[*pos..*pos + len].to_vec();
                *pos += len;
                columns.push(TupleColumn::Binary(value));
            }
            _ => anyhow::bail!("Unknown tuple column type: {}", col_type),
        }
    }

    Ok(TupleData { columns })
}

fn decode_insert(data: &[u8]) -> anyhow::Result<PgOutputMessage> {
    let mut pos = 0;
    let relation_oid = read_u32(data, &mut pos);
    let _new_flag = read_u8(data, &mut pos); // always 'N'
    let tuple = decode_tuple_data(data, &mut pos)?;
    Ok(PgOutputMessage::Insert {
        relation_oid,
        tuple,
    })
}

fn decode_update(data: &[u8]) -> anyhow::Result<PgOutputMessage> {
    let mut pos = 0;
    let relation_oid = read_u32(data, &mut pos);

    let flag = read_u8(data, &mut pos);
    let old_tuple = if flag == b'K' || flag == b'O' {
        let old = decode_tuple_data(data, &mut pos)?;
        let _new_flag = read_u8(data, &mut pos); // 'N'
        Some(old)
    } else {
        None
    };

    let new_tuple = decode_tuple_data(data, &mut pos)?;

    Ok(PgOutputMessage::Update {
        relation_oid,
        old_tuple,
        new_tuple,
    })
}

fn decode_delete(data: &[u8]) -> anyhow::Result<PgOutputMessage> {
    let mut pos = 0;
    let relation_oid = read_u32(data, &mut pos);
    let _key_flag = read_u8(data, &mut pos); // 'K' or 'O'
    let old_tuple = decode_tuple_data(data, &mut pos)?;
    Ok(PgOutputMessage::Delete {
        relation_oid,
        old_tuple,
    })
}

fn decode_truncate(data: &[u8]) -> anyhow::Result<PgOutputMessage> {
    let mut pos = 0;
    let nrels = read_u32(data, &mut pos) as usize;
    let _options = read_u8(data, &mut pos);
    let mut oids = Vec::with_capacity(nrels);
    for _ in 0..nrels {
        oids.push(read_u32(data, &mut pos));
    }
    Ok(PgOutputMessage::Truncate {
        relation_oids: oids,
    })
}

fn decode_type(data: &[u8]) -> anyhow::Result<PgOutputMessage> {
    let mut pos = 0;
    let oid = read_u32(data, &mut pos);
    let schema = read_string(data, &mut pos);
    let name = read_string(data, &mut pos);
    Ok(PgOutputMessage::Type { oid, schema, name })
}

fn decode_origin(data: &[u8]) -> anyhow::Result<PgOutputMessage> {
    let mut pos = 0;
    let lsn = Lsn(read_u64(data, &mut pos));
    let name = read_string(data, &mut pos);
    Ok(PgOutputMessage::Origin { lsn, name })
}

/// Manages OID → RelationInfo mapping.  Populated by Relation messages in the
/// WAL stream.  Service 1 uses this to know which table a given Insert/Update/
/// Delete belongs to.
pub struct RelationCache {
    pub relations: HashMap<u32, CachedRelation>,
}

#[derive(Debug, Clone)]
pub struct CachedRelation {
    pub schema: String,
    pub name: String,
    pub columns: Vec<RelationColumn>,
}

impl RelationCache {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
        }
    }

    pub fn update(&mut self, oid: u32, schema: String, name: String, columns: Vec<RelationColumn>) {
        self.relations.insert(
            oid,
            CachedRelation {
                schema,
                name,
                columns,
            },
        );
    }

    pub fn get(&self, oid: u32) -> Option<&CachedRelation> {
        self.relations.get(&oid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_begin_bytes(lsn: u64, ts: i64, xid: u32) -> Vec<u8> {
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&lsn.to_be_bytes());
        buf.extend_from_slice(&ts.to_be_bytes());
        buf.extend_from_slice(&xid.to_be_bytes());
        buf
    }

    fn make_commit_bytes(flags: u8, commit_lsn: u64, end_lsn: u64, ts: i64) -> Vec<u8> {
        let mut buf = vec![b'C', flags];
        buf.extend_from_slice(&commit_lsn.to_be_bytes());
        buf.extend_from_slice(&end_lsn.to_be_bytes());
        buf.extend_from_slice(&ts.to_be_bytes());
        buf
    }

    #[test]
    fn test_decode_begin() {
        let data = make_begin_bytes(0x16B6C50, 1000000, 42);
        let msg = decode_pgoutput(&data).unwrap();
        match msg {
            PgOutputMessage::Begin {
                final_lsn,
                commit_ts,
                xid,
            } => {
                assert_eq!(final_lsn.0, 0x16B6C50);
                assert_eq!(commit_ts, 1000000);
                assert_eq!(xid, 42);
            }
            _ => panic!("Expected Begin, got {:?}", msg),
        }
    }

    #[test]
    fn test_decode_commit() {
        let data = make_commit_bytes(0, 100, 200, 999);
        let msg = decode_pgoutput(&data).unwrap();
        match msg {
            PgOutputMessage::Commit {
                commit_lsn,
                end_lsn,
                commit_ts,
                ..
            } => {
                assert_eq!(commit_lsn.0, 100);
                assert_eq!(end_lsn.0, 200);
                assert_eq!(commit_ts, 999);
            }
            _ => panic!("Expected Commit"),
        }
    }

    #[test]
    fn test_decode_unknown() {
        let data = vec![b'Z', 1, 2, 3];
        let msg = decode_pgoutput(&data).unwrap();
        match msg {
            PgOutputMessage::Unknown(t) => assert_eq!(t, b'Z'),
            _ => panic!("Expected Unknown"),
        }
    }

    #[test]
    fn test_relation_cache() {
        let mut cache = RelationCache::new();
        cache.update(
            12345,
            "public".to_string(),
            "users".to_string(),
            vec![RelationColumn {
                flags: 1,
                name: "id".to_string(),
                type_oid: 23,
                type_modifier: -1,
            }],
        );

        let rel = cache.get(12345).unwrap();
        assert_eq!(rel.schema, "public");
        assert_eq!(rel.name, "users");
        assert_eq!(rel.columns[0].name, "id");

        assert!(cache.get(99999).is_none());
    }
}
