use crate::mvcc_common::{LmdbError, PageID, Result};

/// Local Position type used by the MVCC tag index nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(pub u64);

/// Length in bytes of the hashed tag key used in tag index leaf/internal nodes
pub const TAG_HASH_LEN: usize = 8;

// ========================= Tag Index (by tag-hash) =========================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagsLeafNode {
    pub keys: Vec<[u8; TAG_HASH_LEN]>,
    // value tuple: (has_tag_tree flag 0/1, positions stored directly if no tag tree)
    pub values: Vec<(u8, Vec<Position>)>,
}

impl TagsLeafNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len + keys + values
        let mut total = 2 + self.keys.len() * TAG_HASH_LEN;
        for (flag, positions) in &self.values {
            let _ = flag; // 1 byte flag
            total += 1; // has_tag_tree flag
            total += 2; // positions len
            total += positions.len() * 8; // each Position is 8 bytes
        }
        total
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(self.calc_serialized_size());

        // keys_len
        out.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // keys
        for key in &self.keys {
            out.extend_from_slice(key);
        }

        // values
        for (flag, positions) in &self.values {
            // flag (1 byte)
            out.push(*flag);
            // positions length (2 bytes)
            out.extend_from_slice(&(positions.len() as u16).to_le_bytes());
            // positions
            for pos in positions {
                out.extend_from_slice(&pos.0.to_le_bytes());
            }
        }

        Ok(out)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        if slice.len() < 2 {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }

        // keys_len
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // keys
        let keys_bytes = 2 + keys_len * TAG_HASH_LEN;
        if slice.len() < keys_bytes {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least {} bytes for keys, got {}",
                keys_bytes,
                slice.len()
            )));
        }

        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + i * TAG_HASH_LEN;
            let mut key = [0u8; TAG_HASH_LEN];
            key.copy_from_slice(&slice[start..start + TAG_HASH_LEN]);
            keys.push(key);
        }

        // values
        let mut values = Vec::with_capacity(keys_len);
        let mut offset = keys_bytes;
        for _ in 0..keys_len {
            if offset + 3 > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading value header".to_string(),
                ));
            }
            // flag
            let flag = slice[offset];
            offset += 1;
            // positions len
            let positions_len = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
            offset += 2;

            // positions
            let need = positions_len * 8;
            if offset + need > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading positions".to_string(),
                ));
            }
            let mut positions = Vec::with_capacity(positions_len);
            for i in 0..positions_len {
                let p = offset + i * 8;
                let pos = u64::from_le_bytes([
                    slice[p],
                    slice[p + 1],
                    slice[p + 2],
                    slice[p + 3],
                    slice[p + 4],
                    slice[p + 5],
                    slice[p + 6],
                    slice[p + 7],
                ]);
                positions.push(Position(pos));
            }
            offset += need;

            values.push((flag, positions));
        }

        Ok(TagsLeafNode { keys, values })
    }

    pub fn pop_last_key_and_value(&mut self) -> Result<([u8; TAG_HASH_LEN], (u8, Vec<Position>))> {
        let last_key = self.keys.pop().ok_or_else(|| {
            LmdbError::DeserializationError("No keys to pop".to_string())
        })?;
        let last_value = self.values.pop().ok_or_else(|| {
            LmdbError::DeserializationError("No values to pop".to_string())
        })?;
        Ok((last_key, last_value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagsInternalNode {
    pub keys: Vec<[u8; TAG_HASH_LEN]>,
    pub child_ids: Vec<PageID>,
}

impl TagsInternalNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len + keys + child_ids (no len field, keys_len+1 implied)
        2 + (self.keys.len() * TAG_HASH_LEN) + (self.child_ids.len() * 8)
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(self.calc_serialized_size());
        // keys_len
        out.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());
        // keys
        for key in &self.keys {
            out.extend_from_slice(key);
        }
        // child_ids
        for id in &self.child_ids {
            out.extend_from_slice(&id.0.to_le_bytes());
        }
        Ok(out)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        if slice.len() < 2 {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;
        let keys_bytes = 2 + keys_len * TAG_HASH_LEN;
        if slice.len() < keys_bytes {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least {} bytes for keys, got {}",
                keys_bytes,
                slice.len()
            )));
        }
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + i * TAG_HASH_LEN;
            let mut key = [0u8; TAG_HASH_LEN];
            key.copy_from_slice(&slice[start..start + TAG_HASH_LEN]);
            keys.push(key);
        }

        let child_ids_len = keys_len + 1;
        let need = child_ids_len * 8;
        if slice.len() < keys_bytes + need {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least {} bytes for child_ids, got {}",
                keys_bytes + need,
                slice.len()
            )));
        }
        let mut child_ids = Vec::with_capacity(child_ids_len);
        for i in 0..child_ids_len {
            let p = keys_bytes + i * 8;
            let id = u64::from_le_bytes([
                slice[p],
                slice[p + 1],
                slice[p + 2],
                slice[p + 3],
                slice[p + 4],
                slice[p + 5],
                slice[p + 6],
                slice[p + 7],
            ]);
            child_ids.push(PageID(id));
        }

        Ok(TagsInternalNode { keys, child_ids })
    }

    pub fn replace_last_child_id(&mut self, old_id: PageID, new_id: PageID) -> Result<()> {
        let last_idx = self.child_ids.len() - 1;
        if self.child_ids[last_idx] == old_id {
            self.child_ids[last_idx] = new_id;
            Ok(())
        } else {
            Err(LmdbError::DatabaseCorrupted(
                "Child ID mismatch".to_string(),
            ))
        }
    }

    pub fn append_promoted_key_and_page_id(
        &mut self,
        promoted_key: [u8; TAG_HASH_LEN],
        promoted_page_id: PageID,
    ) -> Result<()> {
        self.keys.push(promoted_key);
        self.child_ids.push(promoted_page_id);
        Ok(())
    }

    pub(crate) fn split_off(
        &mut self,
    ) -> Result<([u8; TAG_HASH_LEN], Vec<[u8; TAG_HASH_LEN]>, Vec<PageID>)> {
        // Follow the same split approach as other MVCC internal nodes
        let middle_idx = self.keys.len() - 2;
        let promoted_key = self.keys.remove(middle_idx);
        let new_keys = self.keys.split_off(middle_idx);
        let new_child_ids = self.child_ids.split_off(middle_idx + 1);
        Ok((promoted_key, new_keys, new_child_ids))
    }
}

// ========================= Tag position subtree =========================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagLeafNode {
    pub positions: Vec<Position>,
    pub next_leaf_id: Option<PageID>,
}

impl TagLeafNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 8 bytes for next_leaf_id (0 indicates None) + 2 for positions_len + positions
        8 + 2 + (self.positions.len() * 8)
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(self.calc_serialized_size());
        // next_leaf_id as u64 (0 for None)
        let id = self.next_leaf_id.map(|p| p.0).unwrap_or(0);
        out.extend_from_slice(&id.to_le_bytes());
        // positions length
        out.extend_from_slice(&(self.positions.len() as u16).to_le_bytes());
        // positions
        for pos in &self.positions {
            out.extend_from_slice(&pos.0.to_le_bytes());
        }
        Ok(out)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        if slice.len() < 10 {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least 10 bytes, got {}",
                slice.len()
            )));
        }
        // next_leaf_id
        let id = u64::from_le_bytes([
            slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
        ]);
        let next_leaf_id = if id == 0 { None } else { Some(PageID(id)) };
        // positions len
        let positions_len = u16::from_le_bytes([slice[8], slice[9]]) as usize;
        let need = positions_len * 8;
        if slice.len() < 10 + need {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least {} bytes for positions, got {}",
                10 + need,
                slice.len()
            )));
        }
        let mut positions = Vec::with_capacity(positions_len);
        for i in 0..positions_len {
            let p = 10 + i * 8;
            let v = u64::from_le_bytes([
                slice[p],
                slice[p + 1],
                slice[p + 2],
                slice[p + 3],
                slice[p + 4],
                slice[p + 5],
                slice[p + 6],
                slice[p + 7],
            ]);
            positions.push(Position(v));
        }
        Ok(TagLeafNode {
            positions,
            next_leaf_id,
        })
    }

    pub fn pop_last_position(&mut self) -> Result<Position> {
        self.positions
            .pop()
            .ok_or_else(|| LmdbError::DeserializationError("No positions to pop".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagInternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

impl TagInternalNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len + 8 bytes per key + 8 bytes per child id (len implied)
        2 + (self.keys.len() * 8) + (self.child_ids.len() * 8)
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(self.calc_serialized_size());
        // keys_len
        out.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());
        // keys
        for k in &self.keys {
            out.extend_from_slice(&k.0.to_le_bytes());
        }
        // child_ids (no len)
        for id in &self.child_ids {
            out.extend_from_slice(&id.0.to_le_bytes());
        }
        Ok(out)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        if slice.len() < 2 {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;
        let keys_bytes = 2 + keys_len * 8;
        if slice.len() < keys_bytes {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least {} bytes for keys, got {}",
                keys_bytes,
                slice.len()
            )));
        }
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let p = 2 + i * 8;
            let v = u64::from_le_bytes([
                slice[p],
                slice[p + 1],
                slice[p + 2],
                slice[p + 3],
                slice[p + 4],
                slice[p + 5],
                slice[p + 6],
                slice[p + 7],
            ]);
            keys.push(Position(v));
        }

        let child_ids_len = keys_len + 1;
        let need = child_ids_len * 8;
        if slice.len() < keys_bytes + need {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least {} bytes for child_ids, got {}",
                keys_bytes + need,
                slice.len()
            )));
        }
        let mut child_ids = Vec::with_capacity(child_ids_len);
        for i in 0..child_ids_len {
            let p = keys_bytes + i * 8;
            let v = u64::from_le_bytes([
                slice[p],
                slice[p + 1],
                slice[p + 2],
                slice[p + 3],
                slice[p + 4],
                slice[p + 5],
                slice[p + 6],
                slice[p + 7],
            ]);
            child_ids.push(PageID(v));
        }

        Ok(TagInternalNode { keys, child_ids })
    }

    pub fn replace_last_child_id(&mut self, old_id: PageID, new_id: PageID) -> Result<()> {
        let last_idx = self.child_ids.len() - 1;
        if self.child_ids[last_idx] == old_id {
            self.child_ids[last_idx] = new_id;
            Ok(())
        } else {
            Err(LmdbError::DatabaseCorrupted(
                "Child ID mismatch".to_string(),
            ))
        }
    }

    pub fn append_promoted_key_and_page_id(
        &mut self,
        promoted_key: Position,
        promoted_page_id: PageID,
    ) -> Result<()> {
        self.keys.push(promoted_key);
        self.child_ids.push(promoted_page_id);
        Ok(())
    }

    pub(crate) fn split_off(&mut self) -> Result<(Position, Vec<Position>, Vec<PageID>)> {
        let middle_idx = self.keys.len() - 2;
        let promoted_key = self.keys.remove(middle_idx);
        let new_keys = self.keys.split_off(middle_idx);
        let new_child_ids = self.child_ids.split_off(middle_idx + 1);
        Ok((promoted_key, new_keys, new_child_ids))
    }
}

#[cfg(test)]
mod tests {
    use super::{TagsInternalNode, TagsLeafNode, Position, TagInternalNode, TagLeafNode, TAG_HASH_LEN};
    use crate::mvcc_common::PageID;

    #[test]
    fn test_tag_leaf_node_serialize_roundtrip() {
        let leaf = TagLeafNode {
            positions: vec![Position(10), Position(20), Position(30)],
            next_leaf_id: Some(PageID(1234)),
        };
        let ser = leaf.serialize().unwrap();
        let de = TagLeafNode::from_slice(&ser).unwrap();
        assert_eq!(leaf, de);
    }

    #[test]
    fn test_tag_internal_node_serialize_roundtrip() {
        let node = TagInternalNode {
            keys: vec![Position(5), Position(15), Position(25)],
            child_ids: vec![PageID(100), PageID(200), PageID(300), PageID(400)],
        };
        let ser = node.serialize().unwrap();
        let de = TagInternalNode::from_slice(&ser).unwrap();
        assert_eq!(node, de);
    }

    #[test]
    fn test_leaf_node_serialize_roundtrip() {
        let mut k1 = [0u8; TAG_HASH_LEN];
        let mut k2 = [0u8; TAG_HASH_LEN];
        let mut k3 = [0u8; TAG_HASH_LEN];
        k1.copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        k2.copy_from_slice(&[10, 20, 30, 40, 50, 60, 70, 80]);
        k3.copy_from_slice(&[11, 22, 33, 44, 55, 66, 77, 88]);

        let leaf = TagsLeafNode {
            keys: vec![k1, k2, k3],
            values: vec![
                (0, vec![Position(1), Position(2), Position(3)]),
                (1, vec![Position(100)]),
                (0, vec![]),
            ],
        };

        let ser = leaf.serialize().unwrap();
        let de = TagsLeafNode::from_slice(&ser).unwrap();
        assert_eq!(leaf, de);
    }

    #[test]
    fn test_internal_node_serialize_roundtrip() {
        let mut k1 = [0u8; TAG_HASH_LEN];
        let mut k2 = [0u8; TAG_HASH_LEN];
        let mut k3 = [0u8; TAG_HASH_LEN];
        k1.copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        k2.copy_from_slice(&[10, 20, 30, 40, 50, 60, 70, 80]);
        k3.copy_from_slice(&[11, 22, 33, 44, 55, 66, 77, 88]);

        let node = TagsInternalNode {
            keys: vec![k1, k2, k3],
            child_ids: vec![PageID(100), PageID(200), PageID(300), PageID(400)],
        };

        let ser = node.serialize().unwrap();
        let de = TagsInternalNode::from_slice(&ser).unwrap();
        assert_eq!(node, de);
    }
}
