use std::{collections::VecDeque, fmt::Debug};

use serde::{Deserialize, Serialize};

use crate::{
    error::Error,
    page::{PageId, PageTable, PageTableOptions, PageUpdateGuard},
    vfs::Vfs,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Node {
    EmptyRoot,
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl Node {
    fn internal(&self, page_id: PageId) -> Result<&InternalNode, Error> {
        if let Self::Internal(internal_node) = self {
            Ok(internal_node)
        } else {
            Err(Error::InvalidPageData {
                page: page_id,
                message: "not a internal node",
            })
        }
    }

    fn internal_mut(&mut self, page_id: PageId) -> Result<&mut InternalNode, Error> {
        if let Self::Internal(internal_node) = self {
            Ok(internal_node)
        } else {
            Err(Error::InvalidPageData {
                page: page_id,
                message: "not an internal node",
            })
        }
    }

    fn leaf(&self, page_id: PageId) -> Result<&LeafNode, Error> {
        if let Self::Leaf(leaf_node) = self {
            Ok(leaf_node)
        } else {
            Err(Error::InvalidPageData {
                page: page_id,
                message: "not a leaf node",
            })
        }
    }

    fn leaf_mut(&mut self, page_id: PageId) -> Result<&mut LeafNode, Error> {
        if let Self::Leaf(leaf_node) = self {
            Ok(leaf_node)
        } else {
            Err(Error::InvalidPageData {
                page: page_id,
                message: "not a leaf node",
            })
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct InternalNode {
    keys: Vec<Vec<u8>>,
    children: Vec<PageId>,
}

impl InternalNode {
    pub fn new(keys: Vec<Vec<u8>>, children: Vec<PageId>) -> Self {
        assert!(keys.len() + 1 == children.len());
        assert!(!keys.is_empty());
        assert!(is_sorted(&keys));

        Self { keys, children }
    }

    pub fn keys_len(&self) -> usize {
        self.keys.len()
    }

    pub fn _keys_is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[cfg(test)]
    pub fn keys(&self) -> &[Vec<u8>] {
        &self.keys
    }

    pub fn children(&self) -> &[PageId] {
        &self.children
    }

    pub fn verify(&self) -> Option<&'static str> {
        if self.keys.is_empty() || self.children.is_empty() {
            Some("empty key or children")
        } else if self.keys.len() + 1 != self.children.len() {
            Some("key children length mismatch")
        } else if !is_sorted(&self.keys) {
            Some("keys not sorted")
        } else {
            None
        }
    }

    fn search(&self, key: &[u8]) -> Result<usize, usize> {
        self.keys.binary_search_by(|item| (&item[..]).cmp(key))
    }

    pub fn find_child(&self, key: &[u8]) -> PageId {
        assert!(self.keys.len() + 1 == self.children.len());

        match self.search(key) {
            Ok(index) => self.children[index + 1],
            Err(index) => self.children[index],
        }
    }

    pub fn insert_child(&mut self, child_key: Vec<u8>, child_id: PageId) {
        assert!(self.keys.len() + 1 == self.children.len());

        match self.search(&child_key) {
            Ok(_) => {
                panic!("key already exists");
            }
            Err(index) => {
                self.keys.insert(index, child_key);
                self.children.insert(index + 1, child_id);
            }
        }
    }

    pub fn split(&mut self) -> (Vec<u8>, InternalNode) {
        assert!(self.keys.len() >= 3);
        assert!(self.keys.len() + 1 == self.children.len());

        let num_keep = (self.keys.len() as f64 / 2.0).ceil() as usize;

        let adjacent_keys = self.keys.split_off(num_keep);
        let new_parent_key = self.keys.pop().unwrap();

        let adjacent_children = self.children.split_off(num_keep);

        assert!(self.keys.len() + 1 == self.children.len());
        assert!(adjacent_keys.len() + 1 == adjacent_children.len());

        let adjacent_node = InternalNode {
            keys: adjacent_keys,
            children: adjacent_children,
        };

        (new_parent_key, adjacent_node)
    }
}

impl Debug for InternalNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{keys={} ", self.keys.len())?;

        for index in 0..self.keys.len() {
            write!(
                f,
                "({}) {:?} ",
                self.children.get(index).unwrap_or(&PageId::MAX),
                String::from_utf8_lossy(self.keys.get(index).unwrap_or(&Vec::new()))
            )?;
        }
        write!(
            f,
            "({})",
            self.children.get(self.keys.len()).unwrap_or(&PageId::MAX)
        )?;

        write!(f, " }}")
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct LeafNode {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    next_leaf: Option<PageId>,
}

impl LeafNode {
    #[cfg(test)]
    pub fn new(keys: Vec<Vec<u8>>, values: Vec<Vec<u8>>) -> Self {
        assert!(keys.len() == values.len());
        assert!(!keys.is_empty());
        assert!(is_sorted(&keys));

        Self {
            keys,
            values,
            next_leaf: None,
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn first_key(&self) -> Option<&[u8]> {
        self.keys.first().map(|item| item.as_slice())
    }

    pub fn next_leaf(&self) -> Option<PageId> {
        self.next_leaf
    }

    pub fn set_next_leaf(&mut self, value: Option<PageId>) {
        self.next_leaf = value;
    }

    pub fn verify(&self) -> Option<&'static str> {
        // if self.keys.is_empty() || self.values.is_empty() {
        //     Some("empty keys or values")
        // } else
        if self.keys.len() != self.values.len() {
            Some("key value length mismatch")
        } else if !is_sorted(&self.keys) {
            Some("keys not sorted")
        } else {
            None
        }
    }

    fn search(&self, key: &[u8]) -> Result<usize, usize> {
        self.keys.binary_search_by(|item| (&item[..]).cmp(key))
    }

    pub fn find_value(&self, key: &[u8]) -> Option<&[u8]> {
        assert!(self.keys.len() == self.values.len());

        match self.search(key) {
            Ok(index) => Some(&self.values[index]),
            Err(_) => None,
        }
    }

    pub fn find_index(&self, key: &[u8]) -> usize {
        assert!(self.keys.len() == self.values.len());

        match self.search(key) {
            Ok(index) => index,
            Err(index) => index,
        }
    }

    pub fn get(&self, index: usize) -> (&[u8], &[u8]) {
        (&self.keys[index], &self.values[index])
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        assert!(self.keys.len() == self.values.len());

        match self.search(&key) {
            Ok(index) => {
                self.values[index] = value;
            }
            Err(index) => {
                self.keys.insert(index, key);
                self.values.insert(index, value);
            }
        }
    }

    pub fn remove_key(&mut self, key: &[u8]) {
        if let Ok(index) = self.search(key) {
            self.keys.remove(index);
            self.values.remove(index);
        }
    }

    pub fn split(&mut self) -> LeafNode {
        assert!(self.keys.len() >= 2);
        assert!(self.keys.len() == self.values.len());

        let num_keep = self.keys.len() / 2;

        LeafNode {
            keys: self.keys.split_off(num_keep),
            values: self.values.split_off(num_keep),
            next_leaf: self.next_leaf,
        }
    }
}

impl Debug for LeafNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{keys={} ", self.keys.len())?;

        if let Some(next_leaf) = self.next_leaf {
            write!(f, "next_leaf={:?} ", next_leaf)?;
        }

        for index in 0..self.keys.len() {
            write!(
                f,
                "{:?},",
                String::from_utf8_lossy(self.keys.get(index).unwrap_or(&Vec::new()))
            )?;
        }

        write!(f, " }}")
    }
}

pub struct Tree {
    page_table: PageTable<Node>,
    keys_per_node: usize,
}

impl Tree {
    pub fn open(
        vfs: Box<dyn Vfs + Sync + Send>,
        page_table_options: PageTableOptions,
        keys_per_node: usize,
    ) -> Result<Self, Error> {
        assert!(keys_per_node >= 2);

        Ok(Self {
            page_table: PageTable::open(vfs, page_table_options)?,
            keys_per_node,
        })
    }

    pub fn init_if_empty(&mut self) -> Result<(), Error> {
        let root_id = self.page_table.root_id();

        if root_id.is_none() {
            let page_id = self.page_table.new_page_id();
            self.page_table.put(page_id, Node::EmptyRoot)?;
            self.page_table.set_root_id(Some(page_id));
        }

        Ok(())
    }

    pub fn contains_key(&mut self, key: &[u8]) -> Result<bool, Error> {
        let page_id = match self.find_leaf_node(key, None)? {
            Some(page_id) => page_id,
            None => return Ok(false),
        };

        let leaf_node = self.read_node(page_id)?.leaf(page_id)?;

        match leaf_node.find_value(key) {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn get(&mut self, key: &[u8], value_destination: &mut Vec<u8>) -> Result<bool, Error> {
        let page_id = match self.find_leaf_node(key, None)? {
            Some(page_id) => page_id,
            None => return Ok(false),
        };

        let leaf_node = self.read_node(page_id)?.leaf(page_id)?;

        match leaf_node.find_value(key) {
            Some(data) => {
                value_destination.resize(data.len(), 0);
                value_destination.copy_from_slice(data);

                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let keys_per_node = self.keys_per_node;
        let mut node_path = Vec::new();

        if let Some(page_id) = self.find_leaf_node(&key, Some(&mut node_path))? {
            let num_keys = {
                let mut leaf_node_ = self.edit_node(page_id)?;
                let leaf_node = leaf_node_.leaf_mut(page_id)?;

                leaf_node.insert(key, value);
                leaf_node.len()
            };

            if num_keys > keys_per_node {
                self.split_leaf_node(page_id, &mut node_path)?;
            }
        } else {
            self.add_new_root_leaf_node(key, value)?;
        };

        Ok(())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<(), Error> {
        let page_id = match self.find_leaf_node(&key, None)? {
            Some(page_id) => page_id,
            None => return Ok(()),
        };

        let num_keys = {
            let mut leaf_node_ = self.edit_node(page_id)?;
            let leaf_node = leaf_node_.leaf_mut(page_id)?;

            leaf_node.remove_key(key);
            leaf_node.len()
        };

        // TODO: remove empty nodes

        Ok(())
    }

    pub fn cursor_start(&mut self, cursor: &mut TreeCursor, start_key: &[u8]) -> Result<(), Error> {
        match self.find_leaf_node(start_key, None)? {
            Some(page_id) => {
                let leaf_node = self.read_node(page_id)?.leaf(page_id)?.clone();
                cursor.key_index = leaf_node.find_index(start_key);
                cursor.leaf_node = Some(leaf_node);
            }
            None => {
                cursor.leaf_node = None;
            }
        }

        Ok(())
    }

    pub fn cursor_next(
        &mut self,
        cursor: &mut TreeCursor,
        key_buffer: &mut Vec<u8>,
        value_buffer: &mut Vec<u8>,
        range_end: &Option<Vec<u8>>,
    ) -> Result<bool, Error> {
        self.cursor_load_next_leaf_node(cursor)?;

        if let Some(leaf_node) = &cursor.leaf_node {
            let (key, value) = leaf_node.get(cursor.key_index);

            if let Some(range_end) = range_end {
                if key >= range_end {
                    return Ok(false);
                }
            }

            cursor.key_index += 1;

            key_buffer.resize(key.len(), 0);
            key_buffer.copy_from_slice(&key);
            value_buffer.resize(value.len(), 0);
            value_buffer.copy_from_slice(&value);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn cursor_load_next_leaf_node(&mut self, cursor: &mut TreeCursor) -> Result<(), Error> {
        // Loop is required since leaf nodes are allowed to be empty.
        while let Some(leaf_node) = &cursor.leaf_node {
            if cursor.key_index >= leaf_node.len() {
                cursor.key_index = 0;

                match leaf_node.next_leaf() {
                    Some(page_id) => {
                        let next_leaf_node = self.read_node(page_id)?.leaf(page_id)?.clone();
                        cursor.leaf_node = Some(next_leaf_node);
                    }
                    None => {
                        cursor.leaf_node = None;
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        self.page_table.commit()
    }

    pub fn dump_tree(&mut self) -> Result<(), Error> {
        let page_id = self.page_table.root_id().unwrap();
        let mut page_queue = VecDeque::new();

        page_queue.push_back(page_id);

        eprintln!("Root page: {}", page_id);

        while let Some(page_id) = page_queue.pop_front() {
            let node = self.read_node(page_id)?;

            eprintln!("Page {}: {:?}", page_id, &node);

            match node {
                Node::EmptyRoot => {}
                Node::Internal(internal_node) => {
                    for page_id in internal_node.children() {
                        page_queue.push_back(*page_id);
                    }
                }
                Node::Leaf(_) => {}
            }
        }

        Ok(())
    }

    fn find_leaf_node(
        &mut self,
        key: &[u8],
        mut path: Option<&mut Vec<PageId>>,
    ) -> Result<Option<PageId>, Error> {
        let mut page_id = match self.page_table.root_id() {
            Some(page_id) => page_id,
            None => return Ok(None),
        };

        for _ in 0..u16::MAX {
            let node = self.read_node(page_id)?;

            match node {
                Node::EmptyRoot => return Ok(None),
                Node::Internal(internal_node) => {
                    if let Some(ref mut path) = path {
                        path.push(page_id);
                    }

                    debug_assert_eq!(internal_node.verify(), None);
                    page_id = internal_node.find_child(key);
                }
                Node::Leaf(leaf_node) => {
                    debug_assert_eq!(leaf_node.verify(), None);

                    return Ok(Some(page_id));
                }
            }
        }

        Err(Error::LimitExceeded)
    }

    fn read_node(&mut self, page_id: PageId) -> Result<&Node, Error> {
        if let Some(node) = self.page_table.get(page_id)? {
            Ok(node)
        } else {
            Err(Error::InvalidPageData {
                page: page_id,
                message: "page missing",
            })
        }
    }

    fn edit_node(&mut self, page_id: PageId) -> Result<PageUpdateGuard<Node>, Error> {
        if let Some(node) = self.page_table.update(page_id)? {
            Ok(node)
        } else {
            Err(Error::InvalidPageData {
                page: page_id,
                message: "page missing",
            })
        }
    }

    fn add_new_root_leaf_node(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let page_id = self.page_table.new_page_id();
        let mut leaf_node = LeafNode::default();
        leaf_node.insert(key, value);

        self.page_table.put(page_id, Node::Leaf(leaf_node))?;
        self.page_table.set_root_id(Some(page_id));

        Ok(())
    }

    fn split_leaf_node(
        &mut self,
        leaf_node_id: PageId,
        node_path: &mut Vec<PageId>,
    ) -> Result<(), Error> {
        let adjacent_leaf_node_id = self.page_table.new_page_id();

        let mut leaf_node_ = self.edit_node(leaf_node_id)?;
        let leaf_node = leaf_node_.leaf_mut(leaf_node_id)?;

        let adjacent_leaf_node = leaf_node.split();
        let adjacent_leaf_first_key = adjacent_leaf_node.first_key().unwrap().to_vec();

        leaf_node.set_next_leaf(Some(adjacent_leaf_node_id));

        drop(leaf_node_);

        self.page_table
            .put(adjacent_leaf_node_id, Node::Leaf(adjacent_leaf_node))?;

        if let Some(parent_id) = node_path.pop() {
            let parent_key_len = self.connect_leaf_to_parent(
                parent_id,
                adjacent_leaf_first_key,
                adjacent_leaf_node_id,
            )?;

            if parent_key_len > self.keys_per_node {
                self.split_internal_node(parent_id, node_path)?;
            }
        } else {
            self.make_parent_node_of_two_leaf_nodes(leaf_node_id, adjacent_leaf_node_id)?;
        }

        Ok(())
    }

    fn connect_leaf_to_parent(
        &mut self,
        parent_node_id: PageId,
        leaf_first_key: Vec<u8>,
        leaf_id: PageId,
    ) -> Result<usize, Error> {
        let mut parent_node = self.edit_node(parent_node_id)?;
        let parent_node = parent_node.internal_mut(parent_node_id)?;

        parent_node.insert_child(leaf_first_key, leaf_id);

        Ok(parent_node.keys_len())
    }

    fn make_parent_node_of_two_leaf_nodes(
        &mut self,
        left_child_id: PageId,
        right_child_id: PageId,
    ) -> Result<(), Error> {
        let right_child = self.read_node(right_child_id)?.leaf(right_child_id)?;
        let key = right_child.first_key().unwrap().to_vec();

        let parent_node_id = self.page_table.new_page_id();
        let parent_node = InternalNode::new(vec![key], vec![left_child_id, right_child_id]);

        self.page_table
            .put(parent_node_id, Node::Internal(parent_node))?;
        self.page_table.set_root_id(Some(parent_node_id));

        Ok(())
    }

    fn split_internal_node(
        &mut self,
        internal_node_id: PageId,
        node_path: &mut Vec<PageId>,
    ) -> Result<(), Error> {
        let adjacent_internal_node_id = self.page_table.new_page_id();

        let mut internal_node_ = self.edit_node(internal_node_id)?;
        let internal_node = internal_node_.internal_mut(internal_node_id)?;

        let (key, adjacent_internal_node) = internal_node.split();

        drop(internal_node_);

        self.page_table.put(
            adjacent_internal_node_id,
            Node::Internal(adjacent_internal_node),
        )?;

        if let Some(parent_id) = node_path.pop() {
            let parent_key_len = self.reconnect_split_internal_node_to_parent(
                parent_id,
                key,
                adjacent_internal_node_id,
            )?;

            if parent_key_len > self.keys_per_node {
                self.split_internal_node(parent_id, node_path)?;
            }
        } else {
            self.make_parent_node_of_two_nodes(key, internal_node_id, adjacent_internal_node_id)?;
        }

        Ok(())
    }

    fn reconnect_split_internal_node_to_parent(
        &mut self,
        parent_node_id: PageId,
        key: Vec<u8>,
        right_child_id: PageId,
    ) -> Result<usize, Error> {
        let mut parent_node_ = self.edit_node(parent_node_id)?;
        let parent_node = parent_node_.internal_mut(parent_node_id)?;
        parent_node.insert_child(key, right_child_id);

        Ok(parent_node.keys_len())
    }

    fn make_parent_node_of_two_nodes(
        &mut self,
        parent_key: Vec<u8>,
        left_child_id: PageId,
        right_child_id: PageId,
    ) -> Result<(), Error> {
        let parent_node = InternalNode::new(vec![parent_key], vec![left_child_id, right_child_id]);
        let parent_node_id = self.page_table.new_page_id();

        self.page_table
            .put(parent_node_id, Node::Internal(parent_node))?;
        self.page_table.set_root_id(Some(parent_node_id));

        Ok(())
    }
}

#[derive(Default)]
pub struct TreeCursor {
    leaf_node: Option<LeafNode>,
    key_index: usize,
}

fn is_sorted<T>(data: &[T]) -> bool
where
    T: Ord,
{
    // https://stackoverflow.com/a/51272639/1524507
    data.windows(2).all(|w| w[0] <= w[1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_insert_find() {
        let mut node = LeafNode::new(vec![b"key1".to_vec()], vec![b"value1".to_vec()]);
        assert_eq!(node.len(), 1);

        node.insert(b"key2".to_vec(), b"value2".to_vec());
        assert_eq!(node.len(), 2);

        node.insert(b"key1".to_vec(), b"value3".to_vec());
        assert_eq!(node.len(), 2);

        let value = node.find_value(&b"key1".to_vec()).unwrap();
        assert_eq!(value, b"value3");

        let value = node.find_value(&b"key2".to_vec()).unwrap();
        assert_eq!(value, b"value2");

        let value = node.find_value(&b"non exist".to_vec());
        assert!(value.is_none());
    }

    #[test]
    fn test_leaf_node_split() {
        let mut node = LeafNode::new(
            vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()],
            vec![b"value1".to_vec(), b"value2".to_vec(), b"value3".to_vec()],
        );
        node.set_next_leaf(Some(456));

        let adjacent_node = node.split();

        assert_eq!(node.len(), 1);
        assert_eq!(adjacent_node.len(), 2);

        assert_eq!(node.first_key(), Some(&b"key1"[..]));
        assert_eq!(adjacent_node.first_key(), Some(&b"key2"[..]));
    }

    #[test]
    fn test_internal_node_insert_find() {
        let mut node = InternalNode::new(vec![b"key100".to_vec()], vec![4, 8]);
        assert_eq!(node.keys_len(), 1);

        node.insert_child(b"key200".to_vec(), 12);

        assert_eq!(node.keys_len(), 2);
        assert_eq!(node.keys(), vec![b"key100", b"key200"]);
        assert_eq!(node.children(), vec![4, 8, 12]);

        assert_eq!(node.find_child(b"key000"), 4);
        assert_eq!(node.find_child(b"key100"), 8);
        assert_eq!(node.find_child(b"key150"), 8);
        assert_eq!(node.find_child(b"key200"), 12);
        assert_eq!(node.find_child(b"key250"), 12);
    }

    #[test]
    fn test_internal_node_split_odd() {
        let mut node = InternalNode::new(
            vec![b"key100".to_vec(), b"key200".to_vec(), b"key300".to_vec()],
            vec![4, 8, 12, 16],
        );

        let (parent_key, adjacent_node) = node.split();

        assert_eq!(node.keys(), vec![b"key100"]);
        assert_eq!(node.children(), vec![4, 8]);

        assert_eq!(parent_key, b"key200");

        assert_eq!(adjacent_node.keys(), vec![b"key300"]);
        assert_eq!(adjacent_node.children(), vec![12, 16]);
    }

    #[test]
    fn test_internal_node_split_event() {
        let mut node = InternalNode::new(
            vec![
                b"key100".to_vec(),
                b"key200".to_vec(),
                b"key300".to_vec(),
                b"key400".to_vec(),
            ],
            vec![4, 8, 12, 16, 20],
        );

        let (parent_key, adjacent_node) = node.split();

        assert_eq!(node.keys(), vec![b"key100"]);
        assert_eq!(node.children(), vec![4, 8]);

        assert_eq!(parent_key, b"key200");

        assert_eq!(adjacent_node.keys(), vec![b"key300", b"key400"]);
        assert_eq!(adjacent_node.children(), vec![12, 16, 20]);
    }
}
