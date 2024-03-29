use std::{collections::VecDeque, fmt::Debug, ops::RangeBounds};

use serde::{Deserialize, Serialize};

use crate::{
    error::Error,
    page::{PageId, PageTable, PageTableOptions, PageUpdateGuard},
    vfs::Vfs,
};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TreeMetadata {
    pub key_value_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Node {
    EmptyRoot,
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl Node {
    fn _internal(&self, page_id: PageId) -> Result<&InternalNode, Error> {
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

    pub fn keys_is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn keys(&self) -> &[Vec<u8>] {
        &self.keys
    }

    pub fn children(&self) -> &[PageId] {
        &self.children
    }

    pub fn verify(&self) -> Option<&'static str> {
        // Empty is allowed for lazy deletion
        // if self.keys.is_empty() || self.children.is_empty() {
        //     Some("empty key or children")
        // } else
        if self.keys.len() + 1 != self.children.len() {
            Some("key children length mismatch")
        } else if !is_sorted(&self.keys) {
            Some("keys not sorted")
        } else {
            None
        }
    }

    pub fn verify_with_parent_keys(
        &self,
        parent_left_key: Option<&[u8]>,
        parent_right_key: Option<&[u8]>,
    ) -> Option<&'static str> {
        let result = verify_node_within_parent_keys(&self.keys, parent_left_key, parent_right_key);
        if result.is_some() {
            return result;
        }

        self.verify()
    }

    fn search(&self, key: &[u8]) -> Result<usize, usize> {
        self.keys.binary_search_by(|item| (&item[..]).cmp(key))
    }

    pub fn find_child(&self, key: &[u8]) -> PageId {
        debug_assert!(self.keys.len() + 1 == self.children.len());

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

    pub fn remove_child(&mut self, child_id: PageId) -> (Option<PageId>, Option<PageId>) {
        debug_assert!(self.keys.len() + 1 == self.children.len());

        let child_index = self.children.iter().position(|&id| id == child_id).unwrap();
        let key_index = child_index;

        assert_eq!(self.children.get(child_index).cloned(), Some(child_id));

        let left_page_id = if child_index == 0 {
            None
        } else {
            self.children.get(child_index - 1).cloned()
        };
        let right_page_id = self.children.get(child_index + 1).cloned();

        if child_index == self.children.len() - 1 {
            // If the child is the last, then remove the last key instead of
            // the non-existent lessor key
            self.keys.remove(key_index - 1);
        } else {
            // Remove the key that is greater
            self.keys.remove(key_index);
        }
        self.children.remove(child_index);

        (left_page_id, right_page_id)
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

    pub fn _is_empty(&self) -> bool {
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
        // Empty is allowed for lazy deletion
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

    pub fn verify_with_parent_keys(
        &self,
        parent_left_key: Option<&[u8]>,
        parent_right_key: Option<&[u8]>,
    ) -> Option<&'static str> {
        let result = verify_node_within_parent_keys(&self.keys, parent_left_key, parent_right_key);
        if result.is_some() {
            return result;
        }

        self.verify()
    }

    fn search(&self, key: &[u8]) -> Result<usize, usize> {
        self.keys.binary_search_by(|item| (&item[..]).cmp(key))
    }

    pub fn find_value(&self, key: &[u8]) -> Option<&[u8]> {
        debug_assert!(self.keys.len() == self.values.len());

        match self.search(key) {
            Ok(index) => Some(&self.values[index]),
            Err(_) => None,
        }
    }

    pub fn find_index(&self, key: &[u8]) -> usize {
        debug_assert!(self.keys.len() == self.values.len());

        match self.search(key) {
            Ok(index) => index,
            Err(index) => index,
        }
    }

    pub fn get(&self, index: usize) -> (&[u8], &[u8]) {
        (&self.keys[index], &self.values[index])
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> bool {
        assert!(self.keys.len() == self.values.len());

        match self.search(&key) {
            Ok(index) => {
                self.values[index] = value;
                true
            }
            Err(index) => {
                self.keys.insert(index, key);
                self.values.insert(index, value);
                false
            }
        }
    }

    pub fn remove_key(&mut self, key: &[u8]) -> bool {
        if let Ok(index) = self.search(key) {
            self.keys.remove(index);
            self.values.remove(index);
            true
        } else {
            false
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
    page_table: PageTable<Node, TreeMetadata>,
    keys_per_node: usize,
}

impl Tree {
    pub fn open(
        vfs: Box<dyn Vfs + Sync + Send>,
        page_table_options: PageTableOptions,
    ) -> Result<Self, Error> {
        assert!(page_table_options.keys_per_node >= 2);

        Ok(Self {
            keys_per_node: page_table_options.keys_per_node,
            page_table: PageTable::open(vfs, page_table_options)?,
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

    pub fn upgrade(&mut self) -> Result<(), Error> {
        if self.page_table.auxiliary_metadata().is_none() {
            self.page_table
                .set_auxiliary_metadata(Some(TreeMetadata::default()))
        }

        Ok(())
    }

    pub fn metadata(&self) -> Option<&TreeMetadata> {
        self.page_table.auxiliary_metadata()
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
            let (num_keys, replaced) = {
                let mut leaf_node_ = self.edit_node(page_id)?;
                let leaf_node = leaf_node_.leaf_mut(page_id)?;

                let replaced = leaf_node.insert(key, value);
                (leaf_node.len(), replaced)
            };

            if !replaced {
                self.increment_key_value_count();
            }

            if num_keys > keys_per_node {
                self.split_leaf_node(page_id, &mut node_path)?;
            }
        } else {
            self.increment_key_value_count();
            self.add_new_root_leaf_node(key, value)?;
        };

        Ok(())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<(), Error> {
        let mut node_path = Vec::new();

        let page_id = match self.find_leaf_node(key, Some(&mut node_path))? {
            Some(page_id) => page_id,
            None => return Ok(()),
        };

        let (num_keys, found) = {
            let mut leaf_node_ = self.edit_node(page_id)?;
            let leaf_node = leaf_node_.leaf_mut(page_id)?;

            let found = leaf_node.remove_key(key);
            (leaf_node.len(), found)
        };

        if found {
            self.decrement_key_value_count();
        }

        if num_keys == 0 {
            self.remove_leaf_node(page_id, &mut node_path)?;
        }

        // At this point, lazy deletion has occurred. But the invariants
        // of a traditional B+ tree is invalidated and the tree is
        // not balanced.

        // TODO: an operation that traverses the tree to re-balance itself
        // could be done here

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

    pub fn cursor_next<R>(
        &mut self,
        cursor: &mut TreeCursor,
        key_buffer: &mut Vec<u8>,
        value_buffer: &mut Vec<u8>,
        range: &R,
    ) -> Result<bool, Error>
    where
        R: RangeBounds<[u8]>,
    {
        self.cursor_load_next_leaf_node(cursor)?;

        if let Some(leaf_node) = &cursor.leaf_node {
            let (key, value) = leaf_node.get(cursor.key_index);

            if !range.contains(key) {
                return Ok(false);
            }

            cursor.key_index += 1;

            key_buffer.resize(key.len(), 0);
            key_buffer.copy_from_slice(key);
            value_buffer.resize(value.len(), 0);
            value_buffer.copy_from_slice(value);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn cursor_load_next_leaf_node(&mut self, cursor: &mut TreeCursor) -> Result<(), Error> {
        // Loop to find a non-empty leaf node is required since leaf nodes are allowed to be empty.
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

    pub fn verify_tree<P>(&mut self, mut progress_callback: P) -> Result<(), Error>
    where
        P: FnMut(usize, usize),
    {
        let page_id = if let Some(page_id) = self.page_table.root_id() {
            page_id
        } else {
            return Err(Error::InvalidMetadata {
                message: "missing root page ID",
            });
        };
        let mut current = 0usize;
        let mut total = 0usize;
        let mut page_queue = VecDeque::<(u64, Option<Vec<u8>>, Option<Vec<u8>>)>::new();

        page_queue.push_back((page_id, None, None));
        total += 1;

        while let Some((page_id, left_key, right_key)) = page_queue.pop_front() {
            let node = self.read_node(page_id)?;

            current += 1;
            progress_callback(current, total);

            match node {
                Node::EmptyRoot => {}
                Node::Internal(internal_node) => {
                    if let Some(message) = internal_node
                        .verify_with_parent_keys(left_key.as_deref(), right_key.as_deref())
                    {
                        return Err(Error::InvalidPageData {
                            page: page_id,
                            message,
                        });
                    }

                    for (index, page_id) in internal_node.children().iter().enumerate() {
                        let left_key = if index > 0 {
                            internal_node.keys().get(index - 1).cloned()
                        } else {
                            None
                        };
                        let right_key = internal_node.keys().get(index).cloned();

                        page_queue.push_back((*page_id, left_key, right_key));
                        total += 1;
                    }
                }
                Node::Leaf(leaf_node) => {
                    if let Some(message) =
                        leaf_node.verify_with_parent_keys(left_key.as_deref(), right_key.as_deref())
                    {
                        return Err(Error::InvalidPageData {
                            page: page_id,
                            message,
                        });
                    }
                }
            }
        }

        Ok(())
    }

    pub fn dump_tree(&mut self) -> Result<(), Error> {
        let page_id = self.page_table.root_id().unwrap();
        let mut page_queue = VecDeque::new();

        page_queue.push_back((page_id, 0));

        eprintln!("Root page: {}", page_id);

        while let Some((page_id, height)) = page_queue.pop_front() {
            let node = self.read_node(page_id)?;

            eprintln!("Page {}: {} {:?}", page_id, height, &node);

            match node {
                Node::EmptyRoot => {}
                Node::Internal(internal_node) => {
                    for page_id in internal_node.children() {
                        page_queue.push_back((*page_id, height + 1));
                    }
                }
                Node::Leaf(_) => {}
            }
        }

        Ok(())
    }

    // Find a leaf node
    //
    // Path is the list of parents to the leaf node. Path won't include the leaf.
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

    fn check_root_node_is_empty(&mut self) -> Result<(), Error> {
        let root_id = self.page_table.root_id().unwrap();
        let node = self.read_node(root_id)?;

        if let Node::EmptyRoot = node {
            Ok(())
        } else {
            Err(Error::InvalidPageData {
                page: root_id,
                message: "not root node",
            })
        }
    }

    // Set up a empty tree to have the root node as a leaf node.
    fn add_new_root_leaf_node(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        self.check_root_node_is_empty()?;

        let page_id = self
            .page_table
            .root_id()
            .unwrap_or_else(|| self.page_table.new_page_id());

        let mut leaf_node = LeafNode::default();
        leaf_node.insert(key, value);

        self.page_table.put(page_id, Node::Leaf(leaf_node))?;
        self.page_table.set_root_id(Some(page_id));

        Ok(())
    }

    // Split a leaf node into two, creating a new parent if needed
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

    // Make an internal node that is a parent of two leaf nodes.
    // Called when the root is a leaf node that has become split, and a internal
    // node is the new root.
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

    // Split internal node, promoting a key into a parent level
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

    // Make a new internal node become the root.
    // Called when the previous root internal node was newly split.
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

    fn remove_leaf_node(
        &mut self,
        leaf_node_id: PageId,
        node_path: &mut Vec<PageId>,
    ) -> Result<(), Error> {
        if let Some(parent_id) = node_path.pop() {
            // When the leaf node is a child of an internal node
            let adjacent_leafs =
                self.remove_child_from_internal_node(parent_id, leaf_node_id, node_path)?;
            self.join_leaf_nodes(adjacent_leafs.0, adjacent_leafs.1)?;
            self.page_table.remove(leaf_node_id)?;
        } else {
            // When the leaf node was also the root node
            assert_eq!(self.page_table.root_id(), Some(leaf_node_id));
            self.page_table.put(leaf_node_id, Node::EmptyRoot)?;
        }

        Ok(())
    }

    fn remove_child_from_internal_node(
        &mut self,
        internal_node_id: PageId,
        child_node_id: PageId,
        node_path: &mut Vec<PageId>,
    ) -> Result<(Option<PageId>, Option<PageId>), Error> {
        let mut internal_node_ = self.edit_node(internal_node_id)?;
        let internal_node = internal_node_.internal_mut(internal_node_id)?;

        if internal_node.keys_is_empty() {
            // The current internal node simply points to a single child node.
            // In this case, we can delete the node.
            let first_child_id = internal_node.children().first().cloned().unwrap();

            drop(internal_node_);

            assert_eq!(first_child_id, child_node_id);

            if let Some(parent_id) = node_path.pop() {
                // Tell the parent to remove us, then delete
                self.remove_child_from_internal_node(parent_id, internal_node_id, node_path)?;
                self.page_table.remove(internal_node_id)?;
            } else {
                // We're the root, so replace it with empty root node
                assert_eq!(self.page_table.root_id(), Some(internal_node_id));
                self.page_table.put(internal_node_id, Node::EmptyRoot)?;
            }

            Ok((None, None))
        } else {
            // Lazy remove the child node, allowing underflow (traditional B+tree invariants violated)
            let adjacent_nodes = internal_node.remove_child(child_node_id);
            Ok(adjacent_nodes)
        }
    }

    fn join_leaf_nodes(
        &mut self,
        left_leaf_id: Option<PageId>,
        right_leaf_id: Option<PageId>,
    ) -> Result<(), Error> {
        if let Some(left_leaf_id) = left_leaf_id {
            // { [...] , left , current , right, [...] } => {  [...] , left , right, [...] }
            // { [...] , left , current } => { [...] , left }
            let mut left_leaf_ = self.edit_node(left_leaf_id)?;
            let left_leaf = left_leaf_.leaf_mut(left_leaf_id)?;

            left_leaf.set_next_leaf(right_leaf_id);
        }

        // Other cases:
        // { current , right, [...] } => { right, [...] }
        // { current } => { }  (removal of leaf that is also root node)

        Ok(())
    }

    fn increment_key_value_count(&mut self) {
        if let Some(mut meta) = self.page_table.auxiliary_metadata_mut() {
            meta.key_value_count += 1;
        }
    }

    fn decrement_key_value_count(&mut self) {
        if let Some(mut meta) = self.page_table.auxiliary_metadata_mut() {
            meta.key_value_count = meta.key_value_count.saturating_sub(1);
        }
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

#[allow(clippy::nonminimal_bool)]
fn verify_node_within_parent_keys(
    node_keys: &[Vec<u8>],
    parent_left_key: Option<&[u8]>,
    parent_right_key: Option<&[u8]>,
) -> Option<&'static str> {
    if let Some(parent_left_key) = parent_left_key {
        if let Some(first_key) = node_keys.first() {
            if !(parent_left_key <= first_key) {
                return Some("parent left key - first key violation");
            }
        }
    }

    if let Some(parent_right_key) = parent_right_key {
        if let Some(last_key) = node_keys.last() {
            if !(last_key.as_slice() < parent_right_key) {
                return Some("parent right key - last key violation");
            }
        }
    }

    None
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

    #[test]
    fn test_internal_node_remove_child() {
        let mut node = InternalNode::new(
            vec![b"key100".to_vec(), b"key200".to_vec(), b"key300".to_vec()],
            vec![4, 8, 12, 16],
        );

        let (left_id, right_id) = node.remove_child(12);
        //  key100  key300
        // 4      8       16
        assert_eq!(left_id, Some(8));
        assert_eq!(right_id, Some(16));

        let (left_id, right_id) = node.remove_child(16);
        //  key100
        // 4      8
        assert_eq!(left_id, Some(8));
        assert_eq!(right_id, None);

        let (left_id, right_id) = node.remove_child(4);
        //   [_]
        //       8
        assert_eq!(left_id, None);
        assert_eq!(right_id, Some(8));

        assert!(node.keys_is_empty());
        assert_eq!(node.children().len(), 1);
    }

    #[test]
    fn test_verify_internal_node() {
        let node = InternalNode::new(vec![b"key100".to_vec(), b"key200".to_vec()], vec![4, 8, 3]);

        assert_eq!(
            node.verify_with_parent_keys(Some(b"key100"), Some(b"key201")),
            None
        );
    }

    #[test]
    fn test_verify_internal_node_bad_key_sort() {
        let mut node = InternalNode::new(vec![b"key100".to_vec(), b"key200".to_vec()], vec![4, 8, 3]);
        node.keys.reverse();

        assert!(node.verify_with_parent_keys(None, None).is_some());
    }

    #[test]
    fn test_verify_internal_node_bad_parent() {
        let node = InternalNode::new(vec![b"key100".to_vec(), b"key200".to_vec()], vec![4, 8, 3]);

        assert!(node
            .verify_with_parent_keys(Some(b"key100"), Some(b"key150"))
            .is_some());

        assert!(node
            .verify_with_parent_keys(Some(b"key150"), Some(b"key201"))
            .is_some());
    }

    #[test]
    fn test_verify_leaf_node() {
        let node = LeafNode::new(
            vec![b"key100".to_vec(), b"key200".to_vec()],
            vec![b"v1".to_vec(), b"v2".to_vec()],
        );

        assert_eq!(
            node.verify_with_parent_keys(Some(b"key100"), Some(b"key201")),
            None
        );
    }

    #[test]
    fn test_verify_leaf_node_bad_key_sort() {
        let mut node = LeafNode::new(
            vec![b"key100".to_vec(), b"key200".to_vec()],
            vec![b"v1".to_vec(), b"v2".to_vec()],
        );
        node.keys.reverse();
        node.values.reverse();

        assert!(node.verify_with_parent_keys(None, None).is_some());
    }

    #[test]
    fn test_verify_leaf_node_bad_parent() {
        let node = LeafNode::new(
            vec![b"key100".to_vec(), b"key200".to_vec()],
            vec![b"v1".to_vec(), b"v2".to_vec()],
        );

        assert!(node
            .verify_with_parent_keys(Some(b"key100"), Some(b"key150"))
            .is_some());

        assert!(node
            .verify_with_parent_keys(Some(b"key150"), Some(b"key201"))
            .is_some());
    }
}
